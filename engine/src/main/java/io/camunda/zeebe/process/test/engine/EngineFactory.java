/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.process.test.engine;

import com.google.common.collect.Lists;
import io.atomix.raft.storage.log.IndexedRaftLogEntry;
import io.atomix.raft.storage.log.RaftLog;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender;
import io.camunda.zeebe.db.ConsistencyChecksSettings;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.ZeebeDbFactory;
import io.camunda.zeebe.db.impl.rocksdb.RocksDbConfiguration;
import io.camunda.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.camunda.zeebe.engine.processing.EngineProcessors;
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedEventRegistry;
import io.camunda.zeebe.engine.state.ZbColumnFamilies;
import io.camunda.zeebe.engine.state.appliers.EventAppliers;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamBuilder;
import io.camunda.zeebe.logstreams.log.LogStreamReader;
import io.camunda.zeebe.logstreams.log.LoggedEvent;
import io.camunda.zeebe.logstreams.storage.LogStorage;
import io.camunda.zeebe.logstreams.storage.atomix.AtomixLogStorage;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.process.test.engine.exporter.configuration.BrokerCfg;
import io.camunda.zeebe.process.test.engine.exporter.configuration.ExporterCfg;
import io.camunda.zeebe.process.test.engine.exporter.repo.ExporterLoadException;
import io.camunda.zeebe.process.test.engine.exporter.repo.ExporterRepository;
import io.camunda.zeebe.process.test.engine.exporter.stream.ExporterDirector;
import io.camunda.zeebe.process.test.engine.exporter.stream.ExporterDirectorContext;
import io.camunda.zeebe.process.test.engine.exporter.stream.ExporterDirectorContext.ExporterMode;
import io.camunda.zeebe.protocol.impl.record.CopiedRecord;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.util.jar.ExternalJarLoadException;
import io.camunda.zeebe.util.sched.Actor;
import io.camunda.zeebe.util.sched.ActorScheduler;
import io.camunda.zeebe.util.sched.ActorSchedulingService;
import io.camunda.zeebe.util.sched.clock.ActorClock;
import io.camunda.zeebe.util.sched.clock.ControlledActorClock;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class EngineFactory {

  static final int partitionId = 1;
  static final int partitionCount = 1;

  private static Path tempFolder = Path.of("/Users/lizhi/Desktop/eze/");

  static final RaftLog log = RaftLog.builder().withDirectory(tempFolder.toFile()).build();
  static AtomixLogStorage logStorage = null;

  public static ZeebeTestEngine create() {
    return create(26500);
  }

  public static ZeebeTestEngine create(final int port) {

    final ControlledActorClock clock = createActorClock();
    final ActorScheduler scheduler = createAndStartActorScheduler(clock);

    Appender appender = new Appender();
    logStorage = new AtomixLogStorage(log::openUncommittedReader, appender);
    final LogStream logStream = createLogStream(logStorage, scheduler, partitionId);

    final SubscriptionCommandSenderFactory subscriptionCommandSenderFactory =
        new SubscriptionCommandSenderFactory(
            logStream.newLogStreamRecordWriter().join(), partitionId);

    final GrpcToLogStreamGateway gateway =
        new GrpcToLogStreamGateway(
            logStream.newLogStreamRecordWriter().join(), partitionId, partitionCount, port);
    final Server grpcServer = ServerBuilder.forPort(port).addService(gateway).build();

    final GrpcResponseWriter grpcResponseWriter = new GrpcResponseWriter(gateway);

    final ZeebeDb<ZbColumnFamilies> zeebeDb = createDatabase();

    final EngineStateMonitor engineStateMonitor =
        new EngineStateMonitor(logStorage, logStream.newLogStreamReader().join());

    final StreamProcessor streamProcessor =
        createStreamProcessor(
            logStream,
            zeebeDb,
            scheduler,
            grpcResponseWriter,
            engineStateMonitor,
            partitionCount,
            subscriptionCommandSenderFactory);

    final LogStreamReader reader = logStream.newLogStreamReader().join();
    final RecordStreamSourceImpl recordStream = new RecordStreamSourceImpl(reader, partitionId);

    BrokerCfg cfg = new BrokerCfg();
    ExporterCfg exporterCfg = new ExporterCfg();
    exporterCfg.setClassName("io.camunda.zeebe.exporter.ElasticsearchExporter");
    exporterCfg.setArgs(Map.of("url", "http://localhost:9200"));
    Map<String, ExporterCfg> elasticsearch = Map.of("elasticsearch", exporterCfg);
    cfg.setExporters(elasticsearch);

    final ExporterMode exporterMode = ExporterMode.ACTIVE;
    final ExporterDirectorContext exporterCtx =
        new ExporterDirectorContext()
            .id(1003)
            .name(Actor.buildActorName(1, "Exporter", 1))
            .logStream(logStream)
            .zeebeDb(zeebeDb)
            .descriptors(buildExporterRepository(cfg).getExporters().values())
            .exporterMode(exporterMode);

    final ExporterDirector director = new ExporterDirector(exporterCtx, false);

    return new InMemoryEngine(
        grpcServer,
        streamProcessor,
        gateway,
        zeebeDb,
        logStream,
        scheduler,
        recordStream,
        clock,
        engineStateMonitor,
        director);
  }

  private static ControlledActorClock createActorClock() {
    return new ControlledActorClock();
  }

  private static ActorScheduler createAndStartActorScheduler(final ActorClock clock) {
    final ActorScheduler scheduler =
        ActorScheduler.newActorScheduler().setActorClock(clock).build();
    scheduler.start();
    return scheduler;
  }

  private static ExporterRepository buildExporterRepository(final BrokerCfg cfg) {
    final ExporterRepository exporterRepository = new ExporterRepository();
    final var exporterEntries = cfg.getExporters().entrySet();

    // load and validate exporters
    for (final var exporterEntry : exporterEntries) {
      final var id = exporterEntry.getKey();
      final var exporterCfg = exporterEntry.getValue();
      try {
        exporterRepository.load(id, exporterCfg);
      } catch (final ExporterLoadException | ExternalJarLoadException e) {
        throw new IllegalStateException(
            "Failed to load exporter with configuration: " + exporterCfg, e);
      }
    }

    return exporterRepository;
  }

  private static LogStream createLogStream(
      final LogStorage logStorage, final ActorSchedulingService scheduler, final int partitionId) {
    final LogStreamBuilder builder =
        LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withActorSchedulingService(scheduler);

    final CompletableFuture<LogStream> theFuture = new CompletableFuture<>();

    scheduler.submitActor(
        Actor.wrap(
            (control) ->
                builder
                    .buildAsync()
                    .onComplete(
                        (logStream, failure) -> {
                          if (failure != null) {
                            theFuture.completeExceptionally(failure);
                          } else {
                            theFuture.complete(logStream);
                          }
                        })));

    return theFuture.join();
  }

  private static ZeebeDb<ZbColumnFamilies> createDatabase() {
    // final InMemoryDbFactory<ZbColumnFamilies> factory = new InMemoryDbFactory<>();
    final RocksDbConfiguration configuration = new RocksDbConfiguration();
    final ConsistencyChecksSettings settings = new ConsistencyChecksSettings(true, true);
    final ZeebeDbFactory<ZbColumnFamilies> factory =
        new ZeebeRocksDbFactory<>(configuration, settings);
    try {
      final ZeebeDb zeebeDb = factory.createDb(tempFolder.toFile());
      //      TransactionContext transactionContext = zeebeDb.createContext();
      //      ZeebeState zeebeState = new ZeebeDbState(zeebeDb, transactionContext);
      return zeebeDb;
    } catch (final Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static StreamProcessor createStreamProcessor(
      final LogStream logStream,
      final ZeebeDb<ZbColumnFamilies> database,
      final ActorSchedulingService scheduler,
      final GrpcResponseWriter grpcResponseWriter,
      final EngineStateMonitor engineStateMonitor,
      final int partitionCount,
      final SubscriptionCommandSenderFactory subscriptionCommandSenderFactory) {
    return StreamProcessor.builder()
        .logStream(logStream)
        .zeebeDb(database)
        .eventApplierFactory(EventAppliers::new)
        .commandResponseWriter(grpcResponseWriter)
        .streamProcessorFactory(
            context ->
                EngineProcessors.createEngineProcessors(
                    context.listener(engineStateMonitor),
                    partitionCount,
                    subscriptionCommandSenderFactory.createSender(),
                    new SinglePartitionDeploymentDistributor(),
                    new SinglePartitionDeploymentResponder(),
                    jobType -> {}))
        .actorSchedulingService(scheduler)
        .build();
  }

  private static List<Record> createRecordStream(
      final LogStreamReader reader, final long position) {
    if (position > 0) {
      reader.seekToNextEvent(position);
    } else {
      reader.seekToFirstEvent();
    }

    List<Record> records = Lists.newArrayList();

    while (reader.hasNext()) {
      final LoggedEvent event = reader.next();
      RecordMetadata metadata = new RecordMetadata();
      metadata.wrap(event.getMetadata(), event.getMetadataOffset(), event.getMetadataLength());
      final AtomicReference<UnifiedRecordValue> record = new AtomicReference<>();
      Optional.ofNullable(TypedEventRegistry.EVENT_REGISTRY.get(metadata.getValueType()))
          .ifPresent(
              value -> {
                try {
                  record.set(value.getDeclaredConstructor().newInstance());
                } catch (InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException
                    | NoSuchMethodException e) {
                  e.printStackTrace();
                }
              });

      CopiedRecord copiedRecord =
          new CopiedRecord(
              record.get(),
              metadata,
              event.getKey(),
              partitionId,
              event.getPosition(),
              event.getSourceEventPosition(),
              event.getTimestamp());

      records.add(copiedRecord);
    }
    return records;
  }

  private static final class Appender implements ZeebeLogAppender {

    @Override
    public void appendEntry(
        final long lowestPosition,
        final long highestPosition,
        final ByteBuffer data,
        final AppendListener appendListener) {
      final ApplicationEntry entry = new ApplicationEntry(lowestPosition, highestPosition, data);
      final IndexedRaftLogEntry indexedEntry = log.append(new RaftLogEntry(1, entry));

      appendListener.onWrite(indexedEntry);
      log.setCommitIndex(indexedEntry.index());

      appendListener.onCommit(indexedEntry);
      logStorage.onCommit(indexedEntry.index());
    }
  }
}
