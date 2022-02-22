package io.camunda.zeebe.process.test.filters;

import io.camunda.zeebe.process.test.api.RecordStreamSource;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import io.camunda.zeebe.protocol.record.value.JobBatchRecordValue;
import io.camunda.zeebe.protocol.record.value.JobRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageStartEventSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessMessageSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.TimerRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableDocumentRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.camunda.zeebe.protocol.record.value.deployment.Process;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordStream {

  private static final Logger LOG = LoggerFactory.getLogger("io.camunda.zeebe.process.test");

  private RecordStreamSource recordStreamSource;

  private RecordStream(final RecordStreamSource recordStreamSource) {
    this.recordStreamSource = recordStreamSource;
  }

  public static RecordStream of(final RecordStreamSource recordStreamSource) {
    return new RecordStream(recordStreamSource);
  }

  public Iterable<Record<?>> records() {
    return recordStreamSource.records();
  }

  <T extends RecordValue> Iterable<Record<T>> recordsOfValueType(final ValueType valueType) {
    final Stream<Record<T>> stream =
        (Stream)
            StreamSupport.stream(recordStreamSource.records().spliterator(), false)
                .filter((record) -> record.getValueType() == valueType);
    return stream::iterator;
  }

  public Iterable<Record<ProcessInstanceRecordValue>> processInstanceRecords() {
    return recordsOfValueType(ValueType.PROCESS_INSTANCE);
  }

  public Iterable<Record<JobRecordValue>> jobRecords() {
    return recordsOfValueType(ValueType.JOB);
  }

  public Iterable<Record<JobBatchRecordValue>> jobBatchRecords() {
    return recordsOfValueType(ValueType.JOB_BATCH);
  }

  public Iterable<Record<DeploymentRecordValue>> deploymentRecords() {
    return recordsOfValueType(ValueType.DEPLOYMENT);
  }

  public Iterable<Record<Process>> processRecords() {
    return recordsOfValueType(ValueType.PROCESS);
  }

  public Iterable<Record<VariableRecordValue>> variableRecords() {
    return recordsOfValueType(ValueType.VARIABLE);
  }

  public Iterable<Record<VariableDocumentRecordValue>> variableDocumentRecords() {
    return recordsOfValueType(ValueType.VARIABLE_DOCUMENT);
  }

  public Iterable<Record<IncidentRecordValue>> incidentRecords() {
    return recordsOfValueType(ValueType.INCIDENT);
  }

  public Iterable<Record<TimerRecordValue>> timerRecords() {
    return recordsOfValueType(ValueType.TIMER);
  }

  public Iterable<Record<MessageRecordValue>> messageRecords() {
    return recordsOfValueType(ValueType.MESSAGE);
  }

  public Iterable<Record<MessageSubscriptionRecordValue>> messageSubscriptionRecords() {
    return recordsOfValueType(ValueType.MESSAGE_SUBSCRIPTION);
  }

  public Iterable<Record<MessageStartEventSubscriptionRecordValue>>
      messageStartEventSubscriptionRecords() {
    return recordsOfValueType(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION);
  }

  public Iterable<Record<ProcessMessageSubscriptionRecordValue>>
      processMessageSubscriptionRecords() {
    return recordsOfValueType(ValueType.PROCESS_MESSAGE_SUBSCRIPTION);
  }

  public void print(final boolean compact) {
    final List<Record<?>> recordsList = new ArrayList<>();
    recordStreamSource.records().forEach(recordsList::add);

    if (compact) {
      final StringBuilder stringBuilder = new StringBuilder();
      recordsList.forEach(
          record ->
              stringBuilder
                  .append(record.getRecordType())
                  .append(" ")
                  .append(record.getValueType())
                  .append(" ")
                  .append(record.getIntent()));
      LOG.info(stringBuilder.toString());
    } else {
      System.out.println("===== records (count: ${count()}) =====");
      recordsList.forEach(record -> System.out.println(record.toJson()));
      System.out.println("---------------------------");
    }
  }
}