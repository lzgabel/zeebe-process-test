package io.camunda.zeebe.process.test.qa.multithread;

import static io.camunda.zeebe.process.test.assertions.BpmnAssert.assertThat;
import static io.camunda.zeebe.process.test.qa.util.Utilities.deployProcess;
import static io.camunda.zeebe.process.test.qa.util.Utilities.startProcessInstance;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.process.test.api.InMemoryEngine;
import io.camunda.zeebe.process.test.api.RecordStreamSource;
import io.camunda.zeebe.process.test.assertions.BpmnAssert;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import io.camunda.zeebe.process.test.qa.util.Utilities.ProcessPackStartEndEvent;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@ZeebeProcessTest
public class MultiThreadTest {

  private InMemoryEngine engine;
  private ZeebeClient client;
  private RecordStreamSource recordStreamSource;
  private ExecutorService executorService;

  @BeforeEach
  public void beforeEach() {
    executorService = Executors.newFixedThreadPool(5);
  }

  @AfterEach
  public void afterEach() {
    executorService.shutdown();
  }

  @Test
  public void testMultiThreadingThrowsNoExceptions() throws InterruptedException {
    final List<Future<Boolean>> futures =
        executorService.invokeAll(
            Arrays.asList(
                new ProcessRunner(),
                new ProcessRunner(),
                new ProcessRunner(),
                new ProcessRunner(),
                new ProcessRunner()));

    for (final Future<Boolean> future : futures) {
      try {
        Assertions.assertThat(future.get()).isTrue();
      } catch (ExecutionException ex) {
        Assertions.fail("Future completed exceptionally: %s", ExceptionUtils.getStackTrace(ex));
      }
    }
  }

  private class ProcessRunner implements Callable<Boolean> {

    @Override
    public Boolean call() {
      BpmnAssert.initRecordStream(recordStreamSource);

      deployProcess(client, ProcessPackStartEndEvent.RESOURCE_NAME);
      final ProcessInstanceEvent instanceEvent =
          startProcessInstance(engine, client, ProcessPackStartEndEvent.PROCESS_ID);
      engine.waitForIdleState();

      assertThat(instanceEvent).isCompleted();
      BpmnAssert.resetRecordStream();
      return true;
    }
  }
}