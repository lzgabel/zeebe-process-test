package io.camunda.zeebe.bpmnassert.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.bpmnassert.filters.IncidentRecordStreamFilter;
import io.camunda.zeebe.bpmnassert.filters.StreamFilter;
import io.camunda.zeebe.bpmnassert.testengine.RecordStreamSource;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.data.Offset;

/** Assertions for {@code ActivatedJob} instances */
public class JobAssert extends AbstractAssert<JobAssert, ActivatedJob> {

  private final RecordStreamSource recordStreamSource;

  public JobAssert(final ActivatedJob actual, final RecordStreamSource recordStreamSource) {
    super(actual, JobAssert.class);
    this.recordStreamSource = recordStreamSource;
  }

  /**
   * Asserts that the activated job is associated to an element with the given id
   *
   * @param expectedElementId element id to check
   * @return this {@link JobAssert}
   */
  public JobAssert hasElementId(final String expectedElementId) {
    assertThat(expectedElementId).describedAs("expectedElementId").isNotNull().isNotEmpty();
    final String actualElementId = actual.getElementId();

    assertThat(actualElementId)
        .withFailMessage(
            "Job is not associated with expected element id '%s' but is instead associated with '%s'.",
            expectedElementId, actualElementId)
        .isEqualTo(expectedElementId);
    return this;
  }

  // TODO decide whether this assertion has any value

  /**
   * Asserts that the activated job has the given deadline
   *
   * @param expectedDeadline deadline in terms of {@code System.currentTimeMillis()}
   * @param offset offset in milliseconds to tolerate timing invariances
   * @return this {@link JobAssert}
   */
  public JobAssert hasDeadline(final long expectedDeadline, final Offset<Long> offset) {
    assertThat(offset).describedAs("Offset").isNotNull();
    final long actualDeadline = actual.getDeadline();

    assertThat(actualDeadline).describedAs("Deadline").isCloseTo(expectedDeadline, offset);
    return this;
  }

  /**
   * Asserts that the activated job is associated to the given process id
   *
   * @param expectedBpmnProcessId proces id to check
   * @return this {@link JobAssert}
   */
  public JobAssert hasBpmnProcessId(final String expectedBpmnProcessId) {
    assertThat(expectedBpmnProcessId).describedAs("expectedBpmnProcessId").isNotNull().isNotEmpty();
    final String actualBpmnProcessId = actual.getBpmnProcessId();

    assertThat(actualBpmnProcessId)
        .withFailMessage(
            "Job is not associated with BPMN process id '%s' but is instead associated with '%s'.",
            expectedBpmnProcessId, actualBpmnProcessId)
        .isEqualTo(expectedBpmnProcessId);
    return this;
  }

  /**
   * Asserts that the activated job has the given number of retries
   *
   * @param expectedRetries expected retries
   * @return this {@link JobAssert}
   */
  public JobAssert hasRetries(final int expectedRetries) {
    final int actualRetries = actual.getRetries();

    assertThat(actualRetries)
        .withFailMessage(
            "Job does not have %d retries, as expected, but instead has %d retries.",
            expectedRetries, actualRetries)
        .isEqualTo(expectedRetries);
    return this;
  }

  /**
   * Asserts whether any incidents were raised for this job (regardless of whether these incidents
   * are active or already resolved)
   *
   * @return this {@link JobAssert}
   */
  public JobAssert hasAnyIncidents() {
    final boolean incidentsWereRaised =
        getIncidentCreatedRecords().stream().findFirst().isPresent();

    assertThat(incidentsWereRaised)
        .withFailMessage("No incidents were raised for this job")
        .isTrue();
    return this;
  }

  /**
   * Asserts whether no incidents were raised for this job
   *
   * @return this {@link JobAssert}
   */
  public JobAssert hasNoIncidents() {
    final boolean incidentsWereRaised =
        getIncidentCreatedRecords().stream().findFirst().isPresent();

    assertThat(incidentsWereRaised).withFailMessage("Incidents were raised for this job").isFalse();
    return this;
  }

  /**
   * Extracts the latest incident
   *
   * @return {@link IncidentAssert} for the latest incident
   */
  public IncidentAssert extractLatestIncident() {
    hasAnyIncidents();

    final List<Record<IncidentRecordValue>> incidentCreatedRecords =
        getIncidentCreatedRecords().stream().collect(Collectors.toList());

    final Record<IncidentRecordValue> latestIncidentRecord =
        incidentCreatedRecords.get(incidentCreatedRecords.size() - 1);

    return new IncidentAssert(latestIncidentRecord.getKey(), recordStreamSource);
  }

  private IncidentRecordStreamFilter getIncidentCreatedRecords() {
    return StreamFilter.incident(recordStreamSource)
        .withRejectionType(RejectionType.NULL_VAL)
        .withJobKey(actual.getKey());
  }

  /**
   * Extracts the variables of the activated job.
   *
   * @return this {@link JobAssert}
   */
  public MapAssert<String, Object> extractingVariables() {
    return assertThat(actual.getVariablesAsMap()).describedAs("Variables");
  }

  /**
   * Extracts the header values of the activated job.
   *
   * @return this {@link JobAssert}
   */
  public MapAssert<String, String> extractingHeaders() {
    return assertThat(actual.getCustomHeaders()).describedAs("Headers");
  }
}