/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.process.test.engine;

import io.camunda.zeebe.protocol.impl.encoding.AuthInfo;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.InterPartitionCommandSender;

final class CommandSender implements InterPartitionCommandSender {
  private final CommandWriter writer;

  CommandSender(final CommandWriter writer) {
    this.writer = writer;
  }

  @Override
  public void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue command) {
    final RecordMetadata metadata =
        new RecordMetadata().recordType(RecordType.COMMAND).intent(intent).valueType(valueType);
    writer.writeCommandWithoutKey(command, metadata);
  }

  @Override
  public void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final Long recordKey,
      final UnifiedRecordValue command) {
    sendCommand(receiverPartitionId, valueType, intent, recordKey, command, null);
  }

  @Override
  public void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final Long recordKey,
      final UnifiedRecordValue command,
      final AuthInfo authInfo) {
    final RecordMetadata metadata =
        new RecordMetadata().recordType(RecordType.COMMAND).intent(intent).valueType(valueType);
    writer.writeCommandWithKey(recordKey, command, metadata);
  }
}
