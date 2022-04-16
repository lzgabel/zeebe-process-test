/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.process.test.engine.exporter.configuration;

import static io.camunda.zeebe.util.ObjectWriterFactory.getDefaultJsonObjectWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.camunda.zeebe.util.Environment;
import io.camunda.zeebe.util.exception.UncheckedExecutionException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "eze")
public final class BrokerCfg {

  private Map<String, ExporterCfg> exporters = new HashMap<>();

  private boolean executionMetricsExporterEnabled;

  public void init(final String brokerBase) {
    init(brokerBase, new Environment());
  }

  public void init(final String brokerBase, final Environment environment) {
    exporters.values().forEach(e -> e.init(this, brokerBase));
  }

  public Map<String, ExporterCfg> getExporters() {
    return exporters;
  }

  public void setExporters(final Map<String, ExporterCfg> exporters) {
    this.exporters = exporters;
  }

  public boolean isExecutionMetricsExporterEnabled() {
    return executionMetricsExporterEnabled;
  }

  public void setExecutionMetricsExporterEnabled(final boolean executionMetricsExporterEnabled) {
    this.executionMetricsExporterEnabled = executionMetricsExporterEnabled;
  }

  @Override
  public String toString() {
    return "BrokerCfg{" + "exporters=" + exporters + '}';
  }

  public String toJson() {
    try {
      return getDefaultJsonObjectWriter().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new UncheckedExecutionException("Writing to JSON failed", e);
    }
  }
}
