/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.metrics.kafka.KafkaReporterOptions.*;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via kafka {@link Logger}.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter implements MetricReporter, CharacterFilter, Scheduled {
    private static final Logger log = LoggerFactory.getLogger(KafkaReporter.class);

    private static final char SCOPE_SEPARATOR = '_';
    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
        @Override
        public String filterCharacters(String input) {
            return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
        }
    };

    // the initial size roughly fits ~150 metrics with default scope settings

    private KafkaProducer<String, String> kafkaProducer;
    private String servers;
    private String topic;
    private String keyBy;
    private String cluster;

    protected final Map<Gauge<?>, Map<String, String>> gauges = new HashMap<>();
    protected final Map<Counter, Map<String, String>> counters = new HashMap<>();
    protected final Map<Histogram, Map<String, String>> histograms = new HashMap<>();
    protected final Map<Meter, Map<String, String>> meters = new HashMap<>();


    private ObjectMapper mapper = new ObjectMapper();

    @VisibleForTesting
    Map<Gauge<?>, Map<String, String>> getGauges() {
        return gauges;
    }

    @VisibleForTesting
    Map<Counter, Map<String, String>> getCounters() {
        return counters;
    }

    @VisibleForTesting
    Map<Histogram, Map<String, String>> getHistograms() {
        return histograms;
    }

    @VisibleForTesting
    Map<Meter, Map<String, String>> getMeters() {
        return meters;
    }

    @Override
    public void open(MetricConfig config) {
        cluster = config.getString(CLUSTER.key(), CLUSTER.defaultValue());
        servers = config.getString(SERVERS.key(), SERVERS.defaultValue());
        topic = config.getString(TOPIC.key(), TOPIC.defaultValue());
        keyBy = config.getString(KEY_BY.key(), KEY_BY.defaultValue());
        if (servers == null) {
            log.warn("Cannot find config {}", SERVERS.key());
        }


        Properties properties = new Properties();
        properties.put(SERVERS.key(), servers);
        for (Object keyObj : config.keySet()) {
            String key = keyObj.toString();
            if (key.startsWith("prop.")) {
                properties.put(key.substring(5), config.getString(key, ""));
            }
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(null);
            kafkaProducer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        } catch (Exception e) {
            log.warn("KafkaReporter init error.", e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> allVariables = new HashMap<>();
        allVariables.put("name", getLogicalScope(group) + SCOPE_SEPARATOR + CHARACTER_FILTER.filterCharacters(metricName));
        for (Map.Entry<String, String> entry : group.getAllVariables().entrySet()) {
            String key = entry.getKey();
            allVariables.put(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)), entry.getValue());
        }
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.put((Counter) metric, allVariables);
            } else if (metric instanceof Gauge) {
                this.gauges.put((Gauge) metric, allVariables);
            } else if (metric instanceof Histogram) {
                this.histograms.put((Histogram) metric, allVariables);
            } else if (metric instanceof Meter) {
                this.meters.put((Meter) metric, allVariables);
            } else {
                log.warn("Cannot add unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }

    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.remove(metric);
            } else if (metric instanceof Gauge) {
                this.gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                this.histograms.remove(metric);
            } else if (metric instanceof Meter) {
                this.meters.remove(metric);
            } else {
                log.warn("Cannot remove unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }
    }

    private static String getLogicalScope(MetricGroup group) {
        return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    }

    @Override
    public void report() {
        try {
            tryReport();
        } catch (Exception ignored) {
            log.warn("KafkaReporter report error: {}", ignored.getMessage());
        }
    }

    private void tryReport() throws JsonProcessingException {
        if (kafkaProducer == null) {
            return;
        }
        long timeStamp = System.currentTimeMillis();
        for (Map.Entry<Counter, Map<String, String>> metric : counters.entrySet()) {
            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("time_stamp", timeStamp);
            json.putAll(metric.getValue());
            json.put("value", metric.getKey().getCount());
            String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
            kafkaProducer.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(json)));
        }

        for (Map.Entry<Gauge<?>, Map<String, String>> metric : gauges.entrySet()) {
            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("time_stamp", timeStamp);
            json.putAll(metric.getValue());
            json.put("value", metric.getKey().getValue());
            String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
            kafkaProducer.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(json)));
        }

        for (Map.Entry<Meter, Map<String, String>> metric : meters.entrySet()) {
            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("time_stamp", timeStamp);
            json.putAll(metric.getValue());
            json.put("value", metric.getKey().getRate());
            String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
            kafkaProducer.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(json)));
        }


        for (Map.Entry<Histogram, Map<String, String>> metric : histograms.entrySet()) {
            HistogramStatistics stats = metric.getKey().getStatistics();
            Map<String, Object> value = new HashMap<>();
            value.put("count", stats.size());
            value.put("min", stats.getMin());
            value.put("max", stats.getMax());
            value.put("mean", stats.getMean());
            value.put("stddev", stats.getStdDev());
            value.put("p50", stats.getQuantile(0.50));
            value.put("p75", stats.getQuantile(0.75));
            value.put("p95", stats.getQuantile(0.95));
            value.put("p98", stats.getQuantile(0.98));
            value.put("p99", stats.getQuantile(0.99));
            value.put("p999", stats.getQuantile(0.999));

            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("time_stamp", timeStamp);
            json.putAll(metric.getValue());
            json.put("value", value);
            String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
            kafkaProducer.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(json)));
        }
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
