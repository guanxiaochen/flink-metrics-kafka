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

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
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
	private String topic;
	private String keyBy;

	protected final Map<Gauge<?>, Map<String, String>> gauges = new HashMap<>();
	protected final Map<Counter, Map<String, String>> counters = new HashMap<>();
	protected final Map<Histogram, Map<String, String>> histograms = new HashMap<>();
	protected final Map<Meter, Map<String, String>> meters = new HashMap<>();

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
		topic = config.getString(TOPIC.key(), TOPIC.defaultValue());
		keyBy = config.getString(KEY_BY.key(), KEY_BY.defaultValue());

		String servers = config.getString(SERVERS.key(), SERVERS.defaultValue());

		Properties properties = new Properties();
		properties.put("bootstrap.servers", servers);
		properties.put("acks", config.getString("acks", "all"));
		properties.put("retries", config.getInteger("retries", 0));
		properties.put("batch.size", config.getInteger("batch.size", 16384));
		properties.put("linger.ms", config.getInteger("linger.ms", 1));
		properties.put("buffer.memory", config.getInteger("buffer.memory", 33554432));

		Thread.currentThread().setContextClassLoader(null);
		kafkaProducer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
	}

	@Override
	public void close() {
		if(kafkaProducer != null) {
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
		synchronized(this) {
			if (metric instanceof Counter) {
				this.counters.put((Counter)metric, allVariables);
			} else if (metric instanceof Gauge) {
				this.gauges.put((Gauge)metric, allVariables);
			} else if (metric instanceof Histogram) {
				this.histograms.put((Histogram)metric, allVariables);
			} else if (metric instanceof Meter) {
				this.meters.put((Meter)metric, allVariables);
			} else {
				log.warn("Cannot add unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
			}

		}

	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {

		synchronized(this) {
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
		}
		catch (ConcurrentModificationException ignored) {
			// at tryReport() we don't synchronize while iterating over the various maps which might cause a
			// ConcurrentModificationException to be thrown, if concurrently a metric is being added or removed.
		}
	}

	private void tryReport() {
		for (Map.Entry<Counter, Map<String, String>> metric : counters.entrySet()) {
			JSONObject json = new JSONObject();
			json.put("value", metric.getKey().getCount());
			json.putAll(metric.getValue());
			String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
			kafkaProducer.send(new ProducerRecord<>(topic, key, json.toJSONString()));
		}

		for (Map.Entry<Gauge<?>, Map<String, String>> metric : gauges.entrySet()) {
			JSONObject json = new JSONObject();
			json.put("value", metric.getKey().getValue());
			json.putAll(metric.getValue());
			String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
			kafkaProducer.send(new ProducerRecord<>(topic, key, json.toJSONString()));
		}


		for (Map.Entry<Meter, Map<String, String>> metric : meters.entrySet()) {
			JSONObject json = new JSONObject();
			json.put("value", metric.getKey().getRate());
			json.putAll(metric.getValue());
			String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
			kafkaProducer.send(new ProducerRecord<>(topic, key, json.toJSONString()));
		}


		for (Map.Entry<Histogram, Map<String, String>> metric : histograms.entrySet()) {
			HistogramStatistics stats = metric.getKey().getStatistics();
			JSONObject value = new JSONObject();
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

			JSONObject json = new JSONObject();
			json.put("value", value);
			json.putAll(metric.getValue());
			String key = keyBy.isEmpty() ? null : metric.getValue().get(keyBy);
			kafkaProducer.send(new ProducerRecord<>(topic, key, json.toJSONString()));
		}
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}
}
