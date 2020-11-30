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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.testutils.logging.TestLoggerResource;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.Collections;

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link KafkaReporter}.
 */
public class KafkaReporterTest extends TestLogger {

	private static final String HOST_NAME = "localhost";
	private static final String TASK_MANAGER_ID = "tm01";
	private static final String JOB_NAME = "jn01";
	private static final String TASK_NAME = "tn01";
	private static MetricRegistryImpl registry;
	private static TaskMetricGroup taskMetricGroup;
	private static KafkaReporter reporter;

	@Rule
	public final TestLoggerResource testLoggerResource = new TestLoggerResource(KafkaReporter.class, Level.INFO);



	static ReporterSetup createReporterSetup(String reporterName, String servers, String keyBy) {
		MetricConfig metricConfig = new MetricConfig();
		metricConfig.setProperty(KafkaReporterOptions.SERVERS.key(), servers);
		metricConfig.setProperty("keyBy", keyBy);
		metricConfig.setProperty("acks", "all");

		return ReporterSetup.forReporter(reporterName, metricConfig, new KafkaReporter());
	}

	@BeforeClass
	public static void setUp() {
		Configuration configuration = new Configuration();
		configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");

		registry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			Collections.singletonList(createReporterSetup("kafka-test", "tm-cdh-04:9092", "task_attempt_id")));

		taskMetricGroup = new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER_ID)
			.addTaskForJob(new JobID(), JOB_NAME, new JobVertexID(), new ExecutionAttemptID(), TASK_NAME, 0, 0);
		reporter = (KafkaReporter) registry.getReporters().get(0);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		registry.shutdown().get();
	}

	@Test
	public void testAddCounter() throws Exception {
		String counterName = "simpleCounter";

		SimpleCounter counter = new SimpleCounter();
		taskMetricGroup.counter(counterName, counter);

		assertTrue(reporter.getCounters().containsKey(counter));

		reporter.report();
	}

	@Test
	public void testAddGauge() throws Exception {
		String gaugeName = "gauge";

		taskMetricGroup.gauge(gaugeName, null);
		assertTrue(reporter.getGauges().isEmpty());

		Gauge<Long> gauge = () -> null;
		taskMetricGroup.gauge(gaugeName, gauge);
		assertTrue(reporter.getGauges().containsKey(gauge));

		reporter.report();
	}

	@Test
	public void testAddMeter() throws Exception {
		String meterName = "meter";

		Meter meter = taskMetricGroup.meter(meterName, new MeterView(5));
		assertTrue(reporter.getMeters().containsKey(meter));

		reporter.report();
	}

	@Test
	public void testAddHistogram() throws Exception {
		String histogramName = "histogram";

		Histogram histogram = taskMetricGroup.histogram(histogramName, new TestHistogram());
		assertTrue(reporter.getHistograms().containsKey(histogram));

		reporter.report();
	}
}
