package com.snowflake.examples.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogIngestMetrics implements Transformation<SinkRecord> {

    private static final Logger log = LoggerFactory.getLogger(LogIngestMetrics.class);

    @SuppressWarnings("FieldCanBeLocal")
    private static ScheduledExecutorService scheduler;
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    public LogIngestMetrics() {
        startScheduledTaskOnce();
    }

    private static synchronized void startScheduledTaskOnce() {
        if (initialized.compareAndSet(false, true)) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            Runnable task = LogIngestMetrics::fetchAndPrintMetrics;
            // Schedule the task to run every 30 seconds, after an initial delay of 30 seconds
            scheduler.scheduleAtFixedRate(task, 30, 30, TimeUnit.SECONDS);
        }
    }


    private static void fetchAndPrintMetrics() {
        log.info("Fetching JMX metrics");
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName queryName = new ObjectName("snowflake.ingest.sdk:clientName=KC_CLIENT_*,name=latency.*");
            Set<ObjectName> names = mBeanServer.queryNames(queryName, null);
            if (names.isEmpty()) {
                log.info("No JMX metrics found");
            } else {
                for (ObjectName name : names) {
                    Map<String, Object> attributes = new LinkedHashMap<>();
                    attributes.put("metric", name.getCanonicalName());
                    attributes.put("count", mBeanServer.getAttribute(name, "Count"));
                    attributes.put("min", mBeanServer.getAttribute(name, "Min"));
                    attributes.put("max", mBeanServer.getAttribute(name, "Max"));
                    attributes.put("mean", mBeanServer.getAttribute(name, "Mean"));
                    attributes.put("stddev", mBeanServer.getAttribute(name, "StdDev"));
                    attributes.put("p50", mBeanServer.getAttribute(name, "50thPercentile"));
                    attributes.put("p75", mBeanServer.getAttribute(name, "75thPercentile"));
                    attributes.put("p95", mBeanServer.getAttribute(name, "95thPercentile"));
                    attributes.put("p99", mBeanServer.getAttribute(name, "99thPercentile"));
                    attributes.put("p999", mBeanServer.getAttribute(name, "999thPercentile"));
                    attributes.put("duration_unit", mBeanServer.getAttribute(name, "DurationUnit"));
                    // Constructing the key-value pair string dynamically
                    StringBuilder kvPairs = new StringBuilder();
                    for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                        if (kvPairs.length() > 0) {
                            kvPairs.append(", ");
                        }
                        kvPairs.append(entry.getKey()).append("=").append(entry.getValue());
                    }
                    log.info("JMX latency: " + kvPairs);
                }
            }
        } catch (Exception e) {
            log.error("Unable to log JMX metrics", e);
        }
    }

    @Override
    public SinkRecord apply(SinkRecord sinkRecord) {
        return sinkRecord;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef(); // Define configuration here if needed
    }

    @Override
    public void close() {
        // TODO figure out how to stop reporting thread when there are multiple sinks using SMT
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Configuration options can be set here
    }

}