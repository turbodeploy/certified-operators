package com.vmturbo.components.common.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.Maps;

import com.vmturbo.components.common.metrics.ScheduledMetrics.ScheduledMetricsObserver;
import com.vmturbo.proactivesupport.DataMetricGauge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Add memory metrics from java's Native Memory Tracking to data metrics
 * tracked by the component.
 * We use one metric for every NMT category (Java Heap, Class, Code etc.) with
 * label "metric" and possible label values "reserved", "committed", "arena" etc.
 */
public class NativeMemoryTrackingMetrics implements ScheduledMetricsObserver {

    private static Logger logger = LogManager.getLogger();

    private static MBeanServer jmxMBeanServer = ManagementFactory.getPlatformMBeanServer();
    // Method invocation arguments
    private static final String[] ARGS_TYPE = new String[]{"[Ljava.lang.String;"};
    private static final Object[] SUMMARY = new Object[]{new String[]{"summary"}};
    private static final String OPERATION = "vmNativeMemory";
    private static final ObjectName OBJECT_NAME = getObjectName();

    // Key is category (e.g. Java Heap, GC etc.)
    private static final Map<String, DataMetricGauge> dataMetrics = Maps.newHashMap();

    // Parsers for NMT data
    private static final Pattern TOTAL_MATCHER =
            Pattern.compile("^Total: reserved=(\\d*)KB, committed=(\\d*)KB");
    private static final Pattern CATEGORY_MATCHER =
            Pattern.compile("^- *(.*) \\(reserved=(\\d*)KB, committed=(\\d*)KB\\)");
    private static final Pattern MALLOC_MATCHER =
            Pattern.compile("^ *\\(malloc=(\\d*).*");
    private static final Pattern ARENA_MATCHER =
            Pattern.compile("^ *\\(arena=(\\d*).*");
    private static final Pattern MMAP_MATCHER =
            Pattern.compile("^ *\\(mmap: reserved=(\\d*)KB, committed=(\\d*)KB\\).*");

    private static final String TOTAL = "Total";
    private static final String RESERVED = "reserved";
    private static final String COMMITTED = "committed";

    /**
     * Check if this feature is even supported in the current environment. This check will make a
     * request for NMT data in the current environment, and return true if the check test request
     * was successful.
     *
     * @return true, if the NMT request can be succesfully made.
     */
    public static boolean isSupported() {
        try {
            vmtNativeMemorySummary();
        } catch (JMException jme) {
            logger.warn("Exception trying to invoke and parse NMT command: `{}`. We will assume Native Memory " +
                    "Tracking is not supported in this environment.", jme.getMessage());
            return false;
        }
        return true;
    }

    private static String vmtNativeMemorySummary() throws JMException {
        return (String)jmxMBeanServer.invoke(OBJECT_NAME, OPERATION, SUMMARY, ARGS_TYPE);
    }

    @Override
    public void observe() {
        try {
            String nmt = vmtNativeMemorySummary(); // see sample output below
            logger.trace(nmt);
            parse(nmt);
        } catch (JMException e) {
            logger.error("Exception trying to invoke and parse NMT command", e);
        }
    }

    private static void parse(String nmt) {
        String[] lines = nmt.split("\\n");
        String categoryName = null;
        for (String line : lines) {
            if (line.startsWith("-")) {
                Matcher category = CATEGORY_MATCHER.matcher(line);
                if (category.matches()) {
                    categoryName = category.group(1);
                    recordData(categoryName, RESERVED, category.group(2));
                    recordData(categoryName, COMMITTED, category.group(3));
                }
            } else if (line.isEmpty() || line.startsWith("Native")) {
                continue;
            } else if (line.startsWith(TOTAL)) {
                Matcher total = TOTAL_MATCHER.matcher(line);
                if (total.matches()) {
                    recordData(TOTAL, RESERVED, total.group(1));
                    recordData(TOTAL, COMMITTED, total.group(2));
                }
            } else {
                Matcher malloc = MALLOC_MATCHER.matcher(line);
                if (malloc.matches()) {
                    recordData(categoryName, "malloc", malloc.group(1));
                    continue;
                }
                Matcher arena = ARENA_MATCHER.matcher(line);
                if (arena.matches()) {
                    recordData(categoryName, "arena", arena.group(1));
                    continue;
                }
                Matcher mmap = MMAP_MATCHER.matcher(line);
                if (mmap.matches()) {
                    recordData(categoryName, "mmap reserved", mmap.group(1));
                    recordData(categoryName, "mmap committed", mmap.group(2));
                    continue;
                }
            }
        }
    }

    private static void recordData(String category, String metric, String value) {
        logger.debug("NMT category {}: {}={}", category, metric, value);
        Double d = Double.valueOf(value);
        DataMetricGauge gauge =
                dataMetrics.computeIfAbsent(category, NativeMemoryTrackingMetrics::newGauge);
        gauge.labels(metric).setData(d);
    }

    private static DataMetricGauge newGauge(String category) {
        String cat = category.toLowerCase().replaceAll(" ", "_");
        return DataMetricGauge.builder()
                .withName("jvm_native_memory_" + cat)
                .withHelp("JVM Native Memory Tracking for category '" + category + "'")
                .withLabelNames("metric")
                .build()
                .register();
    }

    private static ObjectName getObjectName() {
        try {
            return new ObjectName("com.sun.management:type=DiagnosticCommand");
        } catch (MalformedObjectNameException e) {
            logger.error("Cannot create ObjectName for DiagnosticCommand", e);
        }
        return null;
    }

    /*
    This is a complete example of the nmt command output:

    Native Memory Tracking:

    Total: reserved=4495646KB, committed=735170KB
    -                 Java Heap (reserved=2832384KB, committed=461824KB)
                                (mmap: reserved=2832384KB, committed=461824KB)

    -                     Class (reserved=1138932KB, committed=100468KB)
                                (classes #16481)
                                (  instance classes #15576, array classes #905)
                                (malloc=2292KB #41446)
                                (mmap: reserved=1136640KB, committed=98176KB)
                                (  Metadata:   )
                                (    reserved=88064KB, committed=87000KB)
                                (    used=85453KB)
                                (    free=1547KB)
                                (    waste=0KB =0.00%)
                                (  Class space:)
                                (    reserved=1048576KB, committed=11176KB)
                                (    used=10451KB)
                                (    free=725KB)
                                (    waste=0KB =0.00%)

    -                    Thread (reserved=45408KB, committed=4384KB)
                                (thread #44)
                                (stack: reserved=45200KB, committed=4176KB)
                                (malloc=156KB #266)
                                (arena=52KB #86)

    -                      Code (reserved=249411KB, committed=26947KB)
                                (malloc=1723KB #8762)
                                (mmap: reserved=247688KB, committed=25224KB)

    -                        GC (reserved=145449KB, committed=57485KB)
                                (malloc=7205KB #90436)
                                (mmap: reserved=138244KB, committed=50280KB)

    -                  Compiler (reserved=401KB, committed=401KB)
                                (malloc=270KB #766)
                                (arena=131KB #5)

    -                  Internal (reserved=821KB, committed=821KB)
                                (malloc=789KB #1645)
                                (mmap: reserved=32KB, committed=32KB)

    -                     Other (reserved=32948KB, committed=32948KB)
                                (malloc=32948KB #55)

    -                    Symbol (reserved=25396KB, committed=25396KB)
                                (malloc=23469KB #263172)
                                (arena=1926KB #1)

    -    Native Memory Tracking (reserved=6470KB, committed=6470KB)
                                (malloc=18KB #245)
                                (tracking overhead=6452KB)

    -        Shared class space (reserved=16956KB, committed=16956KB)
                                (mmap: reserved=16956KB, committed=16956KB)

    -               Arena Chunk (reserved=178KB, committed=178KB)
                                (malloc=178KB)

    -                   Logging (reserved=5KB, committed=5KB)
                                (malloc=5KB #189)

    -                 Arguments (reserved=19KB, committed=19KB)
                                (malloc=19KB #514)

    -                    Module (reserved=654KB, committed=654KB)
                                (malloc=654KB #3542)

    -              Synchronizer (reserved=205KB, committed=205KB)
                                (malloc=205KB #1728)

    -                 Safepoint (reserved=8KB, committed=8KB)
                                (mmap: reserved=8KB, committed=8KB)
    */
}
