package com.vmturbo.common.protobuf.memory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.annotation.Nonnull;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for programmatically dumping heap on different JVM's.
 * <p/>
 * Support dumping heap on both the HotSpot JVM (https://en.wikipedia.org/wiki/HotSpot)
 * and IBM OpenJ9 (https://en.wikipedia.org/wiki/OpenJ9).
 */
public class HeapDumper {
    private static final Logger logger = LogManager.getLogger();

    private static final String HPROF_SUFFIX = ".hprof";
    private static final String PHD_SUFFIX = ".phd";

    /**
     * Create a new {@link HeapDumper} object for dumping heap. Not a static utility to
     * permit dependency injection for tests.
     */
    public HeapDumper() {
    }

    /**
     * Dump the heap.
     *
     * @param fileName The name of the file in which the heap dump should be stored.
     * @return A description of the result.
     * @throws IllegalAccessException If the file at the filename cannot be written to.
     * @throws InvocationTargetException If we are unable to invoke the dump method.
     * @throws ClassNotFoundException If we are unable to find some of the JVM internal objects
     *                                necessary to trigger the heap dump.
     * @throws IOException If something goes wrong writing the dump file.
     */
    public String dumpHeap(@Nonnull final String fileName) throws IllegalAccessException,
        InvocationTargetException, ClassNotFoundException, IOException {
        // Figure out what JVM we are running on.
        if (ManagementFactory.getRuntimeMXBean().getVmVersion().toLowerCase().contains("openj9")) {
            return dumpHeapOpenJ9(fileName);
        } else {
            return dumpHeapHotspot(fileName);
        }
    }

    /**
     * Dump the heap when running on the HotSpot JVM.
     *
     * @param filename The name of the file in which to dump the heap.
     * @return The name of the file written to.
     * @throws IOException If something goes wrong writing the dump file.
     */
    private String dumpHeapHotspot(final String filename) throws IOException {
        final String fn = filename.toLowerCase().endsWith(HPROF_SUFFIX) ? filename : (filename + HPROF_SUFFIX);

        // See https://blogs.oracle.com/sundararajan/programmatically-dumping-heap-from-java-applications
        logger.info("Attempting to dump heap using HotSpotDiagnosticMXBean to '{}'.", fn);
        final HotSpotDiagnosticMXBean diagnosticMXBean =
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        diagnosticMXBean.dumpHeap(fn, true);

        return fn;
    }

    /**
     * Dump the heap when running on OpenJ9.
     * <p/>
     * There are actually no publicly available methods to dump the heap programmatically in OpenJ9.
     * This method was reverse engineered from the source code, making use of private internals
     * in a way that is brittle and could break without notice when we apply an OpenJ9 update.
     * <p/>
     * TODO: Write some tests for this so that we can be aware if an update to the JVM breaks this method.
     *
     * @param filename The name of the file to dump the heap to.
     * @return A description of the result of dumping the heap.
     * @throws ClassNotFoundException If unable to find dump bean class.
     * @throws IllegalAccessException If unable to access method for dumping.
     * @throws InvocationTargetException If unable to invoke method to dump.
     */
    private String dumpHeapOpenJ9(final String filename)
        throws ClassNotFoundException, IllegalAccessException, InvocationTargetException {
        final String fn = filename.toLowerCase().endsWith(PHD_SUFFIX) ? filename : (filename + PHD_SUFFIX);
        logger.info("Attempting to dump heap using OpenJ9DiagnosticsMXBean to '{}'.", fn);

        // See jcl/src/jdk.management/share/classes/openj9/lang/management/internal/OpenJ9DiagnosticsMXBeanImpl.java
        // in the OpenJ9 source.
        @SuppressWarnings("unchecked")
        final Class<? extends PlatformManagedObject> klass =
            (Class<? extends PlatformManagedObject>)Class.forName("openj9.lang.management.OpenJ9DiagnosticsMXBean");
        final PlatformManagedObject platformMXBean = ManagementFactory.getPlatformMXBean(klass);

        final Field[] fields = platformMXBean.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.getName().equals("dump_triggerDump")) {
                field.setAccessible(true);
                final Method method = (Method)field.get(platformMXBean);
                final String options = String.format("heap:file=%s", fn);

                return (String)method.invoke(null, options);
            }
        }

        return "Failed to heap dump";
    }
}
