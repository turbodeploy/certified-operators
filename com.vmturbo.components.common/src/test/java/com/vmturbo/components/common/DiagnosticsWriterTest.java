package com.vmturbo.components.common;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

/**
 * Test cases for {@link DiagnosticsWriter}.
 *
 */
public class DiagnosticsWriterTest {

    final File file = new File("test.zip");

    @Before
    public void setup() {
        file.deleteOnExit();
    }

    @Test
    /**
     * Write strings to a zip file, then read the file.
     * @throws IOException
     */
    public void testWriteZipEntries() throws IOException {
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        List<String> list1 = Lists.newArrayList("A", "BB", "CCC");
        List<String> list2 = Lists.newArrayList("X", "Y", "Z");

        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(file));
        writer.writeZipEntry("Test1", list1, zos);
        writer.writeZipEntry("Test2", list2, zos);
        zos.close();
        assertTrue(file.exists());

        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
        ZipEntry ze = zis.getNextEntry();
        // Test that the first entry is read properly
        assertTrue(ze.getName().equals("Test1"));
        byte[] bytes = new byte[20];
        zis.read(bytes);
        assertEquals("A\nBB\nCCC\n", new String(bytes, 0, 9));
        assertEquals(0, bytes[9]);
        zis.close();
    }

    @Test
    public void testWritePrometheusMetrics() throws IOException {
        // Basic JVM metrics don't create a histogram or a summary, so add two of those.
        final Histogram histogram = Histogram.build()
            .name("testHist")
            .help("The histogram")
            .register();
        histogram.observe(10);

        final Summary summary = Summary.build()
            .name("testSummary")
            .help("The summary")
            .register();
        summary.observe(10);

        final DiagnosticsWriter writer = new DiagnosticsWriter();
        final ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(file));
        writer.writePrometheusMetrics(CollectorRegistry.defaultRegistry, zos);

        zos.close();
        assertTrue(file.exists());

        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
        ZipEntry ze = zis.getNextEntry();
        assertTrue(ze.getName().equals(DiagnosticsWriter.PROMETHEUS_METRICS_FILE_NAME));

        byte[] bytes = new byte[1024];
        zis.read(bytes);
        zis.close();

        final String metrics = new String(bytes);
        assertThat(metrics, containsString("testHist"));
        assertThat(metrics, containsString("testSummary"));
    }
}
