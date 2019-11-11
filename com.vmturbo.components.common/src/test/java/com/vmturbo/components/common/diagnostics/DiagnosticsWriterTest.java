package com.vmturbo.components.common.diagnostics;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.gson.Gson;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link DiagnosticsWriter}.
 *
 */
public class DiagnosticsWriterTest {

    private File file;
    private final Gson gson = new Gson();

    private static final int WRITE_CHUNK_SIZE = 64 * 1024;

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("diagWriter",".zip");
        file.deleteOnExit();
    }

    /**
     * Write strings to two different entries in a zip file, then read the file.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWriteZipEntries() throws IOException, DiagnosticsException {
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        Stream<String> list1 = Stream.of("A", "BB", "CCC");
        Stream<String> list2 = Stream.of("X", "Y", "Z");

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

    /**
     * Write a small bytearray to a zip file, then read the file.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWriteZipEntriesByteArray() throws IOException, DiagnosticsException {
        // arrange
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(file));
        String expected = "abcdef";
        // act
        writer.writeZipEntry("test bytearray", expected.getBytes(), zos);
        // assert
        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
        ZipEntry ze = zis.getNextEntry();
        assertEquals(ze.getName(), "test bytearray");
        char[] answer = new char[16];
        int n = new InputStreamReader(zis).read(answer);
        assertEquals(6,n);
        assertEquals(expected, new String(Arrays.copyOf(answer, n)));
    }

    /**
     * Write a large bytearray, greater than the chunk size, to a zip file, then read the file.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWriteZipEntriesLargeByteArray() throws IOException, DiagnosticsException {
        // arrange
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(file));
        final int charsToWrite = WRITE_CHUNK_SIZE + 32;
        char[] largeCharArray = new char[charsToWrite];
        Arrays.fill(largeCharArray, '.');
        String expected = new String(largeCharArray);
        final String testname = "test large bytearray";
        // act
        writer.writeZipEntry(testname, expected.getBytes(), zos);
        // assert
        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
        ZipEntry ze = zis.getNextEntry();
        assertEquals(ze.getName(), testname);
        char[] answer = new char[2 * WRITE_CHUNK_SIZE];
        int n = new InputStreamReader(zis).read(answer);
        assertEquals(charsToWrite, n);
        assertArrayEquals(largeCharArray, Arrays.copyOf(answer, n));
    }

    /**
     * Write streams of objects to two entries in a zip file, then read the file.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWriteZipEntriesStream() throws DiagnosticsException, IOException {
        // arrange
        final ClassA a1 = new ClassA("a", 1);
        final ClassA a2 = new ClassA("b", 2);
        Stream<ClassA> stream1 = Stream.of(a1, a2);

        final ClassB b1 = new ClassB(1.2, true);
        final ClassB b2 = new ClassB(3.4, false);
        Stream<ClassB> stream2 = Stream.of(b1, b2);
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(file));

        // act
        writer.writeZipEntry("Section 1", stream1, ClassA.class, zos);
        writer.writeZipEntry("Section 2", stream2, ClassB.class, zos);

        // assert
        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Section 1", ze.getName());
        BufferedReader reader = new BufferedReader(new InputStreamReader(zis));
        ClassA responseA1  = gson.fromJson(reader.readLine(), ClassA.class);
        ClassA responseA2  = gson.fromJson(reader.readLine(), ClassA.class);
        assertEquals(a1, responseA1);
        assertEquals(a2, responseA2);

        ze = zis.getNextEntry();
        assertEquals("Section 2", ze.getName());
        reader = new BufferedReader(new InputStreamReader(zis));
        ClassB responseB1  = gson.fromJson(reader.readLine(), ClassB.class);
        ClassB responseB2  = gson.fromJson(reader.readLine(), ClassB.class);
        assertEquals(b1, responseB1);
        assertEquals(b2, responseB2);
    }

    /**
     * Test IOException handling when writing a stream of objects to the zip file.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWriteZipEntriesStreamFail() throws IOException, DiagnosticsException {
        // arrange
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        ZipOutputStream zos = mock(ZipOutputStream.class);
        final String testErrorMessage = "Flush error!!!";
        doAnswer(invocation -> {
            throw new IOException(testErrorMessage);
        }).when(zos).flush();
        final ClassA a1 = new ClassA("a", 1);
        // act
        try {
            writer.writeZipEntry("test entry", Stream.of(a1), ClassA.class, zos);
        } catch (DiagnosticsException e) {
            // this exception is expected
            assertNotEquals(null, e.getCause());
            assertThat(e.getCause().getLocalizedMessage(), containsString(testErrorMessage));
            return;
        }
        // assert
        fail("Should never get here");
    }

    /**
     * Test writing Prometheus metrics and reading them back.
     *
     * @throws IOException if there is an error createing a stream for the temp file
     * @throws DiagnosticsException if there is a zip-related exception
     */
    @Test
    public void testWritePrometheusMetrics() throws IOException, DiagnosticsException {
        // Clear the registry so that only the metrics registered by this test are written
        // by the diagnostics dump.
        CollectorRegistry.defaultRegistry.clear();

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
        assertEquals(ze.getName(), DiagnosticsWriter.PROMETHEUS_METRICS_FILE_NAME);

        byte[] bytes = new byte[1024];
        zis.read(bytes);
        zis.close();

        final String metrics = new String(bytes);
        assertThat(metrics, containsString("testHist"));
        assertThat(metrics, containsString("testSummary"));
    }

    private static class ClassA {
        private String a;
        private int b;
        public ClassA(String a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ClassA classA = (ClassA) o;
            return b == classA.b &&
                Objects.equals(a, classA.a);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b);
        }
    }
    private static class ClassB {
        private Double d;
        private boolean b;
        public ClassB(Double d, boolean b) {
            this.d = d;
            this.b = b;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ClassB classB = (ClassB) o;
            return b == classB.b &&
                Objects.equals(d, classB.d);
        }

        @Override
        public int hashCode() {
            return Objects.hash(d, b);
        }
    }
}
