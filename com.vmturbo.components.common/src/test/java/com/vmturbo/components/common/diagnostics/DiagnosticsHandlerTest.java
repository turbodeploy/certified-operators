package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;

/**
 * Unit test for {@link DiagnosticsHandler}.
 */
public class DiagnosticsHandlerTest {

    private static final String FILE1 = "read-only";
    private static final String FILE2 = "read-restore";
    private static final String FILE_BINARY = "some-file.binary";
    private static final List<String> CONTENTS1 =
            Arrays.asList("this is a line", "this is the second");
    private static final List<String> CONTENTS2 =
            Arrays.asList("There was once a grasshopper", "Imagine my friend", "Imagine my friend",
                    "Like cucumber", "So green");

    private StringDiagnosable diagnosable;
    private DiagsRestorable<Void> restorable;
    private DiagsZipReaderFactory zipReaderFactory;

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Initializes test variables.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void init() throws Exception {
        diagnosable = Mockito.mock(StringDiagnosable.class);
        initDiagnosable(diagnosable, FILE1, CONTENTS1);
        restorable = Mockito.mock(DiagsRestorable.class);
        initDiagnosable(restorable, FILE2, CONTENTS2);
        zipReaderFactory = new DefaultDiagsZipReader();
    }

    /**
     * Tests dumping and restoring of diagnostics.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testDumping() throws Exception {
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(diagnosable, restorable));
        final byte[] bytes = collecteDiags(handler);
        Mockito.verify(diagnosable).collectDiags(Mockito.any());
        Mockito.verify(restorable).collectDiags(Mockito.any());
        Assert.assertEquals(Arrays.asList(FILE1, FILE2 + DiagsZipReader.TEXT_DIAGS_SUFFIX),
                new ArrayList<>(getZipEntries(bytes).keySet()));
        restoreDiags(handler, bytes);
        Mockito.verify(restorable).restoreDiags(Mockito.any(), Mockito.any());
    }

    /**
     * Test how exception is treated. It is expected, that other files are also exported after
     * the 1st diagnostics exception is faced.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testOneThrowsException() throws Exception {
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(diagnosable, restorable));
        final String exceptionMessage = "1st exception";
        Mockito.doThrow(new DiagnosticsException(exceptionMessage))
                .when(diagnosable)
                .collectDiags(Mockito.any());
        final byte[] bytes = collecteDiags(handler);
        Mockito.verify(diagnosable).collectDiags(Mockito.any());
        Mockito.verify(restorable).collectDiags(Mockito.any());
        final Map<String, List<String>> zipEntires = getZipEntries(bytes);
        Assert.assertEquals(Arrays.asList(FILE1, FILE2 + DiagsZipReader.TEXT_DIAGS_SUFFIX,
                DiagnosticsHandler.ERRORS_FILE), new ArrayList<>(zipEntires.keySet()));
        Assert.assertEquals(Collections.singletonList(exceptionMessage),
                zipEntires.get(DiagnosticsHandler.ERRORS_FILE));
    }

    /**
     * Tests restoring diagnostics faced a error. It is expected to still load all the other files.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testRestoreFailed() throws Exception {
        final DiagsRestorable failing = Mockito.mock(DiagsRestorable.class);
        initDiagnosable(failing, FILE1, CONTENTS1);
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(failing, restorable));
        final byte[] bytes = collecteDiags(handler);
        final String restoreException = "Restore failed";
        Mockito.doThrow(new DiagnosticsException(restoreException))
                .when(failing)
                .restoreDiags(Mockito.any(), Mockito.any());
        try {
            restoreDiags(handler, bytes);
        } catch (DiagnosticsException e) {
            // NOOP This is expected
        }
        Mockito.verify(failing).restoreDiags(Mockito.any(), Mockito.any());
        @SuppressWarnings("unchecked") final ArgumentCaptor<List<String>> captor =
                (ArgumentCaptor<List<String>>)ArgumentCaptor.forClass((Class)List.class);
        Mockito.verify(restorable).restoreDiags(captor.capture(), Mockito.any());
        Assert.assertEquals(CONTENTS2, captor.getValue());
    }

    /**
     * Tests how data is restored for binary diagnostics.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testBinaryDiagnostics() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final BinaryDiagsRestorable diagnosable = Mockito.mock(BinaryDiagsRestorable.class);
        initDiagnosable(diagnosable, "some file", data);
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(diagnosable, restorable));
        final byte[] diags = collecteDiags(handler);
        restoreDiags(handler, diags);
        final ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        Mockito.verify(diagnosable, Mockito.atLeastOnce()).restoreDiags(captor.capture(),
            Mockito.any());
        Assert.assertArrayEquals(data, captor.getValue());
    }

    /**
     * Tests the case of binary dumping throws an exception. Subsequent diagnosables are expected
     * to still be dumped.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testBinaryDiagnosticsDumpExceptions() throws Exception {
        final BinaryDiagsRestorable diagnosable = Mockito.mock(BinaryDiagsRestorable.class);
        initDiagnosable(diagnosable, FILE_BINARY, new byte[]{0, 1, 2});
        Mockito.doThrow(new DiagnosticsException("some exception"))
                .when(diagnosable)
                .collectDiags(Mockito.any());
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(diagnosable, restorable));
        final byte[] diags = collecteDiags(handler);
        final Set<String> entries = getZipEntries(diags).keySet();
        Assert.assertTrue(entries.contains(FILE_BINARY + DiagsZipReader.BINARY_DIAGS_SUFFIX));
        Mockito.verify(diagnosable, Mockito.atLeastOnce()).collectDiags(Mockito.any());
        Mockito.verify(restorable, Mockito.atLeastOnce()).collectDiags(Mockito.any());
    }

    /**
     * Tests the case of binary restoring throws an exception. Subsequent diagnosables are expected
     * to still be restored.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testBinaryDiagnosticsRestoreExceptions() throws Exception {
        final BinaryDiagsRestorable diagnosable = Mockito.mock(BinaryDiagsRestorable.class);
        initDiagnosable(diagnosable, FILE_BINARY, new byte[]{0, 1, 2});
        Mockito.doThrow(new DiagnosticsException("some exception"))
                .when(diagnosable)
                .restoreDiags(Mockito.any(), Mockito.any());
        final DiagnosticsHandlerImportable handler =
                new DiagnosticsHandlerImportable(zipReaderFactory,
                        Arrays.asList(diagnosable, restorable));
        final byte[] diags = collecteDiags(handler);
        List<String> errors;
        try {
            restoreDiags(handler, diags);
            Assert.fail("Exception expected");
            errors = null;
        } catch (DiagnosticsException e) {
            errors = e.getErrors();
        }
        Assert.assertEquals(Collections.singletonList("some exception"), errors);
        Mockito.verify(diagnosable, Mockito.atLeastOnce()).restoreDiags(Mockito.any(), Mockito.any());
        Mockito.verify(restorable, Mockito.atLeastOnce()).restoreDiags(Mockito.any(), Mockito.any());
    }

    /**
     * Tests when unknown diagnosable is passed to diagnostics handler (neither binary nor sting).
     */
    @Test
    public void testUnrecognizedDiagnosable() {
        final Diagnosable diagnosable = Mockito.mock(Diagnosable.class);
        Mockito.when(diagnosable.getFileName()).thenReturn(FILE1);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Diagnosable could not be processed");
        new DiagnosticsHandler(Collections.singletonList(diagnosable));
    }

    @Nonnull
    private byte[] collecteDiags(@Nonnull DiagnosticsHandlerImportable handler) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            try (ZipOutputStream zos = new ZipOutputStream(os)) {
                handler.dump(zos);
                return os.toByteArray();
            }
        }
    }

    @Nonnull
    private String restoreDiags(@Nonnull DiagnosticsHandlerImportable handler,
            @Nonnull byte[] bytes) throws IOException, DiagnosticsException {
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            return handler.restore(is, null);
        }
    }

    @Nonnull
    private Map<String, List<String>> getZipEntries(@Nonnull byte[] bytes) throws IOException {
        final Map<String, List<String>> entries = new LinkedHashMap<>();
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            try (ZipInputStream zis = new ZipInputStream(is)) {
                ZipEntry nextEntry = zis.getNextEntry();
                while (nextEntry != null) {
                    final List<String> lines = IOUtils.readLines(zis, Charsets.UTF_8);
                    entries.put(nextEntry.getName(), lines);
                    nextEntry = zis.getNextEntry();
                }
            }
        }
        return entries;
    }

    private static void initDiagnosable(@Nonnull StringDiagnosable diagnosable,
            @Nonnull String filename, @Nonnull List<String> contents) throws DiagnosticsException {
        Mockito.when(diagnosable.getFileName()).thenReturn(filename);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final DiagnosticsAppender appender =
                        invocation.getArgumentAt(0, DiagnosticsAppender.class);
                for (String line : contents) {
                    appender.appendString(line);
                }
                return null;
            }
        }).when(diagnosable).collectDiags(Mockito.any());
    }

    private static void initDiagnosable(@Nonnull BinaryDiagnosable diagnosable,
            @Nonnull String filename, @Nonnull byte[] contents)
            throws DiagnosticsException, IOException {
        Mockito.when(diagnosable.getFileName()).thenReturn(filename);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final OutputStream appender = invocation.getArgumentAt(0, OutputStream.class);
                appender.write(contents);
                return null;
            }
        }).when(diagnosable).collectDiags(Mockito.any());
    }
}
