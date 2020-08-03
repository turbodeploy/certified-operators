package com.vmturbo.mediation.diagnostic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link MediationDiagnosticsHandler}.
 */
public class MediationDiagnosticsHandlerTest {

    private static final String FILE1 = "data.file.1";
    private static final String FILE2 = "data.file.2.a.little.bit.longer";

    /**
     * Temporary folder to store files for dumps.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private File file1;
    private File file2;
    private MediationDiagnosticsHandler handler;

    /**
     * Setup tests.
     *
     * @throws IOException on error operating with filesystem
     */
    @Before
    public void init() throws IOException {
        file1 = tmpFolder.newFile(FILE1);
        file2 = tmpFolder.newFile(FILE2);
        writeBinaryFile(file1);
        writeBinaryFile(file2);
        handler = new MediationDiagnosticsHandler(tmpFolder.getRoot().getAbsolutePath());
    }

    /**
     * Tests dumping 2 binary files.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testWrite2Files() throws Exception {
        final byte[] data = dumpDiags();
        final Map<String, Integer> entries = getZipEntries(data);
        Assert.assertEquals(Sets.newHashSet(file1.toString(), file2.toString()), entries.keySet());
        Assert.assertEquals(1036, (int)entries.get(file1.toString()));
        Assert.assertEquals(1056, (int)entries.get(file2.toString()));
    }

    /**
     * Tests dumping file in subdirectory.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testWriteDirectory() throws Exception {
        final File subdir = tmpFolder.newFolder("some-folder");
        final File file3 = new File(subdir, "internal-file");
        writeBinaryFile(file3);
        final byte[] data = dumpDiags();
        final Map<String, Integer> entries = getZipEntries(data);
        Assert.assertEquals(Sets.newHashSet(file1.toString(), file2.toString(), file3.toString()),
                entries.keySet());
        Assert.assertEquals(1036, (int)entries.get(file1.toString()));
        Assert.assertEquals(1056, (int)entries.get(file2.toString()));
        Assert.assertEquals(1038, (int)entries.get(file3.toString()));
    }

    private void writeBinaryFile(@Nonnull File file) throws IOException {
        try (OutputStream fis = new FileOutputStream(file)) {
            fis.write(file.getName().getBytes());
            fis.write('\n');
            for (int i = 0; i < 1024; i++) {
                fis.write(i);
            }
        }
    }

    @Nonnull
    private byte[] dumpDiags() throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(os)) {
            handler.dump(zos);
        }
        return os.toByteArray();
    }

    @Nonnull
    private Map<String, Integer> getZipEntries(@Nonnull byte[] data) throws IOException {
        final ByteArrayInputStream is = new ByteArrayInputStream(data);
        final Map<String, Integer> result = new HashMap<>();
        try (ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                final byte[] entryData = IOUtils.toByteArray(zis);
                result.put(entry.getName(), entryData.length);
                entry = zis.getNextEntry();
            }
        }
        return result;
    }
}
