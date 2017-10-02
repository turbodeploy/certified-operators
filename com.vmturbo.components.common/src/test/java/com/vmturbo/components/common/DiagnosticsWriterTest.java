package com.vmturbo.components.common;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test cases for {@link DiagnosticsWriter}.
 *
 */
public class DiagnosticsWriterTest {

    @Test
    /**
     * Write strings to a zip file, then read the file.
     * @throws IOException
     */
    public void testWriteZipEntries() throws IOException {
        final DiagnosticsWriter writer = new DiagnosticsWriter();
        List<String> list1 = Lists.newArrayList("A", "BB", "CCC");
        List<String> list2 = Lists.newArrayList("X", "Y", "Z");
        File f = new File("test.zip");
        f.deleteOnExit();

        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(f));
        writer.writeZipEntry("Test1", list1, zos);
        writer.writeZipEntry("Test2", list2, zos);
        zos.close();
        assertTrue(f.exists());

        ZipInputStream zis = new ZipInputStream(new FileInputStream(f));
        ZipEntry ze = zis.getNextEntry();
        // Test that the first entry is read properly
        assertTrue(ze.getName().equals("Test1"));
        byte[] bytes = new byte[20];
        zis.read(bytes);
        assertEquals("A\nBB\nCCC\n", new String(bytes, 0, 9));
        assertEquals(0, bytes[9]);
        zis.close();
    }
}
