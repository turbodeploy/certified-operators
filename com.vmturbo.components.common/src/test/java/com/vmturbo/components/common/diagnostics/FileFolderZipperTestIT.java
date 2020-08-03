package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the FileFolderZipper.
 */
public class FileFolderZipperTestIT {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private String path;

    @Before
    public void setup() throws IOException {
        path = tempFolder.newFolder().getCanonicalPath();
        Files.createFile(Paths.get(path + "/file1"));
        Files.createDirectory(Paths.get(path + "/dir"));
        Files.createFile(Paths.get(path + "/dir/file2"));
    }

    @Test
    public void testZip() throws Exception {
        FileFolderZipper zipper = new FileFolderZipper();
        ByteArrayOutputStream outData = new ByteArrayOutputStream();
        ZipOutputStream out = new ZipOutputStream(outData);
        zipper.zipFilesInFolder("test", out, Paths.get(path));
        ByteArrayInputStream inData = new ByteArrayInputStream(outData.toByteArray());
        ZipInputStream in = new ZipInputStream(inData);

        Assert.assertEquals("test/dir/file2", in.getNextEntry().getName());
        Assert.assertEquals("test/file1", in.getNextEntry().getName());
    }
}
