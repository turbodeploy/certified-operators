package com.vmturbo.auth.api;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

/**
 * The test for {@link Base64CodecUtils}.
 */
public class Base64CodecUtilsTest {

    @Test
    public void testEncodeAndDecode() throws Exception {
        String toWrite = "Hello";
        String encodedStre = Base64CodecUtils.encode(toWrite.getBytes());
        byte[] bytes = Base64CodecUtils.decode(encodedStre);
        File tmpFile = File.createTempFile("test1", ".tmp");
        Path path = Paths.get(tmpFile.getAbsolutePath());
        java.nio.file.Files.write(path, bytes);
        BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
        assertEquals(toWrite, reader.readLine());
        reader.close();
        tmpFile.deleteOnExit();
    }
}