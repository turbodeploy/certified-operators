package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.diagnostics.Diags.UncompressedDiags;

public class DiagsZipReaderTest extends Assert {

    @Test
    public void testDiagsZipReader() throws IOException {
        ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withTextFile("a.diags", "a", "b", "c")
            .withBinaryFile("b.binary", 1, 2, 3)
            .withEmbeddedZip("zip.zip", ZipStreamBuilder.builder()
                .withTextFile("a.txt", "hello a")
                .withBinaryFile("c.binary", 1, 2, 3)
                .withDirectory("x")
                .withTextFile("x/d.txt", "hello d")
                .withTextFile("e.diags", "e", "f", "g")
            )
            .withBinaryFile("f.binary", 1, 2, 3)
            .withTextFile("g.diags", "g", "h", "i");
        checkDiags(builder, new Object[][]{
            {"a.diags", bytes("a", "b", "c")},
            {"b.binary", bytes(1, 2, 3)},
            {"c.binary", bytes(1, 2, 3)},
            {"e.diags", bytes("e", "f", "g")},
            {"f.binary", bytes(1, 2, 3)},
            {"g.diags", bytes("g", "h", "i")}
        });
    }

    private void checkDiags(ZipStreamBuilder zipBuilder, Object[][] expected) throws IOException {
        DiagsZipReader diags = new DiagsZipReader(zipBuilder.toInputStream());
        Iterator<Diags> diagsIter = diags.iterator();
        for (final Object[] objects : expected) {
            assertTrue(diagsIter.hasNext());
            Diags actualDiags = diagsIter.next();
            String name = (String) objects[0];
            Object content = objects[1];
            boolean isText = content instanceof List;
            byte[] castContent = (byte[]) content;
            Diags expectedDiags = new UncompressedDiags(name, castContent);
            assertEquals("Incorrect diags name",
                expectedDiags.getName(), actualDiags.getName());
            if (isText) {
                assertEquals(expectedDiags.getLines(), actualDiags.getLines());
            } else {
                assertArrayEquals(expectedDiags.getBytes(), actualDiags.getBytes());
            }
        }
        assertFalse(diagsIter.hasNext());
    }

    private byte[] bytes(int... values) {
        return ZipStreamBuilder.intsToBytes(values);
    }

    private byte[] bytes(String... values) {
        return ZipStreamBuilder.linesToBytes(values);
    }
}
