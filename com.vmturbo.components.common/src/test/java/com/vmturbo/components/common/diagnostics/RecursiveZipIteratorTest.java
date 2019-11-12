 package com.vmturbo.components.common.diagnostics;

 import static com.vmturbo.components.common.diagnostics.RecursiveZipIteratorTest.ENTRY_TYPE.BINARY;
 import static com.vmturbo.components.common.diagnostics.RecursiveZipIteratorTest.ENTRY_TYPE.DIR;
 import static com.vmturbo.components.common.diagnostics.RecursiveZipIteratorTest.ENTRY_TYPE.TEXT;

 import java.io.IOException;
 import java.util.Arrays;
 import java.util.stream.Collectors;
 import java.util.stream.Stream;

 import com.google.common.base.Charsets;

 import org.junit.Assert;
 import org.junit.Test;

 import com.vmturbo.components.common.diagnostics.RecursiveZipIterator.WrappedZipEntry;

public class RecursiveZipIteratorTest extends Assert {
    enum ENTRY_TYPE {TEXT, BINARY, DIR}

    @Test
    public void testBasicZipIterator() throws IOException {
        final ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withTextFile("a.txt", "aaaa")
            .withBinaryFile("xxx.bin", 10, 20, 30, 40, 50)
            .withTextFile("abc.txt", "aaaaa", "bbbbb", "ccccc")
            .withTextFile("empty.txt")
            .withBinaryFile("empty.bin")
            .withDirectory("foo")
            .withTextFile("foo/bar.txt", "foo", "bar");
        testZipIterator(builder, new Object[][] {
            {TEXT, "a.txt", "aaaa"},
            {BINARY, "xxx.bin", 10, 20, 30, 40, 50},
            {TEXT, "abc.txt", "aaaaa", "bbbbb", "ccccc"},
            {TEXT, "empty.txt"},
            {BINARY, "empty.bin"},
            {DIR, "foo/"},
            {TEXT, "foo/bar.txt", "foo", "bar"}
            });
    }

    @Test
    public void testRecursiveZip() throws IOException {
        final ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withTextFile("a.txt", "hello a")
            .withEmbeddedZip("zip1.zip", ZipStreamBuilder.builder()
                .withEmbeddedZip(" zip2.zip", ZipStreamBuilder.builder()
                    .withTextFile("b.txt", "hello b")
                )
                .withTextFile("c.txt", "hello c")
            )
            .withBinaryFile("d.bin", 1, 2, 3)
            .withEmbeddedZip("zip3.zip", ZipStreamBuilder.builder()
                .withTextFile("e.txt", "hello e")
                .withEmbeddedZip("zip4.zip", ZipStreamBuilder.builder()
                    .withTextFile("f.txt", "hello f")
                )
            )
            .withBinaryFile("g.bin", 1, 2, 3);
        testZipIterator(builder, new Object[][] {
            {TEXT, "a.txt", "hello a"},
            {TEXT, "b.txt", "hello b"},
            {TEXT, "c.txt", "hello c"},
            {BINARY, "d.bin", 1, 2, 3},
            {TEXT, "e.txt", "hello e"},
            {TEXT, "f.txt", "hello f"},
            {BINARY, "g.bin", 1, 2, 3}
        });
    }



    private void testZipIterator(ZipStreamBuilder builder, Object[][] expected) throws IOException {
        RecursiveZipIterator zip = new RecursiveZipIterator(builder.toZipInputStream());
        for (final Object[] objects : expected) {
            assertTrue(zip.hasNext());
            WrappedZipEntry entry = zip.next();
            checkEntry(entry, objects);
        }
        assertFalse(zip.hasNext());
    }

    private void checkEntry(WrappedZipEntry entry, Object[] expected) throws IOException {
        switch((ENTRY_TYPE) expected[0]) {
            case TEXT:
                checkTextEntry(entry, expected);
                break;
            case BINARY:
                checkBinaryEntry(entry, expected);
                break;
            case DIR:
                checkDirectoryEntry(entry, expected);
                break;
        }
    }

    private void checkTextEntry(WrappedZipEntry entry, Object[] expected) throws IOException {
        checkName(entry.getName(), (String) expected[1]);
        String expectedText = Stream.of(Arrays.copyOfRange(expected, 2, expected.length))
            .map(line -> (String) line)
            .collect(Collectors.joining("\n"));
        String actualText = new String(entry.getContent(), Charsets.UTF_8);
        assertEquals("Unexpected text content", expectedText, actualText);
    }

    private void checkBinaryEntry(WrappedZipEntry entry, Object[] expected) throws IOException {
        checkName(entry.getName(), (String) expected[1]);
        byte[] bytes = new byte[expected.length-2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((int) ((Integer) expected[i+2]));
        }
        assertArrayEquals("Unexpected binary content", bytes,  entry.getContent());
    }

    private void checkDirectoryEntry(WrappedZipEntry entry, Object[] expected) {
        checkName(entry.getName(), (String) expected[1]);
        assertEquals(2, expected.length);
    }

    private void checkName(String actual, String expected) {
        assertEquals("Wrong entry name", expected, actual);
    }
}
