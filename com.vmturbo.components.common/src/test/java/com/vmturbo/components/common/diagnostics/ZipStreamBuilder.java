package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Charsets;

public class ZipStreamBuilder {

    private final ZipOutputStream zipStream;
    private final ByteArrayOutputStream byteStream;

    private ZipStreamBuilder() {
        this.byteStream = new ByteArrayOutputStream();
        this.zipStream = new ZipOutputStream(byteStream);
    }

    public static ZipStreamBuilder builder() {
        return new ZipStreamBuilder();
    }

    ZipStreamBuilder withTextFile(String name, String... lines) throws IOException {
        return withFile(name, linesToBytes(lines));
    }

    ZipStreamBuilder withBinaryFile(String name, int... values) throws IOException {
        return withFile(name, intsToBytes(values));
    }

    private ZipStreamBuilder withFile(String name, byte[] content) throws IOException {
        zipStream.putNextEntry(new ZipEntry(name));
        zipStream.write(content);
        zipStream.closeEntry();
        return this;
    }

    ZipStreamBuilder withDirectory(String name) throws IOException {
        zipStream.putNextEntry(new ZipEntry(name+"/"));
        return this;
    }

    ZipStreamBuilder withEmbeddedZip(String name, ZipStreamBuilder embedded) throws IOException {
        zipStream.putNextEntry(new ZipEntry(name));
        zipStream.write(embedded.getBytes());
        return this;
    }

    public byte[] getBytes() throws IOException {
        zipStream.close();
        return byteStream.toByteArray();
    }

    InputStream toInputStream() throws IOException {
        return new ByteArrayInputStream(getBytes());
    }

    ZipInputStream toZipInputStream() throws IOException {
        return new ZipInputStream(toInputStream());
    }

    static byte[] intsToBytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    static byte[] linesToBytes(String... lines) {
        final String text = String.join("\n", lines);
        return text.getBytes(Charsets.UTF_8);
    }
}
