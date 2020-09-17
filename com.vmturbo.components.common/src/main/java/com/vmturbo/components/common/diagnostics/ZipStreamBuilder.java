package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Charsets;

/**
 * Util class to create a zipstream.
 */
public class ZipStreamBuilder {

    private final ZipOutputStream zipStream;
    private final ByteArrayOutputStream byteStream;

    private ZipStreamBuilder() {
        this.byteStream = new ByteArrayOutputStream();
        this.zipStream = new ZipOutputStream(byteStream);
    }

    /**
     * Class builder.
     *
     * @return ZipStreamBuilder the builder
     */
    public static ZipStreamBuilder builder() {
        return new ZipStreamBuilder();
    }

    /**
     * Add a text file to the builder.
     *
     * @param name the name of the file
     * @param lines the content of the file
     * @return ZipStreamBuilder the builder
     * @throws IOException if an error occurs
     */
    public ZipStreamBuilder withTextFile(String name, String... lines) throws IOException {
        return withFile(name, linesToBytes(lines));
    }

    /**
     * Add a binary file to the builder.
     *
     * @param name the name of the file
     * @param values the content of the file
     * @return ZipStreamBuilder the builder
     * @throws IOException if an error occurs
     */
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
        zipStream.putNextEntry(new ZipEntry(name + "/"));
        return this;
    }

    ZipStreamBuilder withEmbeddedZip(String name, ZipStreamBuilder embedded) throws IOException {
        zipStream.putNextEntry(new ZipEntry(name));
        zipStream.write(embedded.getBytes());
        return this;
    }

    private byte[] getBytes() throws IOException {
        zipStream.close();
        return byteStream.toByteArray();
    }

    /**
     * Transform the zip stream into an InputStream.
     *
     * @return InputStream the InputStream
     * @throws IOException if an error occurs
     */
    public InputStream toInputStream() throws IOException {
        return new ByteArrayInputStream(getBytes());
    }

    ZipInputStream toZipInputStream() throws IOException {
        return new ZipInputStream(toInputStream());
    }

    static byte[] intsToBytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)values[i];
        }
        return bytes;
    }

    static byte[] linesToBytes(String... lines) {
        final String text = String.join("\n", lines);
        return text.getBytes(Charsets.UTF_8);
    }
}
