package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.commons.io.IOUtils;

import com.vmturbo.components.api.SharedByteBuffer;

/**
 * A diagnostics item. Usually the content of one zip entry in a zip file.
 * Use with {@link DiagsZipReader} to iterate over the content of a zip file.
 * The iterator returns an instance of Diags for every zipped file. It also
 * handles nested zip files.
 */
public interface Diags {

    /**
     * The name of the diagnostics which is the name of the corresponding ZipEntry.
     *
     * @return the name of the diagnostics
     */
    String getName();

    /**
     * If the diags entry is a text file, returns the lines. If it's a binary file, returns null.
     *
     * @return the content of the diagnostics as a list of lines, or null.
     */
    @Nullable
    List<String> getLines();

    /**
     * If the diags entry is a binary file, returns the contents. If it's a text file, returns null.
     *
     * @return The contents of the binary file, or null.
     */
    @Nullable
    byte[] getBytes();

    /**
     * The content of the diagnostics item as one string.
     *
     * @return the content of the diagnostics as one string
     */
    String getContent();

    /**
     * A {@link Diags} implementation which compresses the content in-memory, and decompresses
     * it on demand when we want to access the contents.
     */
    class CompressedDiags implements Diags {
        private final String name;
        private final byte[] compressedBytes;
        private final int uncompressedLength;

        /**
         * Create a new instance.
         *
         * @param name The name of the zip entry.
         * @param content The contents of the zip entry.
         * @param sharedByteBuffer A {@link SharedByteBuffer} to use for compression.
         */
        public CompressedDiags(@Nonnull final String name,
                final byte[] content,
                @Nonnull final SharedByteBuffer sharedByteBuffer) {
            this.name = name;
            final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
            uncompressedLength = content.length;
            final int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
            final byte[] compressionBuffer = sharedByteBuffer.getBuffer(maxCompressedLength);
            final int compressedLength = compressor.compress(content, compressionBuffer);
            this.compressedBytes = Arrays.copyOf(compressionBuffer, compressedLength);
        }

        @Nonnull
        private UncompressedDiags getDecompressed() {
            // Supplier doesn't return null, so this will never return null.
            final LZ4FastDecompressor decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
            byte[] uncompressed = decompressor.decompress(compressedBytes, uncompressedLength);
            try {
                return new UncompressedDiags(name, uncompressed);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Nullable
        @Override
        public List<String> getLines() {
            return getDecompressed().getLines();
        }

        @Nullable
        @Override
        public byte[] getBytes() {
            return getDecompressed().getBytes();
        }

        @Override
        public String getContent() {
            return getDecompressed().getContent();
        }

        @Override
        public String toString() {
            final String description;
            return name + " : compressed";
        }
    }

    /**
     * A {@link Diags} implementation which keeps the diags data in memory.
     */
    class UncompressedDiags implements Diags {
        private final String name;
        private final List<String> lines;
        private final byte[] bytes;

        UncompressedDiags(String name, byte[] content) throws IOException {
            this.name = name;
            if (Diags.isTextDiags(name)) {
                this.lines = IOUtils.readLines(new ByteArrayInputStream(content),
                        Charsets.UTF_8);
                this.bytes = null;
            } else if (Diags.isBinaryDiags(name)) {
                this.lines = null;
                this.bytes = content;
            } else {
               // should never happen, but if it does, treat it as an iterator-terminating condition
               throw new IllegalArgumentException("Invalid file extension for diags entry: " + name);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        @Nullable
        public List<String> getLines() {
            return lines;
        }

        @Override
        @Nullable
        public byte[] getBytes() {
            return bytes;
        }

        @Override
        public String getContent() {
            return String.join("\n", lines);
        }

        @Override
        public String toString() {
            final String description;
            if (lines != null) {
                description = lines.toString();
            } else if (bytes != null) {
                description = bytes.length + "bytes";
            } else {
                description = null;
            }
            return name + " : " + description;
        }
    }

    /**
     * Check whether the given zip file entry looks like a text diags entry.
     *
     * @param entryName the name entry to be checked.
     * @return true if it looks like a text diags entry.
     */
    static boolean isTextDiags(@Nonnull final String entryName) {
        return entryName.toLowerCase().endsWith(DiagsZipReader.TEXT_DIAGS_SUFFIX);
    }

    /**
     * Check whether the given zip file entry looks like a binary diags entry.
     *
     * @param entryName the name of the entry to be checked.
     * @return true if it looks like a binary diags entry.
     */
    static boolean isBinaryDiags(@Nonnull final String entryName) {
        return entryName.toLowerCase().endsWith(DiagsZipReader.BINARY_DIAGS_SUFFIX);
    }
}
