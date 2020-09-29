package com.vmturbo.components.api;

import java.util.Arrays;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Utility class to compress protobuf objects.
 *
 * @param <T> The type of the protobuf message.
 * @param <B> The builder type of the message.
 */
public class CompressedProtobuf<T extends AbstractMessage, B extends AbstractMessage.Builder> {

    private final byte[] compressedBytes;

    private final int uncompressedLength;

    private CompressedProtobuf(T protobuf, SharedByteBuffer buffer) {
        final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
        final byte[] uncompressedBytes = protobuf.toByteArray();
        uncompressedLength = uncompressedBytes.length;
        final int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
        final byte[] compressionBuffer = buffer.getBuffer(maxCompressedLength);
        final int compressedLength = compressor.compress(uncompressedBytes, compressionBuffer);
        this.compressedBytes = Arrays.copyOf(compressionBuffer, compressedLength);
    }

    /**
     * Create a new instance, if you already have a byte array and the uncompressed length
     * obtained from a previous use of an LZ4 compressor.
     *
     * @param compressedBytes The compressed bytes.
     * @param uncompressedLength The uncompressed length.
     */
    public CompressedProtobuf(final byte[] compressedBytes, final int uncompressedLength) {
        this.compressedBytes = compressedBytes;
        this.uncompressedLength = uncompressedLength;
    }

    /**
     * Compress a protobuf message.
     *
     * @param protobuf  The protobuf object.
     * @param <T> The type of the object.
     * @param <B> The type of the object's builder.
     * @return The {@link CompressedProtobuf} containing the compressed bytes.
     */
    public static <T extends AbstractMessage, B extends AbstractMessage.Builder> CompressedProtobuf<T, B> compress(T protobuf) {
        return compress(protobuf, new SharedByteBuffer(protobuf.getSerializedSize()));
    }

    /**
     * Compress a protobuf message.
     *
     * @param protobuf The protobuf object.
     * @param buffer A {@link SharedByteBuffer} used for the compression. Useful to avoid allocating
     *               new buffers when compressing lots of objects.
     * @param <T> The type of the object.
     * @param <B> The type of the object's builder.
     * @return The {@link CompressedProtobuf} containing the compressed bytes.
     */
    public static <T extends AbstractMessage, B extends AbstractMessage.Builder> CompressedProtobuf<T, B> compress(T protobuf, SharedByteBuffer buffer) {
        return new CompressedProtobuf<>(protobuf, buffer);
    }

    /**
     * Decompress the compressed protobuf into a builder.
     * @param bldr The builder (must be of the correct type).
     * @throws InvalidProtocolBufferException If there is an issue with decompression.
     */
    public void decompressInto(B bldr) throws InvalidProtocolBufferException {
        // Use the fastest available java instance to avoid using off-heap memory.
        final LZ4FastDecompressor decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
        bldr.mergeFrom(decompressor.decompress(compressedBytes, uncompressedLength));
    }

    public int getUncompressedLength() {
        return uncompressedLength;
    }

    public byte[] getCompressedBytes() {
        return compressedBytes;
    }

}
