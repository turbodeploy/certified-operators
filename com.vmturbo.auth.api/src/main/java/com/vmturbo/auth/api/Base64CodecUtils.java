package com.vmturbo.auth.api;

import javax.annotation.Nonnull;

import io.jsonwebtoken.impl.Base64Codec;

/**
 * The BinaryCodec implements encoding/decoding byte array to Base64 String.
 */
public class Base64CodecUtils {

    /**
     * This class must not be instantiated.
     */
    private Base64CodecUtils() {
    }

    /**
     * Encode bytes to Base64 string
     *
     * @param bytes bytes to encode
     * @return Base64 encoded string
     */
    public static @Nonnull
    String encode(@Nonnull byte[] bytes) {
        assert bytes.length > 0;
        StringBuilder sb = new StringBuilder();
        Base64Codec codec = new Base64Codec();
        sb.append(codec.encode(bytes));
        return sb.toString();
    }

    /**
     * Decode Base64 encoded string to bytes
     *
     * @param encodedString Base64 encoded string
     * @return bytes
     */
    public static @Nonnull
    byte[] decode(final @Nonnull String encodedString) {
        Base64Codec codec = new Base64Codec();
        return codec.decode(encodedString);
    }
}
