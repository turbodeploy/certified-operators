package com.vmturbo.ml.datastore.influx;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Utility for obfuscating sensitive strings
 */
public interface Obfuscator {
    /**
     * Obfuscate a string so that it is no longer human-readable.
     * Invariant: When given the same string, the call to obfuscate should be the same as prior calls
     *            with the same string. This must be persistently true across component restarts.
     *
     * @param toObfuscate The string to obfuscate.
     * @return The result of obfuscating the string.
     */
    @Nonnull
    String obfuscate(@Nonnull final String toObfuscate);

    class HashingObfuscator implements Obfuscator {

        private final HashFunction hashFunction;
        private static final Logger logger = LogManager.getLogger();

        public HashingObfuscator() {
            hashFunction = Hashing.hmacMd5("ml-datastore".getBytes());
        }

        @Nonnull
        @Override
        public String obfuscate(@Nonnull String toObfuscate) {
            return hashFunction
                .hashString(toObfuscate, StandardCharsets.UTF_8)
                .toString();
        }
    }
}
