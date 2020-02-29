package com.vmturbo.testbases.flyway;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Class to represent a whitelist for what would normally be disallowed changes to the
 * Flyway migration sequence.
 *
 * <p>Whitelisted violations are logged but do not cause the integrity checks to fail.</p>
 *
 * <p>Each whitelist entry is tied to a specific migration version and applies to that
 * version only. Whitelist entries may also be tied to specific content signatures, though
 * currently these matter only for CHANGE entries. Signatures are the 40-hex-digit SHA-1
 * signatures produced by git (e.g. `ls-tree commit -- path-to-file).</p>
 */
public class Whitelist {

    private final Map<String, Entry> whitelist;

    /**
     * Create a new entry.
     */
    Whitelist() {
        whitelist = new HashMap<>();
    }

    /**
     * Create a new whitelist from a preconstructed map.
     *
     * @param whitelist whitelist map structure
     */
    Whitelist(Map<String, Entry> whitelist) {
        this.whitelist = whitelist;
    }

    /**
     * Construct a whitelist structure from a json or yaml file.
     *
     * @param url URL that can be parsed to yield the whitelist. Typically
     *            a value produced by {@link Class#getResource(String)}, or null
     *            for an empty whitelist
     * @return parsed whitelist, or an empty whitelist if the passed URL is null
     * @throws IOException if there's an error accessing the properties file
     */
    static Whitelist fromResource(URL url) throws IOException {
        if (url == null) {
            return Whitelist.empty();
        }
        final String path = url.getPath().toLowerCase();
        final ObjectMapper mapper;
        if (path.endsWith(".yaml")) {
            mapper = new YAMLMapper();
        } else if (path.endsWith("json")) {
            mapper = new ObjectMapper();
        } else {
            throw new IllegalArgumentException(
                    "Whitelist file name must end in .json or .yaml");
        }
        TypeReference<Map<String, Entry>> typeRef = new TypeReference<Map<String, Entry>>() {
        };
        final Map<String, Entry> map = mapper.readValue(url.openStream(), typeRef);
        return new Whitelist(map);
    }

    /**
     * Check if a given migration version is whitelisted for a given violation type and with a
     * given content signature.
     *
     * @param version       migration version number
     * @param violationType violation type
     * @param signature     git signature for content, or null if not supplied
     * @return true if the violation is whitelisted
     */
    boolean isWhitelisted(String version, ViolationType violationType, @Nullable String signature) {
        return whitelist.containsKey(version)
                && whitelist.get(version).isWhitelisted(violationType, signature);
    }

    /**
     * Check if a given migration is whitelisted for a given violation type.
     *
     * <p>No content signature is supplied, so whitelist entries that include signatures will
     * not apply.</p>
     *
     * @param version       migration version number
     * @param violationType violation type
     * @return true if the violation is whitelisted
     */
    boolean isWhitelisted(String version, ViolationType violationType) {
        return isWhitelisted(version, violationType, null);
    }

    /**
     * Construct an empty whitelist.
     *
     * @return empty whitelist
     */
    static Whitelist empty() {
        return new Whitelist();
    }

    /**
     * Check if the whitelist is empty.
     *
     * @return true if the whitelist is empty
     */
    boolean isEmpty() {
        return whitelist.isEmpty();
    }

    /**
     * Whitelist builder class.
     */
    static class Builder {
        private final ImmutableMap.Builder<String, Entry> builder;

        /**
         * Create a new builder instance.
         */
        Builder() {
            this.builder = ImmutableMap.builder();
        }

        /**
         * Add an entry to the the whitelist.
         *
         * @param version       migration version number
         * @param violationType violation type
         * @param signatures    signatures to which whitelist applies, may be empty
         * @return current builder instance
         */
        Builder whitelist(String version, ViolationType violationType, String... signatures) {
            builder.put(version, new Entry(violationType, ImmutableSet.copyOf(signatures)));
            return this;
        }

        /**
         * Construct the whitelist from the builder.
         *
         * @return built whitelist
         */
        Whitelist build() {
            return new Whitelist(builder.build());
        }
    }

    /**
     * An entry in a whitelist.
     */
    static class Entry {

        private ViolationType violationType;
        private Set<String> signatures;

        Entry() {
            this.signatures = new HashSet<>();
        }

        /**
         * Create a new instance.
         *
         * @param violationType violation type
         * @param signatures    eligible content signatures, if any
         */
        Entry(ViolationType violationType, Set<String> signatures) {
            this.violationType = violationType;
            this.signatures = signatures;
        }

        public void setViolationType(final ViolationType violationType) {
            this.violationType = violationType;
        }

        public void setSignatures(final Set<String> signatures) {
            this.signatures = signatures;
        }

        /**
         * Check if this entry matches the given violation type and signature.
         *
         * @param violationType violation type
         * @param signature     presented signature, or null for none
         * @return true if the entry matches, meaning the type matches and either
         * presented signature appears in this entry's signature list, or the signature
         * list is empty
         */
        boolean isWhitelisted(final ViolationType violationType, final String signature) {
            return (this.violationType == violationType &&
                    (signatures.isEmpty() || signatures.contains(signature)));
        }
    }

    /**
     * Violation types of whitelist entries.
     */
    public enum ViolationType {
        /** The migration file for the whitelisted version is permitted to change. */
        CHANGE,
        /**
         * The whitelisted version is permitted to enter the migration sequence at a point
         * preceding the prior end of the sequence.
         */
        INSERT,
        /** The whitelisted version is permitted to have its migration deleted. */
        DELETE
    }
}

