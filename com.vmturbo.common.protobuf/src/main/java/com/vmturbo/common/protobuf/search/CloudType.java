package com.vmturbo.common.protobuf.search;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

/**
 * Cloud types enumeration.
 */
public enum CloudType {
    /**
     * Amazon Web Services.
     */
    AWS,
    /**
     * Microsoft Azure.
     */
    AZURE,
    /**
     * Google Cloud.
     */
    GCP;

    private static Map<String, CloudType> cloudTypeByProbeType =
            new ImmutableMap.Builder<String, CloudType>().put("aws", AWS)
                    .put("aws billing", AWS)
                    .put("aws cost", AWS)
                    .put("aws lambda", AWS)
                    .put("azure subscription", AZURE)
                    .put("azure ea", AZURE)
                    .put("azure service principal", AZURE)
                    .put("azure cost", AZURE)
                    .put("azure wasted volumes", AZURE)
                    .put("gcp", GCP)
                    .put("gcp cost", GCP)
                    .build();

    /**
     * Gets cloud type from string representation.
     *
     * @param src string to search
     * @return cloud type, if any.
     */
    public static Optional<CloudType> fromString(@Nonnull String src) {
        for (CloudType cloudType : CloudType.values()) {
            if (cloudType.name().equals(src)) {
                return Optional.of(cloudType);
            }
        }
        return Optional.empty();
    }

    /**
     * Return the cloud type of the probe type passed in.  Returns AWS or AZURE for probes of the
     * relevant types.  Otherwise returns UNKNOWN.
     *
     * @param probeType String representing the probe type.
     * @return AWS or AZURE if the probe type is recognized as one of those types or else UNKNOWN.
     */
    public static Optional<CloudType> fromProbeType(@Nonnull String probeType) {
        Objects.requireNonNull(probeType);
        return Optional.ofNullable(cloudTypeByProbeType.get(probeType.toLowerCase()));
    }
}
