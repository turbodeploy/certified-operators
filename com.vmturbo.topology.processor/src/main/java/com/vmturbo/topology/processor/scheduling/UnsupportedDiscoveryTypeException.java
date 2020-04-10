package com.vmturbo.topology.processor.scheduling;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;

/**
 * Exception thrown when a discovery type is not supported at some point in scheduling.
 */
public class UnsupportedDiscoveryTypeException extends Exception {

    /**
     * Constructor for {@link UnsupportedDiscoveryTypeException}.
     *
     * @param discoveryType type of the discovery which is not supported
     * @param probeType probe type for which the given discovery type is not supported
     */
    public UnsupportedDiscoveryTypeException(@Nonnull DiscoveryType discoveryType,
                                             @Nonnull String probeType) {
        super("Discovery type: " + discoveryType + " is not supported for probe type: " + probeType);
    }
}
