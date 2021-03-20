package com.vmturbo.topology.processor.operation.discovery;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An discovery operation on a target.
 */
public class Discovery extends Operation {

    /**
     * Type of the discovery. Currently only FULL and INCREMENTAL are supported.
     */
    private final DiscoveryType discoveryType;

    /**
     * The unique id of the mediation message sent to probe for this discovery. It's guaranteed
     * that discovery which happens later get a larger value.
     */
    private int mediationMessageId;

    private static final DataMetricSummary DISCOVERY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_discovery_duration_seconds")
        .withHelp("Duration of a discovery in the Topology Processor.")
        .build()
        .register();

    private static final DataMetricCounter DISCOVERY_STATUS_COUNTER = DataMetricCounter.builder()
        .withName("tp_discovery_status_total")
        .withHelp("Status of all completed discoveries.")
        .withLabelNames("status")
        .build()
        .register();

    /**
     * Constructor for creating a Discovery with default discovery type "FULL".
     *
     * @param probeId The id of the probe that will be doing the discovery.
     * @param targetId The id of the target being discovered.
     * @param identityProvider The identity provider to use to get an ID.
     */
    public Discovery(final long probeId,
                     final long targetId,
                     @Nonnull final IdentityProvider identityProvider) {
        this(probeId, targetId, DiscoveryType.FULL, identityProvider);
    }

    /**
     * Constructor for creating a Discovery with the given discovery type.
     *
     * @param probeId The id of the probe that will be doing the discovery.
     * @param targetId The id of the target being discovered.
     * @param discoveryType The type of the discovery
     * @param identityProvider The identity provider to use to get an ID.
     */
    public Discovery(final long probeId,
                     final long targetId,
                     final DiscoveryType discoveryType,
                     @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, DISCOVERY_DURATION_SUMMARY);
        this.discoveryType = discoveryType;
    }

    @Override
    public String toString() {
        return new StringBuilder()
            .append(discoveryType)
            .append(" Discovery ")
            .append(super.toString()).toString();
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return DISCOVERY_STATUS_COUNTER;
    }

    @Nonnull
    @Override
    public String getErrorString() {
        return "Discovery failed: " + String.join(", ", getErrors());
    }

    public DiscoveryType getDiscoveryType() {
        return discoveryType;
    }

    public int getMediationMessageId() {
        return mediationMessageId;
    }

    public void setMediationMessageId(int mediationMessageId) {
        this.mediationMessageId = mediationMessageId;
    }
}
