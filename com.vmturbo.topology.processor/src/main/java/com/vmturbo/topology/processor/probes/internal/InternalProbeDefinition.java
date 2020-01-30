package com.vmturbo.topology.processor.probes.internal;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;

/**
 * Interface defines parameters for an internal probe.
 */
public interface InternalProbeDefinition {

    /**
     * Type of an internal probe.
     *
     * @return type.
     */
    @Nonnull
    String getProbeType();

    /**
     * Defines basic probes parameters: type, category, creating mode and account definition, etc.
     *
     * @return {@link ProbeInfo} instance.
     */
    @Nonnull
    ProbeInfo getProbeInfo();

    /**
     * Defines basic target parameters: probe Id, account values, weather it`s hidden, etc.
     *
     * @param probeId - ID of probe for this target.
     * @return {@link TargetSpec} instance.
     */
    @Nonnull
    TargetSpec getProbeTarget(long probeId);

    /**
     * Defines a transport for messaging with this internal probe.
     * Topology Processor uses this interface to communicate, send/receive messages.
     *
     * @return {@link ITransport} interface instance.
     */
    @Nonnull
    ITransport<MediationServerMessage, MediationClientMessage> getTransport();
}
