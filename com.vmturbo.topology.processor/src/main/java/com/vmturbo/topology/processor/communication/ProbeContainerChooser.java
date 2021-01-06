package com.vmturbo.topology.processor.communication;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.probes.ProbeException;

/**
 * Interface implemented by classes that provide a way to choose a transport to communicate with
 * when multiple probe containers of the same type are running.
 */
public interface ProbeContainerChooser {

    /**
     * Chooses transport of a target.
     * @param  probeId the id of the probe
     * @param targetIdentifyingValues the serialized identifying field of a target
     * @param message the message that is being sent to the probe
     * @return return the assigned or created ITransport
     * @throws ProbeException if probe can't be found
     */
    @Nonnull
     ITransport<MediationServerMessage, MediationClientMessage> choose(long probeId,
                                                                       @Nonnull String targetIdentifyingValues,
                                                                       @Nonnull MediationServerMessage message) throws ProbeException;

    /**
     * Assign the transport to a target.
     *
     * @param  transport the transport to assign of the probe
     * @param probeType the probeType for the target represented by targetIdentifyingValues
     * @param targetIdentifyingValues the serialized identifying field of a target
     */
     void assignTargetToTransport(@Nonnull ITransport<MediationServerMessage,
         MediationClientMessage> transport, @Nonnull String probeType,
             @Nonnull String targetIdentifyingValues);

}
