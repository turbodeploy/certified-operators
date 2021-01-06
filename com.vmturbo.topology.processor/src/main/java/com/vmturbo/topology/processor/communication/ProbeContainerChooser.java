package com.vmturbo.topology.processor.communication;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Interface implemented by classes that provide a way to choose a transport to communicate with
 * when multiple probe containers of the same type are running.
 */
public interface ProbeContainerChooser {

    /**
     * Chooses transport of a target.
     * @param target the target to which the message is being sent to
     * @param message the message that is being sent to the probe
     * @return return the assigned or created ITransport
     * @throws ProbeException if probe can't be found
     */
    @Nonnull
     ITransport<MediationServerMessage, MediationClientMessage> choose(@Nonnull Target target,
                                                                       @Nonnull MediationServerMessage message) throws ProbeException;

    /**
     * Parse information from a {@link ContainerInfo}.
     * @param containerInfo the container info to parse
     * @param transport the transport associated with the container info
     */
    void parseContainerInfoWithTransport(@Nonnull ContainerInfo containerInfo,
                                         @Nonnull ITransport<MediationServerMessage,
                                             MediationClientMessage> transport);

}
