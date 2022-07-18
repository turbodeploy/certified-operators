package com.vmturbo.topology.processor.communication;

import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.communication.ProbeContainerChooserImpl.TransportReassignment;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Interface implemented by classes that provide a way to choose a transport to communicate with
 * when multiple probe containers of the same type are running.
 */
public interface ProbeContainerChooser {

    /**
     * Chooses transport of a target.
     *
     * @param target the target to which the message is being sent
     * @param message the message that is being sent to the probe
     * @return the assigned or created ITransport
     * @throws ProbeException if probe can't be found
     */
    @Nonnull
     ITransport<MediationServerMessage, MediationClientMessage> choose(@Nonnull Target target,
                                                                       @Nonnull MediationServerMessage message) throws ProbeException;

    /**
     * Triggered when a new transport is registered.
     *
     * @param containerInfo the container info to parse
     * @param transport the transport associated with the container info
     */
    void onTransportRegistered(@Nonnull ContainerInfo containerInfo,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport);

    /**
     * Triggered when an existing transport is removed.
     *
     * @param transport the transport which was removed
     */
    void onTransportRemoved(ITransport<MediationServerMessage, MediationClientMessage> transport);

    /**
     * Get the transport assigned to given target.
     *
     * @param targetId the pair of probe type and target id
     * @return the transport for given target
     */
    ITransport<MediationServerMessage, MediationClientMessage> getTransportByTargetId(Pair<String, String> targetId);

    /**
     * Triggered when a new target is added.
     *
     * @param target the target which was added
     * @throws ProbeException if probe can't be found
     */
    void onTargetAdded(Target target) throws ProbeException;

    /**
     * Triggered when a target is removed.
     *
     * @param target the target which was removed
     * @throws ProbeException if probe can't be found
     */
    void onTargetRemoved(Target target) throws ProbeException;

    /**
     * Register a callback which will be called when rebalance finished and target changes transport.
     *
     * @param rebalanceCallback the callback
     */
    void registerRebalanceCallback(Consumer<List<TransportReassignment>> rebalanceCallback);
}
