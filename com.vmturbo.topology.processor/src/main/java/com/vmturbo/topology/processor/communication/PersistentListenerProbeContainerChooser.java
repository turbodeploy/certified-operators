package com.vmturbo.topology.processor.communication;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;

/**
 * Class for choosing a ITransport to use to communicate with a probe when multiple probes of the
 * same type are running.  This {@link ProbeContainerChooser} chooses an ITransport if it is
 * already assigned to a targetId. If not, it chooses the next ITransport and assigns it to that
 * targetId
 */
public class PersistentListenerProbeContainerChooser extends RoundRobinProbeContainerChooser {

    private final Logger logger = LogManager.getLogger();

    private Map<Long, ITransport<MediationServerMessage, MediationClientMessage>> targetIdToTransport =
        new HashMap<>();


    @Override
    public ITransport<MediationServerMessage, MediationClientMessage> choose(
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>>
            transportCollection) {
        return super.choose(transportCollection);
    }

    /**
     * Assign or fetch the transport of a target
     * @param  targetId the id of the target
     * @param transportCollection the collection of transports
     */
    public ITransport<MediationServerMessage, MediationClientMessage> getOrCreateTransportForTarget(
        final long targetId,
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>>
            transportCollection) {
        targetIdToTransport.computeIfAbsent(targetId, k -> choose(transportCollection));
        return targetIdToTransport.get(targetId);
        }
    }
