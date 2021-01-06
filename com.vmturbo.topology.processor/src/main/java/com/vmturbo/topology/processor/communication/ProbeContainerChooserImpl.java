package com.vmturbo.topology.processor.communication;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Class for choosing a ITransport to use to communicate with a probe when multiple probes of the
 * same type are running. For probes that supports incremental discoveries This
 * {@link ProbeContainerChooser} chooses an ITransport if it is already assigned to a targetId.
 * If not, it chooses the next ITransport and assigns it to that targetId. For full discoveries,
 * validation and action executions, this chooser just select a transport using round robin,
 * without assigning it to the target.
 */
public class ProbeContainerChooserImpl implements ProbeContainerChooser {

    private final Logger logger = LogManager.getLogger();
    private final ProbeStore probeStore;

    /**
     * Index of the next ITransport from the collection to use.  We increment by one each time the
     * choose method is called and then mod it by the size of the collection.
     */
    private int index = -1;

    private final Map<Pair<String, String>,
            ITransport<MediationServerMessage, MediationClientMessage>> targetIdToTransport =
        new ConcurrentHashMap<>();

    /**
     * Initialize a new ProbeContainerChooserImp.
     * @param probeStore containing the probes
     */
    public ProbeContainerChooserImpl(@Nonnull final ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    /**
     * Chooses transport of a target.
     * @param  probeId the id of the probe
     * @param targetIdentifyingValues the serialized identifying field of a target
     * @param message the message that is being sent to the probe
     * @return return the assigned or created ITransport
     * @throws ProbeException if probe can't be found
     */
    @Override
    public ITransport<MediationServerMessage, MediationClientMessage> choose(
        final long probeId, @Nonnull final String targetIdentifyingValues,
        @Nonnull final MediationServerMessage message) throws ProbeException {
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>> transportCollection = getProbeTransports(probeId);

        if (!message.hasDiscoveryRequest()) {
            return getTransportUsingRoundRobin(transportCollection);
        }
        ProbeInfo probeInfo =
            probeStore.getProbe(probeId).orElseThrow(() -> new ProbeException(String.format(
                "Probe %s is not registered", String.valueOf(probeId))));
        if (!probeSupportsPersistentConnections(probeInfo)) {
            return getTransportUsingRoundRobin(transportCollection);
        }

        final Pair<String, String> lookupKey = new Pair<>(probeInfo.getProbeType(),
                targetIdentifyingValues);
        final ITransport<MediationServerMessage, MediationClientMessage> targetTransport =
            targetIdToTransport.get(lookupKey);
        // Check that the transport that we have cached is still among the available transports
        // of the probe
        if (targetTransport != null) {
            if (!transportCollection.contains(targetTransport)) {
                targetIdToTransport.remove(lookupKey);
            }
        }
        return targetIdToTransport.computeIfAbsent(lookupKey,
            k -> getTransportUsingRoundRobin(transportCollection));
        }

    /**
     * Assign the transport to a target.
     * @param  transport the transport to assign of the probe
     * @param targetIdentifyingValues the serialized identifying field of a target
     */
    @Override
    public void assignTargetToTransport(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull String probeType,
            @Nonnull String targetIdentifyingValues) {
        final Pair<String, String> lookupKey = new Pair<>(probeType,
                targetIdentifyingValues);
        if (targetIdToTransport.containsKey(lookupKey) && !transport.equals(
                targetIdToTransport.get(lookupKey))) {
            logger.warn("Transport {} for target {} being replaced with transport {}",
                    targetIdToTransport.get(lookupKey), targetIdentifyingValues,
                    transport);
        }
        logger.debug("Assigning target {} to transport {}", lookupKey, transport);
        targetIdToTransport.put(lookupKey,
            transport);
    }

    @Nonnull
    private ITransport<MediationServerMessage, MediationClientMessage> getTransportUsingRoundRobin(@Nonnull Collection<ITransport<MediationServerMessage,
        MediationClientMessage>>  transportCollection) {
        index = (index + 1) % transportCollection.size();
        logger.debug("Choosing container at index {}", index);
        return transportCollection.stream().skip(index).findAny().get();
    }

    private boolean probeSupportsPersistentConnections(@Nonnull ProbeInfo probeInfo) {
        return probeInfo.hasIncrementalRediscoveryIntervalSeconds();
    }

    private Collection<ITransport<MediationServerMessage, MediationClientMessage>> getProbeTransports(long probeId) throws ProbeException {
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>> transportCollection = probeStore.getTransport(probeId);
        logger.debug("Choosing transport from {} options.",
            transportCollection.size());
        return transportCollection;
    }
}
