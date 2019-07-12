package com.vmturbo.topology.processor.communication;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;

/**
 * Class for choosing a ITransport to use to communicate with a probe when multiple probes of the
 * same type are running.  This {@link ProbeContainerChooser} chooses each ITransport from the
 * collection in turn and then starts from the beginning when it reaches the last one.
 */
public class RoundRobinProbeContainerChooser implements ProbeContainerChooser {

    private final Logger logger = LogManager.getLogger();

    /**
     * Index of the next ITransport from the collection to use.  We increment by one each time the
     * choose method is called and then mod it by the size of the collection.
     */
    private int index = -1;

    @Override
    public ITransport<MediationServerMessage, MediationClientMessage> choose(
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>>
            transportCollection) {
        List<ITransport<MediationServerMessage, MediationClientMessage>> transports =
            transportCollection.stream()
                .sorted((o1, o2) -> Integer.compare(o1.hashCode(), o2.hashCode()))
                .collect(Collectors.toList());
        index = (index + 1) % transportCollection.size();
        logger.debug("Choosing container at index {}", index);
        return transports.get(index);
    }
}
