package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertTrue;

import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;

/**
 * Class that tests a PersistentListenerProbeContainerChooser.
 */
public class PersistentListenerProbeContainerChooserTest {

    /**
     * Tests getting the transport for different targets, but one probe instance.
     */
    @Test
    public void testWithOneTransport() {
        final long target1 = 1L;
        final long target2 = 2L;
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        transportList.sort((o1, o2) -> Integer.compare(o1.hashCode(), o2.hashCode()));
        PersistentListenerProbeContainerChooser chooser = new PersistentListenerProbeContainerChooser();
        // test that if the collection is a singleton, we always choose the same transport
        assertTrue(chooser.getOrCreateTransportForTarget(target1, transportList) == transport1);
        assertTrue(chooser.getOrCreateTransportForTarget(target2, transportList) == transport1);
    }

    /**
     * Tests getting the transport for different targets, and different probe instances.
     */
    @Test
    public void testMultipleTransports() {
        final long target1 = 1L;
        final long target2 = 2L;
        final long target3 = 3L;

        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1, transport2);
        PersistentListenerProbeContainerChooser chooser = new PersistentListenerProbeContainerChooser();
        // Create new transports, for each target, until transports are available, then use round
        // robin
        assertTrue(chooser.getOrCreateTransportForTarget(target1, transportList) == transportList.get(0));
        assertTrue(chooser.getOrCreateTransportForTarget(target2, transportList) == transportList.get(1));
        assertTrue(chooser.getOrCreateTransportForTarget(target3, transportList) == transportList.get(0));

        // Make sure we can retrieve already created transport for a target
        assertTrue(chooser.getOrCreateTransportForTarget(target3, transportList) == transportList.get(0));
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(ITransport.class);
    }

}
