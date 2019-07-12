package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;

public class RoundRobinProbeContainerChooserTest {

    @Test
    public void testRoundRobinOrder() {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        final RoundRobinProbeContainerChooser chooser = new RoundRobinProbeContainerChooser();
        // test that if the collection is a singleton, we always choose the same transport
        assertTrue(chooser.choose(transportList) == transport1);
        assertTrue(chooser.choose(transportList) == transport1);
        transportList.add(transport2);
        // test that if we choose more transports than exist in the list, we cycle back around
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice1 =
            chooser.choose(transportList);
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice2 =
            chooser.choose(transportList);
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice3 =
            chooser.choose(transportList);
        assertTrue(transportChoice1 != transportChoice2);
        assertTrue(transportChoice1 == transportChoice3);
        assertTrue(transportChoice1 == transport1 || transportChoice1 == transport2);
        assertTrue(transportChoice2 == transport1 || transportChoice2 == transport2);
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>) Mockito.mock(
            ITransport.class);
    }

}
