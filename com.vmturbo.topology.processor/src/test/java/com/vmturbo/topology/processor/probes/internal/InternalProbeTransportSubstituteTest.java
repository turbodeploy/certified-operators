package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import static com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest.newBuilder;
import static com.vmturbo.topology.processor.probes.internal.UserDefinedEntitiesProbe.UDE_PROBE_TYPE;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;

/**
 * Test class for {@link InternalProbeTransportSubstitute}.
 */
public class InternalProbeTransportSubstituteTest {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * A probe`s method 'discoverTarget' should be called one time.
     */
    @Test
    public void testSendDiscoveryResponse() {
        EventHandler handler = Mockito.mock(EventHandler.class);
        IDiscoveryProbe probe = Mockito.mock(IDiscoveryProbe.class);
        sendMessageOnTransport(probe, handler);
        try {
            Mockito.verify(probe, Mockito.times(1))
                    .discoverTarget(Mockito.any(UserDefinedEntitiesProbeAccount.class));
        } catch (Exception e) {
            LOGGER.warn(e);
        }
    }

    /**
     * A probe`s method 'discoverTarget' should not be called if the transport has no response handler.
     */
    @Test
    public void testBlockDiscoveryResponse() {
        IDiscoveryProbe probe = Mockito.mock(IDiscoveryProbe.class);
        sendMessageOnTransport(probe, null);
        try {
            Mockito.verify(probe, Mockito.times(0))
                    .discoverTarget(Mockito.any(UserDefinedEntitiesProbeAccount.class));
        } catch (Exception e) {
            LOGGER.warn(e);
        }

    }

    private void sendMessageOnTransport(@Nonnull IDiscoveryProbe probe, @Nullable EventHandler handler) {
        Discovery.DiscoveryResponse response = Discovery.DiscoveryResponse.newBuilder().build();
        Mockito.when(probe.getAccountDefinitionClass()).thenReturn(UserDefinedEntitiesProbeAccount.class);
        try {
            Mockito.when(probe.discoverTarget(Mockito.any(UserDefinedEntitiesProbeAccount.class))).thenReturn(response);
        } catch (Exception e) {
            LOGGER.warn(e);
        }
        InternalProbeTransportSubstitute transport = new InternalProbeTransportSubstitute(probe);
        if (handler != null) {
            transport.addEventHandler(handler);
        }
        MediationServerMessage message = createDiscoveryRequest();
        transport.send(message);

    }

    private MediationServerMessage createDiscoveryRequest() {
        return MediationServerMessage.newBuilder()
                .setDiscoveryRequest(newBuilder()
                        .setProbeType(UDE_PROBE_TYPE)
                        .setDiscoveryType(DiscoveryType.FULL)
                        .build())
                .build();
    }

}
