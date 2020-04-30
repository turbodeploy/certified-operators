package com.vmturbo.mediation.client.it;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.common.tests.probes.EternalWorkingProbe;
import com.vmturbo.mediation.common.tests.util.NotValidatableProbe;
import com.vmturbo.mediation.common.tests.util.ProbeConfig;
import com.vmturbo.mediation.common.tests.util.IntegrationTestProbeConfiguration;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.SdkTarget;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test class to ensure correct behaviour on transport goes down.
 */
public class TransportCloseTest extends AbstractIntegrationTest {

    /**
     * Test transport closed during discovery. RemoteMediation is expected to report transport
     * closed event.
     *
     * @throws Exception on exception occur
     */
    @Ignore
    @Test
    public void testCloseWhenDiscovery() throws Exception {
        final IntegrationTestProbeConfiguration probeConfig =
                        new NotValidatableProbe(EternalWorkingProbe.class, ProbeConfig.Empty);
        final SdkProbe probe = new SdkProbe(probeConfig, "empty-probe");
        final SdkTarget target = new SdkTarget(probe, testName.getMethodName());
        final SdkContainer container = startSdkComponent(probe);
        container.awaitRegistered();
        final Target targetMock = Mockito.mock(Target.class);
        Mockito.when(targetMock.getSerializedIdentifyingFields()).thenReturn(target.getTargetId());

        final RmMessageReceipient<Discovery> responseHandler =
                new RmMessageReceipient(Discovery.class);
        final DiscoveryRequest request =
                        DiscoveryRequest.newBuilder()
                                .addAllAccountValue(target.getAccountValues())
                                .setDiscoveryType(DiscoveryType.FULL)
                                .setProbeType(probe.getType()).build();
        getRemoteMediation().sendDiscoveryRequest(targetMock,
            request,
            responseHandler);
        container.close();
        responseHandler.await(TIMEOUT);
        Assert.assertTrue(responseHandler.isTransportClosed());
    }

    /**
     * Test transport closed during validation. RemoteMediation is expected to report transport
     * closed event.
     *
     * @throws Exception on exception occur
     */
    @Ignore
    @Test
    public void testCloseWhenValidation() throws Exception {
        final IntegrationTestProbeConfiguration probeConfig =
                        new NotValidatableProbe(EternalWorkingProbe.class, ProbeConfig.Empty);
        final SdkProbe probe = new SdkProbe(probeConfig, "empty-probe");
        final SdkTarget target = new SdkTarget(probe, testName.getMethodName());
        final SdkContainer container = startSdkComponent(probe);
        container.awaitRegistered();
        final Target targetMock = Mockito.mock(Target.class);
        Mockito.when(targetMock.getSerializedIdentifyingFields()).thenReturn(target.getTargetId());

        final RmMessageReceipient<Validation> responseHandler =
                new RmMessageReceipient(Validation.class);
        final ValidationRequest request =
                        ValidationRequest.newBuilder().addAllAccountValue(target.getAccountValues())
                                        .setProbeType(probe.getType()).build();
        getRemoteMediation().sendValidationRequest(targetMock, request, responseHandler);
        container.close();
        responseHandler.await(TIMEOUT);
        Assert.assertTrue(responseHandler.isTransportClosed());
    }
}
