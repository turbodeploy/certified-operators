package com.vmturbo.mediation.client.it;

import java.time.Clock;

import javax.annotation.Nonnull;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.common.integration.tests.AbstractIntegrationTest;
import com.vmturbo.mediation.common.tests.probes.EternalWorkingProbe;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.SdkTarget;
import com.vmturbo.mediation.common.tests.util.TestConstants;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test class to ensure correct behaviour on transport goes down.
 */
public class TransportCloseTest extends AbstractIntegrationTest {

    private SdkTarget target;
    private SdkProbe probe;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occurred.
     */
    @Before
    public void initLocal() throws Exception {
        createDefaultContainer();
        probe = createProbe(EternalWorkingProbe.class);
        target = probe.createSdkTarget();
        startMediationContainer(true);
    }

    /**
     * Test transport closed during discovery. RemoteMediation is expected to report transport
     * closed event.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testCloseWhenDiscovery() throws Exception {
        final TestOperationCallback<DiscoveryResponse> callback = new TestOperationCallback<>();
        final DiscoveryMessageHandler responseHandler = new DiscoveryMessageHandler(
                Mockito.mock(Discovery.class), Clock.systemUTC(), Long.MAX_VALUE, callback);
        getSdkEngine().getBean(RemoteMediationServer.class).sendDiscoveryRequest(
                createTarget(target), DiscoveryRequest.newBuilder()
                        .setProbeType(probe.getType())
                        .addAllAccountValue(target.getAccountValues())
                        .setDiscoveryType(DiscoveryType.FULL)
                        .build(), responseHandler);
        getDefaultContainer().stop();
        callback.await(TestConstants.TIMEOUT);
        Assert.assertThat(callback.getError(),
                CoreMatchers.containsString("Communication transport to remote probe closed."));
    }

    /**
     * Test transport closed during validation. RemoteMediation is expected to report transport
     * closed event.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testCloseWhenValidation() throws Exception {
        final TestOperationCallback<ValidationResponse> callback = new TestOperationCallback<>();
        final ValidationMessageHandler responseHandler = new ValidationMessageHandler(
                Mockito.mock(Validation.class), Clock.systemUTC(), Long.MAX_VALUE, callback);
        getSdkEngine().getBean(RemoteMediationServer.class).sendValidationRequest(
                createTarget(target), ValidationRequest.newBuilder()
                        .setProbeType(probe.getType())
                        .addAllAccountValue(target.getAccountValues())
                        .build(), responseHandler);
        getDefaultContainer().stop();
        callback.await(TestConstants.TIMEOUT);
        Assert.assertThat(callback.getError(),
                CoreMatchers.containsString("Communication transport to remote probe closed."));
    }

    @Nonnull
    private Target createTarget(@Nonnull SdkTarget target) {
        final Target targetMock = Mockito.mock(Target.class);
        Mockito.when(targetMock.getSerializedIdentifyingFields()).thenReturn(target.getTargetId());
        final long probeId = getSdkEngine().getBean(ProbeStore.class).getProbeIdForType(
                target.getProbe().getType()).get();
        Mockito.when(targetMock.getProbeId()).thenReturn(probeId);
        return targetMock;
    }

    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
