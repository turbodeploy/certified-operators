package com.vmturbo.mediation.webhook.component;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;

import common.HealthCheck.HealthState;

/**
 * Test class for {@link TargetRegistration}.
 */
public class TargetRegistrationTest {

    private static final ProbeInfo WEBHOOK_PROBE = new ProbeDescription(1L,
            SDKProbeType.WEBHOOK.getProbeType(), ProbeCategory.ORCHESTRATOR.getCategory(),
            ProbeCategory.ORCHESTRATOR.getCategory(), null, CreationMode.INTERNAL,
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            new HashSet<>(Arrays.asList(ProbeFeature.ACTION_EXECUTION, ProbeFeature.DISCOVERY)));

    private static final TargetInfo WEBHOOK_TARGET = new TargetInfo(2L, "Webhook",
            Collections.emptyList(), new TargetSpec(1L, Collections.emptyList(), Optional.empty(),
            "System"),
            true, "VALID", LocalDateTime.now(), null, null, HealthState.NORMAL);

    private TopologyProcessor topologyProcessor;

    /**
     * Initialize test environment.
     */
    @Before
    public void init() {
        topologyProcessor = Mockito.mock(TopologyProcessor.class);
    }

    /**
     * Test that only one webhook target can exist in the environment.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSingleWebhookTarget() throws Exception {
        // ARRANGE#1
        Mockito.when(topologyProcessor.getAllProbes()).thenReturn(
                Collections.singleton(WEBHOOK_PROBE));
        Mockito.when(topologyProcessor.getAllTargets()).thenReturn(Collections.emptySet());
        final TargetRegistration targetRegistration = new TargetRegistration(topologyProcessor);

        // ACT#1
        targetRegistration.checkTargetRegistration();

        // ASSERT#1
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(WEBHOOK_PROBE.getId()),
                Mockito.any(TargetData.class));

        Mockito.reset(topologyProcessor);

        // ARRANGE#2
        Mockito.when(topologyProcessor.getAllProbes()).thenReturn(
                Collections.singleton(WEBHOOK_PROBE));
        Mockito.when(topologyProcessor.getAllTargets()).thenReturn(
                Collections.singleton(WEBHOOK_TARGET));

        // ACT#2
        targetRegistration.checkTargetRegistration();

        // ASSERT#2
        // Checks that we don't add a webhook target if it is already exist
        Mockito.verify(topologyProcessor, Mockito.never()).addTarget(
                Mockito.eq(WEBHOOK_PROBE.getId()), Mockito.any(TargetData.class));
    }
}