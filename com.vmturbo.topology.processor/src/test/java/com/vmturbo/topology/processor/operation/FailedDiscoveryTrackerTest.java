package com.vmturbo.topology.processor.operation;

import java.time.LocalDateTime;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.FailedDiscoveryTracker.DiscoveryFailure;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Unit tests for {@link FailedDiscoveryTracker}.
 */
public class FailedDiscoveryTrackerTest {
    private static final long PROBE_ID = 11111111L;
    private static final long TARGET_ID = 22222222L;

    private FailedDiscoveryTracker failedDiscoveryTracker;
    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    /**
     * Common code to run before every test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        Mockito.when(probeStore.isProbeConnected(PROBE_ID)).thenReturn(true);
        failedDiscoveryTracker = new FailedDiscoveryTracker(probeStore);

        IdentityGenerator.initPrefix(0);
        Mockito.when(identityProvider.generateOperationId()).thenAnswer((invocation) -> IdentityGenerator.next());
    }

    /**
     * Test adding and retrieving failed discovery information.
     * Sends notification about failed discovery twice.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testStoringFailedDiscovery() throws Exception {
        final Discovery discovery = createFailedDiscovery();
        failedDiscoveryTracker.notifyOperationState(discovery);
        final Map<Long, DiscoveryFailure> failedDiscoveries = failedDiscoveryTracker.getFailedDiscoveries();
        Assert.assertEquals(1, failedDiscoveries.size());
        final Map.Entry<Long, DiscoveryFailure> entry = failedDiscoveries.entrySet().iterator().next();
        final Long targetId = entry.getKey();
        final DiscoveryFailure failedDiscovery = entry.getValue();
        Assert.assertEquals(TARGET_ID, targetId.longValue());
        final LocalDateTime failTime = failedDiscovery.getFailTime();
        Assert.assertNotNull(failTime);
        Assert.assertEquals(1, failedDiscovery.getFailsCount());

        failedDiscoveryTracker.notifyOperationState(discovery);
        final DiscoveryFailure failure = failedDiscoveries.get(targetId);
        Assert.assertNotNull(failure);
        Assert.assertEquals(failTime, failure.getFailTime());
        Assert.assertEquals(2, failedDiscovery.getFailsCount());
    }

    private Discovery createFailedDiscovery() {
        final Discovery discovery = new Discovery(PROBE_ID, TARGET_ID, DiscoveryType.FULL, identityProvider);
        discovery.addError(ErrorDTO.newBuilder().setErrorType(ErrorType.UNAUTHENTICATED)
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("Wrong credentials").build());
        discovery.fail();
        return discovery;
    }

    /**
     * Test removing information on target remove.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRemove() throws Exception {
        final Target target = Mockito.mock(Target.class);
        Mockito.when(target.getId()).thenReturn(TARGET_ID);
        failedDiscoveryTracker.notifyOperationState(createFailedDiscovery());
        Assert.assertEquals(1, failedDiscoveryTracker.getFailedDiscoveries().size());
        failedDiscoveryTracker.onTargetRemoved(target);
        Assert.assertEquals(0, failedDiscoveryTracker.getFailedDiscoveries().size());
    }

}
