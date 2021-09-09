package com.vmturbo.topology.processor.rpc;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

/**
 * Unit tests for {@link TargetHealthRetriever}.
 */
public class TargetHealthRetrieverTest {
    private IOperationManager operationManager = mock(IOperationManager.class);

    private TargetStore targetStore = mock(TargetStore.class);

    private TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);

    private Clock clock = new MutableFixedClock(1_000_000);

    private TargetHealthRetriever healthRetriever = new TargetHealthRetriever(operationManager, targetStatusTracker, targetStore, clock);

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final long probeId = 101;

    /**
     * Common setup before each test.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        when(identityProvider.generateOperationId()).thenReturn(IdentityGenerator.next());
        when(operationManager.getLastValidationForTarget(anyLong())).thenReturn(Optional.empty());
        when(operationManager.getLastDiscoveryForTarget(anyLong(), any())).thenReturn(Optional.empty());
    }

    /**
     * Test single target health - no successful validation, but successful discovery.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthNoGoodValidationButDiscoveryOk() throws Exception {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.success();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        //Test with no validation data.
        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());

        //Now test with a failed validation but a newer discovery.
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        validation.fail();
        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .setErrorType(errorType)
                .setDescription("connection timed out")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        validation.addError(errorDTO);
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));

        //Set time of success for discovery:
        discovery.success();

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * Test single target health - no data.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthNoData() throws Exception {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION,
                healthInfo.getSubcategory());
        Assert.assertEquals("Validation pending.", healthInfo.getErrorText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * Test single target health - super healthy.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthOk() throws Exception   {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());
    }

    /**
     * Test single target health - validation failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthValidationFailed() throws Exception   {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .setErrorType(errorType)
                .setDescription("connection timed out")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        validation.addError(errorDTO);
        validation.fail();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));

        //Test with no discovery data.
        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(1, healthInfo.getConsecutiveFailureCount());

        //Test with present failed discovery.
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.addError(errorDTO);
        discovery.fail();
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        for (int i = 0; i < 2; i++)    {
            discoveryFailure.incrementFailsCount();
        }
        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        when(targetStatusTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(1, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * Test single target health - discovery failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthDiscoveryFailed() throws Exception   {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        validation.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.fail();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.DATA_IS_MISSING;
        int numberOfFailures = 2;
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .setErrorType(errorType)
                .setDescription("data is missing")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        discovery.addError(errorDTO);
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        for (int i = 0; i < numberOfFailures; i++)    {
            discoveryFailure.incrementFailsCount();
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        when(targetStatusTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(numberOfFailures, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * Test that last discovery time is present given a successful discovery.
     */
    @Test
    public void testLastDiscoveryTimePresent() {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        discovery.success();
        validation.success();

        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(System.currentTimeMillis());
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertTrue(healthInfo.hasLastSuccessfulDiscovery());
    }

    /**
     * Test that last discovery time is persistent.
     */
    @Test
    public void testLastDiscoveryTimePersists() {
        final Target target = mockTarget(probeId, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        discovery.success();
        validation.success();

        long time = System.currentTimeMillis();

        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(time);
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertTrue(healthInfo.hasLastSuccessfulDiscovery());
        Assert.assertEquals(time, healthInfo.getLastSuccessfulDiscovery());

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.DATA_IS_MISSING;
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .setErrorType(errorType)
                .setDescription("data is missing")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        validation.addError(errorDTO);
        discovery.fail();
        validation.fail();

        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(time, healthInfo.getLastSuccessfulDiscovery());
    }

    private Target mockTarget(long probeId, long targetId, String displayName) {
        Target target = mock(Target.class);
        when(target.getProbeId()).thenReturn(probeId);
        when(target.getId()).thenReturn(targetId);
        when(target.getDisplayName()).thenReturn(displayName);
        when(targetStore.getTargets(Collections.singleton(targetId))).thenReturn(Collections.singletonList(target));
        return target;
    }

    private TargetHealth getTargetHealth(long targetId) {
        return healthRetriever.getTargetHealth(Collections.singleton(targetId), false).get(targetId);
    }
}
