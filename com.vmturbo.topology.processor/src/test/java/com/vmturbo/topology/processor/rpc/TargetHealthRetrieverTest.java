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
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

import common.HealthCheck.HealthState;

/**
 * Unit tests for {@link TargetHealthRetriever}.
 */
public class TargetHealthRetrieverTest {
    private IOperationManager operationManager = mock(IOperationManager.class);

    private TargetStore targetStore = mock(TargetStore.class);

    private ProbeStore probeStore = mock(ProbeStore.class);

    private TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);

    private Clock clock = new MutableFixedClock(1_000_000);

    private TargetHealthRetriever healthRetriever = new TargetHealthRetriever(
                    operationManager, targetStatusTracker, targetStore, probeStore, clock);

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final long PROBE_ID = 101;

    private ProbeInfo probeInfo;

    private static final int DEFAULT_REDISCOVERY_PERIOD = 600;
    private static final long DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER = 10;

    /**
     * Common setup before each test.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        when(identityProvider.generateOperationId()).thenReturn(IdentityGenerator.next());
        when(operationManager.getLastValidationForTarget(anyLong())).thenReturn(Optional.empty());
        when(operationManager.getLastDiscoveryForTarget(anyLong(), any())).thenReturn(Optional.empty());

        when(probeStore.isProbeConnected(PROBE_ID)).thenReturn(true);
        probeInfo = ProbeInfo.newBuilder()
                        .setProbeCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                        .setProbeType(SDKProbeType.SERVICENOW.getProbeType())
                        .setFullRediscoveryIntervalSeconds(DEFAULT_REDISCOVERY_PERIOD)
                        .build();
    }

    /**
     * Test single target health - no successful validation, but successful discovery.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthNoGoodValidationButDiscoveryOk() throws Exception {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        discovery.success();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        //Test with no validation data.
        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.NORMAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());

        //Now test with a failed validation but a newer discovery.
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
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
        long successfulDiscoveryTime = TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock);
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(
                new Pair<>(successfulDiscoveryTime - 500 * DEFAULT_REDISCOVERY_PERIOD, successfulDiscoveryTime));

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.NORMAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
    }

    /**
     * Test single target health - no data.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthNoData() throws Exception {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.MINOR, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals("Validation pending.", healthInfo.getMessageText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());

        //Now set the probe to be unconnected.
        when(probeStore.isProbeConnected(PROBE_ID)).thenReturn(false);
        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals(ErrorDTO.ErrorType.OTHER, healthInfo.getErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());
    }

    /**
     * Test single target health - super healthy.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthOk() throws Exception   {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.NORMAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
    }

    /**
     * Test single target health - validation failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthValidationFailed() throws Exception   {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
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
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock),
                        healthInfo.getTimeOfFirstFailure());

        //Test with present failed discovery.
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
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
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock),
                        healthInfo.getTimeOfFirstFailure());
    }

    /**
     * Test single target health - discovery failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthDiscoveryFailed() throws Exception   {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        validation.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        discovery.fail();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.DATA_IS_MISSING;
        int numberOfFailures = 4;
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
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock),
                        healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(numberOfFailures, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * If Validation failed and then there was a successful Discovery then the latter overrides the former.
     * If after that another Discovery fails then a Discovery failure must be reported. Test this case.
     * @throws Exception to satisfy requirements for Thread.sleep()
     */
    @Test
    public void testValidationFailureOverridenByDiscoveryFailure() throws Exception {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        validation.fail();
        ErrorDTO.ErrorType validationErrorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        ErrorDTO validationErrorDTO = ErrorDTO.newBuilder()
                .setErrorType(validationErrorType)
                .setDescription("connection timed out")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        validation.addError(validationErrorDTO);
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));

        //Now setup a record about a successful Discovery that took place after that failed Validation.
        long successfulstartTime = System.currentTimeMillis();
        Thread.sleep(1000);
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(
                new Pair<>(successfulstartTime, System.currentTimeMillis()));

        //Add a failed discovery.
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        discovery.fail();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        ErrorDTO.ErrorType discoveryErrorType = ErrorDTO.ErrorType.DATA_IS_MISSING;
        int numberOfFailures = 4;
        ErrorDTO discoveryErrorDTO = ErrorDTO.newBuilder()
                .setErrorType(discoveryErrorType)
                .setDescription("data is missing")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        discovery.addError(discoveryErrorDTO);
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        for (int i = 0; i < numberOfFailures; i++)    {
            discoveryFailure.incrementFailsCount();
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        when(targetStatusTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);

        //Check the result.
        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(discoveryErrorType, healthInfo.getErrorType());
        Assert.assertEquals(TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock),
                        healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(numberOfFailures, healthInfo.getConsecutiveFailureCount());
    }

    /**
     * Test that last discovery time is present given a successful discovery.
     */
    @Test
    public void testLastDiscoveryTimePresent() {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        discovery.success();
        validation.success();

        long currentTimestamp = System.currentTimeMillis();
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId))
                .thenReturn(new Pair<>(currentTimestamp - 500 * DEFAULT_REDISCOVERY_PERIOD, currentTimestamp));
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertTrue(healthInfo.hasLastSuccessfulDiscoveryCompletionTime());
    }

    /**
     * Test that last discovery time is persistent.
     */
    @Test
    public void testLastDiscoveryTimePersists() {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        discovery.success();
        validation.success();

        long time = System.currentTimeMillis();

        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId))
                .thenReturn(new Pair<>(time - 500 * DEFAULT_REDISCOVERY_PERIOD, time));
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertTrue(healthInfo.hasLastSuccessfulDiscoveryCompletionTime());
        Assert.assertEquals(time, healthInfo.getLastSuccessfulDiscoveryCompletionTime());

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
        Assert.assertEquals(time, healthInfo.getLastSuccessfulDiscoveryCompletionTime());
    }

    /**
     * Test the detection of delayed data.
     */
    @Test
    public void testDelayedData() {
        final Target target = mockTarget(PROBE_ID, 1, "43");
        final long targetId = target.getId();

        //Set the last successful discovery to have been long ago.
        long successfulDiscoveryTime = System.currentTimeMillis() - DEFAULT_REDISCOVERY_PERIOD * 1000
                        * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER * 2;
        long successfulDiscoveryStart = successfulDiscoveryTime - 500 * DEFAULT_REDISCOVERY_PERIOD;
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(
                new Pair<>(successfulDiscoveryStart, successfulDiscoveryTime));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DELAYED_DATA, healthInfo.getSubcategory());
        Assert.assertEquals(ErrorDTO.ErrorType.DELAYED_DATA, healthInfo.getErrorType());
        Assert.assertEquals(successfulDiscoveryTime, healthInfo.getLastSuccessfulDiscoveryCompletionTime());
        Assert.assertEquals(successfulDiscoveryStart, healthInfo.getLastSuccessfulDiscoveryStartTime());
        Assert.assertEquals(System.currentTimeMillis(), healthInfo.getTimeOfCheck(), 1000);
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());

        //Now add an actual successful discovery and change the record about the last discovery to be
        //not too old but too slow.
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        discovery.success();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        successfulDiscoveryTime = TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock);
        successfulDiscoveryStart = successfulDiscoveryTime - DEFAULT_REDISCOVERY_PERIOD * 1000
                        * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER * 2;
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(
                new Pair<>(successfulDiscoveryStart, successfulDiscoveryTime));

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DELAYED_DATA, healthInfo.getSubcategory());
        Assert.assertEquals(ErrorDTO.ErrorType.DELAYED_DATA, healthInfo.getErrorType());
        Assert.assertEquals(successfulDiscoveryTime, healthInfo.getLastSuccessfulDiscoveryCompletionTime());
        Assert.assertEquals(successfulDiscoveryStart, healthInfo.getLastSuccessfulDiscoveryStartTime());
    }

    private Target mockTarget(long probeId, long targetId, String displayName) {
        Target target = mock(Target.class);
        when(target.getProbeId()).thenReturn(probeId);
        when(target.getId()).thenReturn(targetId);
        when(target.getDisplayName()).thenReturn(displayName);
        when(target.getProbeInfo()).thenReturn(probeInfo);
        when(targetStore.getTargets(Collections.singleton(targetId))).thenReturn(Collections.singletonList(target));
        return target;
    }

    private TargetHealth getTargetHealth(long targetId) {
        return healthRetriever.getTargetHealth(Collections.singleton(targetId), false).get(targetId);
    }
}
