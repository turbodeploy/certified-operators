package com.vmturbo.topology.processor.rpc;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ConnectionTimeOutErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DataIsMissingErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DelayedDataErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.OtherErrorType;
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

    private SettingServiceMole settingsService = Mockito.spy(new SettingServiceMole());
    private GrpcTestServer grpcServer = GrpcTestServer.newServer(settingsService);

    private TargetHealthRetriever healthRetriever;

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final long PROBE_ID = 101;

    private ProbeInfo probeInfo;

    private static final int DEFAULT_REDISCOVERY_PERIOD = 600;
    private static final long DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER = 10;
    private static final ErrorTypeInfo connectionTimeoutError = ErrorTypeInfo.newBuilder()
            .setConnectionTimeOutErrorType(ConnectionTimeOutErrorType.getDefaultInstance()).build();
    private static final ErrorTypeInfo otherProbeError = ErrorTypeInfo.newBuilder()
            .setOtherErrorType(OtherErrorType.getDefaultInstance()).build();
    private static final ErrorTypeInfo dataIsMissingError = ErrorTypeInfo.newBuilder()
            .setDataIsMissingErrorType(DataIsMissingErrorType.getDefaultInstance()).build();
    private static final ErrorTypeInfo delayedDataError = ErrorTypeInfo.newBuilder()
            .setDelayedDataErrorType(DelayedDataErrorType.getDefaultInstance()).build();

    /**
     * Common setup before each test.
     * @throws IOException can theoretically be thrown by grpcServer.start()
     */
    @Before
    public void setup() throws IOException {
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

        List<Setting> settingsForRetriever = new ArrayList<>(2);
        settingsForRetriever.add(Setting.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.FailedDiscoveryCountThreshold.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(5))
                        .build());
        settingsForRetriever.add(Setting.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.DelayedDataThresholdMultiplier.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10))
                        .build());
        Mockito.when(settingsService.getMultipleGlobalSettings(Mockito.any())).thenReturn(settingsForRetriever);

        grpcServer.start();
        SettingServiceBlockingStub settingServiceClient = SettingServiceGrpc
                        .newBlockingStub(grpcServer.getChannel());

        healthRetriever = new TargetHealthRetriever(operationManager, targetStatusTracker,
                        targetStore, probeStore, clock, settingServiceClient);
    }

    /**
     * Release the test resources.
     */
    @After
    public void shutdown() {
        grpcServer.close();
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
        Assert.assertTrue(healthInfo.getErrorTypeInfoList().isEmpty());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());

        //Now test with a failed validation but a newer discovery.
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        validation.fail();
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(connectionTimeoutError)
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
        Assert.assertTrue(healthInfo.getErrorTypeInfoList().isEmpty());
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
        Assert.assertEquals(1, healthInfo.getErrorTypeInfoList().size());
        Assert.assertEquals(otherProbeError, healthInfo.getErrorTypeInfoList().get(0));
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
        Assert.assertTrue(healthInfo.getErrorTypeInfoList().isEmpty());
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

        ErrorTypeInfo errorTypeInfo = ErrorTypeInfo.newBuilder().setConnectionTimeOutErrorType(
                ConnectionTimeOutErrorType.getDefaultInstance()).build();
        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(errorTypeInfo)
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
        Assert.assertEquals(healthInfo.getErrorTypeInfoList().size(), 1);
        Assert.assertEquals(errorTypeInfo, healthInfo.getErrorTypeInfo(0));
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
        Assert.assertEquals(healthInfo.getErrorTypeInfoList().size(), 1);
        Assert.assertEquals(errorTypeInfo, healthInfo.getErrorTypeInfo(0));
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

        ErrorTypeInfo errorTypeInfo = ErrorTypeInfo.newBuilder().setDataIsMissingErrorType(
                DataIsMissingErrorType.getDefaultInstance()).build();
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(errorTypeInfo)
                .setDescription("data is missing")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        discovery.addError(errorDTO);
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        //Try first with the number of discovery failures below the threshold set in setup().
        int numberOfFailures = 4;
        for (int i = 0; i < numberOfFailures; i++)    {
            discoveryFailure.incrementFailsCount();
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        when(targetStatusTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.NORMAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(numberOfFailures, healthInfo.getConsecutiveFailureCount());

        //Now increase the number of failures by 1.
        numberOfFailures++;
        discoveryFailure.incrementFailsCount();

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(healthInfo.getErrorTypeInfoList().size(), 1);
        Assert.assertEquals(errorTypeInfo, healthInfo.getErrorTypeInfo(0));
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
        ErrorDTO validationErrorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(connectionTimeoutError)
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

        ErrorTypeInfo errorTypeInfo = ErrorTypeInfo.newBuilder().setDataIsMissingErrorType(
                DataIsMissingErrorType.getDefaultInstance()).build();
        ErrorDTO discoveryErrorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(errorTypeInfo)
                .setDescription("data is missing")
                .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                .build();
        discovery.addError(discoveryErrorDTO);
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        int numberOfFailures = 5;
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
        Assert.assertEquals(healthInfo.getErrorTypeInfoList().size(), 1);
        Assert.assertEquals(errorTypeInfo, healthInfo.getErrorTypeInfo(0));
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

        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(dataIsMissingError)
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
        Assert.assertEquals(1, healthInfo.getErrorTypeInfoList().size());
        Assert.assertEquals(delayedDataError, healthInfo.getErrorTypeInfoList().get(0));
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
        Assert.assertEquals(1, healthInfo.getErrorTypeInfoList().size());
        Assert.assertEquals(delayedDataError, healthInfo.getErrorTypeInfoList().get(0));
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
