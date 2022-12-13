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
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ErrorTypeInfoCase;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.OtherErrorType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeVersionFactory;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

import common.HealthCheck.HealthState;

/**
 * Unit tests for {@link TargetHealthRetriever}.
 */
public class TargetHealthRetrieverTest {
    private final IOperationManager operationManager = mock(IOperationManager.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final SettingServiceMole settingsService = Mockito.spy(new SettingServiceMole());
    private final GrpcTestServer grpcServer = GrpcTestServer.newServer(settingsService);

    private TargetHealthRetriever healthRetriever;

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

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
    private static final ProbeRegistrationDescription olderProbe = mockProbe("8.3.5");
    private final ProbeRegistrationDescription newerProbe = mockProbe("8.3.7");
    private final ProbeRegistrationDescription perfectProbe = mockProbe("8.3.6");
    private static final Scheduler scheduler = mock(Scheduler.class);

    /**
     * Public method to return a TargetHealthRetriever constructor outside the package
     * (used in TopologyPipelineFactoryFromDiagsTest).
     * @param operationManager to use in the constructor
     * @param targetStatusTracker to use in the constructor
     * @param targetStore to use in the constructor
     * @param probeStore to use in the constructor
     * @param clock to use in the constructor
     * @param settingServiceClient to use in the constructor
     * @return a new instance of TargetHealthRetriever
     */
    public static TargetHealthRetriever createInstance(IOperationManager operationManager,
            TargetStatusTracker targetStatusTracker, TargetStore targetStore, ProbeStore probeStore,
            Clock clock, SettingServiceBlockingStub settingServiceClient) {
        return new TargetHealthRetriever(operationManager, targetStatusTracker, targetStore,
                probeStore, clock, settingServiceClient);
    }

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

        when(probeStore.isAnyTransportConnectedForTarget(any())).thenReturn(true);
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
        healthRetriever.setScheduler(scheduler);
        setRediscoveryInterval();
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
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(ImmutableList.of(perfectProbe));

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
        final Target target = mockTarget();
        final long targetId = target.getId();

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.MINOR, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
        Assert.assertEquals("Validation pending.", healthInfo.getMessageText());
        Assert.assertFalse(healthInfo.hasErrorType());
        Assert.assertFalse(healthInfo.hasTimeOfFirstFailure());
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());

        //Now set the probe to be unconnected.
        when(probeStore.isAnyTransportConnectedForTarget(target)).thenReturn(false);
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
    public void testSingleTargetWithProbeVersionMismatch() throws Exception   {
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(
                ImmutableList.of(olderProbe, newerProbe, perfectProbe));

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.MAJOR, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.VALIDATION, healthInfo.getSubcategory());
    }

    /**
     * Test single target health - super healthy.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthOk() throws Exception   {
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(ImmutableList.of(perfectProbe));

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
     * Test target health loaded from diags.
     * @throws Exception any exception.
     */
    @Test
    public void testTargetHealthLoadedFromDiags() throws Exception   {
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(ImmutableList.of(perfectProbe));

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        TargetHealth targetHealth1 = TargetHealth.newBuilder().setHealthState(HealthState.CRITICAL)
                .setSubcategory(TargetHealthSubCategory.DELAYED_DATA).build();
        TargetHealth targetHealth2 = TargetHealth.newBuilder().setHealthState(HealthState.NORMAL)
                .setSubcategory(TargetHealthSubCategory.DISCOVERY).build();
        Map<Long, TargetHealth> targetHealthFromDiags = ImmutableMap.of(1111L, targetHealth1, 2222L, targetHealth2);
        mockTarget(1111L, "target1111");
        mockTarget(2222L, "target2222");

        //Two targets loaded from diags.
        healthRetriever.setHealthFromDiags(targetHealthFromDiags);

        //target1 should still be NORMAL.
        Assert.assertEquals(HealthState.NORMAL, getTargetHealth(targetId).getHealthState());

        //target1111 and target 2222 are loaded from diags.
        TargetHealth targetHealthRetrieved1 = getTargetHealth(1111L);
        Assert.assertEquals(HealthState.CRITICAL, targetHealthRetrieved1.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DELAYED_DATA, targetHealthRetrieved1.getSubcategory());

        TargetHealth targetHealthRetrieved2 = getTargetHealth(2222L);
        Assert.assertEquals(HealthState.NORMAL, targetHealthRetrieved2.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, targetHealthRetrieved2.getSubcategory());
    }

    /**
     * Test single target health - validation failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSingleTargetHealthValidationFailed() throws Exception   {
        final Target target = mockTarget();
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
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(ImmutableList.of(perfectProbe));

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
        Assert.assertFalse(healthInfo.getErrorTypeInfoList().isEmpty());
        Assert.assertEquals(ErrorTypeInfoCase.DATA_IS_MISSING_ERROR_TYPE,
                        healthInfo.getErrorTypeInfoList().get(0).getErrorTypeInfoCase());

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
        final Target target = mockTarget();
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
        final Target target = mockTarget();
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
        final Target target = mockTarget();
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
     * Test the detection of delayed data .
     */
    @Test
    public void testDelayedDataDefaultDiscoveryInterval() {
        final Target target = mockTarget();
        final long targetId = target.getId();

        //Set the last successful discovery to have been long ago.
        Pair<Long, Long> interval = setLastDiscoveryTime(targetId,
                DEFAULT_REDISCOVERY_PERIOD * 500,
                DEFAULT_REDISCOVERY_PERIOD * 1000 * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER
                        * 2);

        TargetHealth healthInfo = getTargetHealth(targetId);
        assertDelayedData(healthInfo, interval.getFirst(), interval.getSecond());
        Assert.assertEquals(System.currentTimeMillis(), healthInfo.getTimeOfCheck(), 1000);
        Assert.assertFalse(healthInfo.hasConsecutiveFailureCount());

        //Now add an actual successful discovery and change the record about the last discovery to be
        //not too old but too slow.
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        discovery.success();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        interval = setLastDiscoveryTime(targetId,
                DEFAULT_REDISCOVERY_PERIOD * 1000 * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER * 2);

        healthInfo = getTargetHealth(targetId);
        assertDelayedData(healthInfo, interval.getFirst(), interval.getSecond());
    }

    /**
     * Test delayed data calculation after setting the rediscovery interval manually.
     */
    @Test
    public void testDelayedDataCustomRediscoveryInterval() {
        // Let the length of the discovery time exceed the threshold
        final long targetId = mockTarget().getId();
        Pair<Long, Long> interval = setLastDiscoveryTime(targetId,
                TimeUnit.SECONDS.toMillis(DEFAULT_REDISCOVERY_PERIOD)
                        * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER + 1);
        TargetHealth healthInfo = getTargetHealth(targetId);
        assertDelayedData(healthInfo, interval.getFirst(), interval.getSecond());

        // Since we have a larger rediscovery interval, the new threshold would be 10*(10000*1000),
        // versus the old 10*(600*1000). Now, the length of discovery would be well within the
        // threshold
        final long newRediscoveryInterval = 10000;
        setRediscoveryInterval(newRediscoveryInterval);
        setLastDiscoveryTime(targetId,
                TimeUnit.SECONDS.toMillis(DEFAULT_REDISCOVERY_PERIOD)
                        * DELAYED_DATA_PERIOD_THRESHOLD_MULTIPLIER + 1);
        healthInfo = getTargetHealth(targetId);
        Assert.assertNotEquals(HealthState.CRITICAL, healthInfo.getHealthState());
        Assert.assertNotEquals(TargetHealthSubCategory.DELAYED_DATA, healthInfo.getSubcategory());
    }

    @Nonnull
    private Target mockTarget() {
        Target target = mock(Target.class);
        when(target.getProbeId()).thenReturn(TargetHealthRetrieverTest.PROBE_ID);
        when(target.getId()).thenReturn((long)1);
        when(target.getDisplayName()).thenReturn("43");
        when(target.getProbeInfo()).thenReturn(probeInfo);
        when(targetStore.getTargets(Collections.singleton((long)1))).thenReturn(Collections.singletonList(target));
        return target;
    }

    @Nonnull
    private Target mockTarget(long id, String displayName) {
        Target target = mock(Target.class);
        when(target.getProbeId()).thenReturn(TargetHealthRetrieverTest.PROBE_ID);
        when(target.getId()).thenReturn((long)id);
        when(target.getDisplayName()).thenReturn(displayName);
        when(target.getProbeInfo()).thenReturn(probeInfo);
        when(targetStore.getTargets(Collections.singleton(id))).thenReturn(Collections.singletonList(target));
        return target;
    }

    private TargetHealth getTargetHealth(long targetId) {
        return healthRetriever.getTargetHealth(Collections.singleton(targetId), false).get(targetId);
    }

    /**
     * Mock a {@link ProbeRegistrationDescription} object with health state and status derived from
     * the given probe and platform versions.
     *
     * @param probeVersion the probe version
     * @return the mocked probe registration
     */
    private static ProbeRegistrationDescription mockProbe(final String probeVersion) {
        final Pair<HealthState, String> probeHealth = ProbeVersionFactory.deduceProbeHealth(probeVersion,
                "8.3.6");
        return new ProbeRegistrationDescription(
                0, 0, "", probeVersion, 0, "", probeHealth.getFirst(), probeHealth.getSecond());
    }

    /**
     * Set the rediscovery interval that will be returned by the scheduler. When -1, the default
     * probe interval is used.
     *
     * @param rediscoveryInterval the rediscovery interval
     */
    private static void setRediscoveryInterval(long rediscoveryInterval) {
        final TargetDiscoverySchedule targetDiscoverySchedule = mock(TargetDiscoverySchedule.class);
        when(targetDiscoverySchedule.getScheduleInterval(Mockito.any()))
                .thenReturn(rediscoveryInterval);
        when(scheduler.getDiscoverySchedule(Mockito.anyLong(), Mockito.any()))
                .thenReturn(Optional.of(targetDiscoverySchedule));
    }

    /**
     * See {@link #setRediscoveryInterval(long)}.
     */
    private static void setRediscoveryInterval() {
        setRediscoveryInterval(DEFAULT_REDISCOVERY_PERIOD);
    }

    /**
     * Set the last successful discovery given the length of the discovery and the offset. Return the
     * interval of the discovery. The following would be the calculated discovery interval like the following
     * (using a timeline).
     *    Timeline:       |-----------------------|----------------|
     *                    A        (length)       B          (currentTime)
     *                    |_______________________|________________|
     *                        DiscoveryInterval      endTimeOffset
     * @return (A, B)
     */
    @Nonnull
    private Pair<Long, Long> setLastDiscoveryTime(long targetId, long length, long endTimeOffset) {
        long endTime = System.currentTimeMillis() - endTimeOffset;
        long startTime = endTime - length;
        Pair<Long, Long> interval = new Pair<>(startTime, endTime);
        when(targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId)).thenReturn(interval);
        return interval;
    }

    /**
     * See {@link #setLastDiscoveryTime(long, long, long)}.
     */
    @Nonnull
    private Pair<Long, Long> setLastDiscoveryTime(long targetId, long length) {
        return setLastDiscoveryTime(targetId, length, 0);
    }

    private void assertDelayedData(TargetHealth healthInfo, long discoveryCompletionStart,
            long discoveryCompletionTime) {
        Assert.assertEquals(HealthState.MAJOR, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DELAYED_DATA, healthInfo.getSubcategory());
        Assert.assertEquals(1, healthInfo.getErrorTypeInfoList().size());
        Assert.assertEquals(delayedDataError, healthInfo.getErrorTypeInfoList().get(0));
        Assert.assertEquals(discoveryCompletionStart, healthInfo.getLastSuccessfulDiscoveryStartTime());
        Assert.assertEquals(discoveryCompletionTime, healthInfo.getLastSuccessfulDiscoveryCompletionTime());
    }

     /**
     * Test when the target discovery is success with warnings.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWhenDiscoverySuccessWithWarnings() throws Exception   {
        final Target target = mockTarget();
        final long targetId = target.getId();
        when(probeStore.getProbeRegistrationsForTarget(any())).thenReturn(ImmutableList.of(perfectProbe));

        final Validation validation = new Validation(PROBE_ID,  targetId, identityProvider);
        final Discovery discovery = new Discovery(PROBE_ID, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));

        ErrorTypeInfo errorTypeInfo = ErrorTypeInfo.newBuilder().setOtherErrorType(ErrorTypeInfo.OtherErrorType.getDefaultInstance()).build();

        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                .addErrorTypeInfo(errorTypeInfo)
                .setDescription("Discovery partially successful:MAJOR")
                .setSeverity(ErrorDTO.ErrorSeverity.WARNING)
                .build();

        discovery.addError(errorDTO);

        TargetHealth healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(HealthState.MAJOR, healthInfo.getHealthState());
        Assert.assertEquals(TargetHealthSubCategory.DISCOVERY, healthInfo.getSubcategory());
        Assert.assertEquals(healthInfo.getErrorTypeInfoList().size(), 1);
        Assert.assertEquals(errorTypeInfo, healthInfo.getErrorTypeInfo(0));
    }
}
