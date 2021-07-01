package com.vmturbo.topology.processor.targets.status;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

/**
 * Test class for {@link TargetStatusTrackerImpl}.
 */
public class TargetStatusTrackerImplTest {
    private static final long PROBE_ID_1 = 1L;
    private static final long PROBE_ID_2 = 11L;
    private static final long TARGET_ID_1 = 2L;
    private static final long TARGET_ID_2 = 22L;
    private static final String TARGET_ID_FIELD = "TargetId";
    final TargetSpec.Builder targetSpec1 = createTargetSpec(PROBE_ID_1, TARGET_ID_1);
    final TargetSpec.Builder targetSpec2 = createTargetSpec(PROBE_ID_2, TARGET_ID_2);

    private TargetStatusTrackerImpl targetStatusTracker;
    private TargetStore targetStore;
    private ProbeStore probeStore;
    private TargetStatusStore targetStatusStore;
    private IdentityProvider identityProvider;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Captor
    private ArgumentCaptor<TargetStatus> targetStatusCaptor;

    /**
     * Initialize test environment.
     *
     * @throws InvalidTargetException if failed to create a target
     */
    @Before
    public void initialize() throws InvalidTargetException {
        MockitoAnnotations.initMocks(this);

        targetStore = Mockito.mock(TargetStore.class);
        probeStore = Mockito.mock(ProbeStore.class);
        targetStatusStore = Mockito.mock(TargetStatusStore.class);
        targetStatusTracker = new TargetStatusTrackerImpl(targetStatusStore, targetStore, probeStore, clock);
        identityProvider = Mockito.mock(IdentityProvider.class);

        final ProbeInfo probeInfo1 = createProbeInfo(ProbeCategory.ORCHESTRATOR.getCategory(),
                SDKProbeType.SERVICENOW.getProbeType());
        final ProbeInfo probeInfo2 = createProbeInfo(ProbeCategory.HYPERVISOR.getCategory(),
                SDKProbeType.VCENTER.getProbeType());
        Mockito.when(probeStore.getProbe(PROBE_ID_1)).thenReturn(Optional.of(probeInfo1));
        Mockito.when(probeStore.getProbe(PROBE_ID_2)).thenReturn(Optional.of(probeInfo2));
        Mockito.when(probeStore.isProbeConnected(PROBE_ID_1)).thenReturn(true);
        Mockito.when(probeStore.isProbeConnected(PROBE_ID_2)).thenReturn(true);
        final Target target1 = new Target(TARGET_ID_1, probeStore, targetSpec1.build(), false);
        final Target target2 = new Target(TARGET_ID_2, probeStore, targetSpec2.build(), false);
        Mockito.when(targetStore.getTarget(TARGET_ID_1)).thenReturn(Optional.of(target1));
        Mockito.when(targetStore.getTarget(TARGET_ID_2)).thenReturn(Optional.of(target2));
    }

    /**
     * Test that we upload targets statuses from DB into in-memory cache when initialize
     * TargetStatusTracker.
     *
     * @throws TargetStatusStoreException if communication issue with DB happened
     */
    @Test
    public void testTargetStatusCacheInitialising() throws TargetStatusStoreException {
        // ARRANGE
        final List<ProbeStageDetails> discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage #1"));
        final TargetStatus persistedTargetStatus = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_1)
                .addAllStageDetails(discoveryStages)
                .setOperationCompletionTime(clock.millis())
                .build();

        Mockito.when(targetStatusStore.getTargetsStatuses(null)).thenReturn(
                ImmutableMap.of(TARGET_ID_1, persistedTargetStatus));

        // ACT
        Consumer<Runnable> immediateInitializer = Runnable::run;
        final TargetStatusTrackerImpl targetStatusTracker = new TargetStatusTrackerImpl(
                targetStatusStore, targetStore, probeStore, clock, immediateInitializer);
        final Map<Long, TargetStatus> targetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.emptySet(), true);

        // ASSERT
        Assert.assertEquals(1, targetsStatuses.size());
        Assert.assertNotNull(targetsStatuses.get(TARGET_ID_1));
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                targetsStatuses.get(TARGET_ID_1).getStageDetailsList()));
    }

    /**
     * Test persisting/updating target status in accordance with the latest target operation.
     */
    @Test
    public void testPersistingTargetStatusOfTheLatestOperation() {
        // ARRANGE
        final List<ProbeStageDetails> validationStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Validation stage"));
        final Validation validation = createValidation(PROBE_ID_1, TARGET_ID_1, validationStages);
        final List<ProbeStageDetails> discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage #1"));
        final Discovery discovery = createDiscovery(PROBE_ID_1, TARGET_ID_1, discoveryStages);

        // ACT
        // validation of the target
        targetStatusTracker.notifyOperationState(validation);
        // discovery of the target
        targetStatusTracker.notifyOperationState(discovery);
        final Map<Long, TargetStatus> targetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.singleton(TARGET_ID_1), false);

        // ASSERT
        Assert.assertEquals(1L, targetsStatuses.size());
        final TargetStatus targetStatus = targetsStatuses.get(TARGET_ID_1);
        Assert.assertNotNull(targetStatus);
        Assert.assertEquals(1L, targetStatus.getStageDetailsList().size());
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                targetStatus.getStageDetailsList()));
    }

    /**
     * Test setting target status when finished discovery/validation for the target.
     *
     * @throws TargetStatusStoreException if communication issue with DB happened
     */
    @Test
    public void testSetTargetStatus() throws TargetStatusStoreException {
        // ARRANGE
        final ProbeStageDetails discoveryStage1 = createStageDetails(StageStatus.SUCCESS,
                "Discovery stage #1");
        final ProbeStageDetails discoveryStage2 = createStageDetails(StageStatus.FAILURE,
                "Discovery stage #2");
        final List<ProbeStageDetails> discoveryStages = Arrays.asList(discoveryStage1,
                discoveryStage2);
        final Discovery discovery = createDiscovery(PROBE_ID_1, TARGET_ID_1, discoveryStages);

        // ACT
        targetStatusTracker.notifyOperationState(discovery);
        final Map<Long, TargetStatus> targetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.singleton(TARGET_ID_1), true);

        // ASSERT
        Assert.assertEquals(1L, targetsStatuses.size());
        final TargetStatus targetStatus = targetsStatuses.get(TARGET_ID_1);
        Assert.assertNotNull(targetStatus);
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                targetStatus.getStageDetailsList()));
        Mockito.verify(targetStatusStore, Mockito.times(1)).setTargetStatus(
                targetStatusCaptor.capture());
        final TargetStatus targetStatusToPersist = targetStatusCaptor.getValue();
        Assert.assertEquals(TARGET_ID_1, targetStatusToPersist.getTargetId());
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                targetStatusToPersist.getStageDetailsList()));
    }

    /**
     * Test getting statuses for all the targets for which status details were persisted.
     */
    @Test
    public void testGetAllTargetsStatuses() {
        // ARRANGE
        final ProbeStageDetails discoveryStage1 = createStageDetails(StageStatus.SUCCESS,
                "Discovery stage #1");
        final ProbeStageDetails discoveryStage2 = createStageDetails(StageStatus.FAILURE,
                "Discovery stage #2");
        final List<ProbeStageDetails> discoveryStages = Arrays.asList(discoveryStage1,
                discoveryStage2);
        final Discovery discovery1 = createDiscovery(PROBE_ID_1, TARGET_ID_1, discoveryStages);
        final Discovery discovery2 = createDiscovery(PROBE_ID_2, TARGET_ID_2, discoveryStages);

        // emulate discovery of the targets
        targetStatusTracker.notifyOperationState(discovery1);
        targetStatusTracker.notifyOperationState(discovery2);

        // ACT
        final Map<Long, TargetStatus> allTargetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.emptySet(), true);

        // ASSERT
        Assert.assertEquals(2L, allTargetsStatuses.size());
        final TargetStatus target1Status = allTargetsStatuses.get(TARGET_ID_1);
        final TargetStatus target2Status = allTargetsStatuses.get(TARGET_ID_2);
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                target1Status.getStageDetailsList()));
        Assert.assertTrue(CollectionUtils.isEqualCollection(discoveryStages,
                    target2Status.getStageDetailsList()));
    }

    /**
     * Test getting status of the certain target (by target id).
     */
    @Test
    public void testGetStatusOfTheCertainTarget() {
        // ARRANGE
        final List<ProbeStageDetails> target1discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage #1"));
        final List<ProbeStageDetails> target2discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.FAILURE, "Discovery stage #2"));
        final Discovery discovery1 = createDiscovery(PROBE_ID_1, TARGET_ID_1,
                target1discoveryStages);
        final Discovery discovery2 = createDiscovery(PROBE_ID_2, TARGET_ID_2,
                target2discoveryStages);

        // emulate discovery of the targets
        targetStatusTracker.notifyOperationState(discovery1);
        targetStatusTracker.notifyOperationState(discovery2);

        // ACT
        final Map<Long, TargetStatus> targetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.singleton(TARGET_ID_2), false);

        // ASSERT
        Assert.assertEquals(1L, targetsStatuses.size());
        final TargetStatus target2Status = targetsStatuses.get(TARGET_ID_2);
        Assert.assertTrue(CollectionUtils.isEqualCollection(target2discoveryStages,
                target2Status.getStageDetailsList()));
    }

    /**
     * Test remove target status when target is deleted.
     *
     * @throws InvalidTargetException if failed to create a target
     */
    @Test
    public void testRemoveTargetStatus() throws InvalidTargetException {
        // ARRANGE
        final List<ProbeStageDetails> discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage #1"));
        final Discovery discovery = createDiscovery(PROBE_ID_1, TARGET_ID_1,
                discoveryStages);
        final Target target = new Target(TARGET_ID_1, probeStore, targetSpec1.build(), false);


        // discovery of the target
        targetStatusTracker.notifyOperationState(discovery);
        // target removed
        targetStatusTracker.onTargetRemoved(target);

        // ACT
        final Map<Long, TargetStatus> targetsStatuses = targetStatusTracker.getTargetsStatuses(
                Collections.singleton(TARGET_ID_1), false);

        // ASSERT
        Assert.assertTrue(targetsStatuses.isEmpty());
    }

    /**
     * Test not updating target status when target deleted or probe is not connected.
     *
     * @throws TargetStatusStoreException if communication issue with DB happened
     */
    @Test
    public void notUpdateTargetStatusWhenTargetIsDeleted() throws TargetStatusStoreException {
        // ARRANGE
        Mockito.when(probeStore.isProbeConnected(PROBE_ID_1)).thenReturn(false);
        Mockito.when(targetStore.getTarget(TARGET_ID_1)).thenReturn(Optional.empty());
        final List<ProbeStageDetails> discoveryStages = Collections.singletonList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage #1"));
        final Discovery discovery = createDiscovery(PROBE_ID_1, TARGET_ID_1,
                discoveryStages);

        // ACT
        targetStatusTracker.notifyOperationState(discovery);

        // ASSERT
        Mockito.verify(targetStatusStore, Mockito.never()).setTargetStatus(Mockito.any());

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
        targetStatusTracker.notifyOperationState(discovery);
        final Map<Long, DiscoveryFailure> failedDiscoveries = targetStatusTracker.getFailedDiscoveries();
        Assert.assertEquals(1, failedDiscoveries.size());
        final Map.Entry<Long, DiscoveryFailure> entry = failedDiscoveries.entrySet().iterator().next();
        final Long targetId = entry.getKey();
        final DiscoveryFailure failedDiscovery = entry.getValue();
        Assert.assertEquals(TARGET_ID_1, targetId.longValue());
        final LocalDateTime failTime = failedDiscovery.getFailTime();
        Assert.assertNotNull(failTime);
        Assert.assertEquals(1, failedDiscovery.getFailsCount());

        targetStatusTracker.notifyOperationState(discovery);
        final DiscoveryFailure failure = failedDiscoveries.get(targetId);
        Assert.assertNotNull(failure);
        Assert.assertEquals(failTime, failure.getFailTime());
        Assert.assertEquals(2, failedDiscovery.getFailsCount());
    }

    private Discovery createFailedDiscovery() {
        final Discovery discovery = new Discovery(PROBE_ID_1, TARGET_ID_1, DiscoveryType.FULL, identityProvider);
        discovery.addError(ErrorDTO.newBuilder().setErrorType(ErrorType.UNAUTHENTICATED)
                .setSeverity(ErrorSeverity.CRITICAL)
                .setDescription("Wrong credentials").build());
        discovery.fail();
        return discovery;
    }

    /**
     * Test removing information related to failed discoveries on target remove.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRemove() throws Exception {
        final Target target = Mockito.mock(Target.class);
        Mockito.when(target.getId()).thenReturn(TARGET_ID_1);
        targetStatusTracker.notifyOperationState(createFailedDiscovery());
        Assert.assertEquals(1, targetStatusTracker.getFailedDiscoveries().size());
        targetStatusTracker.onTargetRemoved(target);
        Assert.assertEquals(0, targetStatusTracker.getFailedDiscoveries().size());
    }

    private Validation createValidation(long probeId, long targetId,
            @Nonnull Collection<ProbeStageDetails> probeStageDetails) {
        final Validation validation = new Validation(probeId, targetId, identityProvider);
        validation.addStagesReports(probeStageDetails);
        validation.success();
        return validation;
    }

    private Discovery createDiscovery(long probeId, long targetId,
            @Nonnull Collection<ProbeStageDetails> probeStageDetails) {
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.addStagesReports(probeStageDetails);
        discovery.success();
        return discovery;
    }

    @Nonnull
    private Builder createTargetSpec(final long probeId1, final long targetId1) {
        return TargetSpec.newBuilder().setProbeId(probeId1).addAccountValue(
                AccountValue.newBuilder()
                        .setKey(TARGET_ID_FIELD)
                        .setStringValue(String.valueOf(targetId1)));
    }


    @Nonnull
    private ProbeInfo createProbeInfo(String probeCategory, String probeType) {
        return ProbeInfo.newBuilder()
                .setProbeCategory(probeCategory)
                .setProbeType(probeType)
                .addTargetIdentifierField(TARGET_ID_FIELD)
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .build();
    }

    @Nonnull
    private ProbeStageDetails createStageDetails(final StageStatus failure,
            final String description) {
        return ProbeStageDetails.newBuilder()
                .setStatus(failure)
                .setDescription(description)
                .build();
    }

}
