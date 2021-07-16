package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.schema.enums.EntityType.CONTAINER_SPEC;
import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_VOLUME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.action.commodity.ActionCommodityData;
import com.vmturbo.extractor.action.commodity.ActionCommodityDataRetriever;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.ActionImpactedCosts;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ActionCommodity;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ImpactedMetric;
import com.vmturbo.extractor.schema.json.common.BuyRiInfo;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory.RICoverageData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link ActionAttributeExtractor}.
 */
public class ActionAttributeExtractorTest {

    private static final long vm1 = 10L;
    private static final long vm2 = 11L;
    private static final long host1 = 20L;
    private static final long host2 = 21L;
    private static final long storage1 = 30L;
    private static final long storage2 = 31L;
    private static final long storage3 = 32L;
    private static final long storage4 = 33L;
    private static final long storage5 = 34L;
    private static final long storage6 = 35L;
    private static final long volume1 = 41L;
    private static final long volume2 = 42L;
    private static final long volume3 = 43L;
    private static final long workloadController1 = 51L;
    private static final long containerSpec1 = 61L;
    private static final long computeTier1 = 71L;
    private static final long region1 = 81L;
    private static final long account1 = 91L;
    private static final OffsetDateTime NOW = OffsetDateTime.now(ZoneOffset.UTC);

    private static final ActionDTO.ActionSpec COMPOUND_MOVE = ActionSpec.newBuilder()
        .setRecommendation(ActionDTO.Action.newBuilder()
                .setId(1001L)
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vm1)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(host1)
                                                .setType(EntityType.PHYSICAL_MACHINE_VALUE))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(host2)
                                                .setType(EntityType.PHYSICAL_MACHINE_VALUE)))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(storage1)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(storage2)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .addResource(ActionEntity.newBuilder()
                                                .setId(volume1)
                                                .setType(EntityType.VIRTUAL_VOLUME_VALUE)))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(storage3)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(storage4)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .addResource(ActionEntity.newBuilder()
                                                .setId(volume2)
                                                .setType(EntityType.VIRTUAL_VOLUME_VALUE)))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(storage5)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(storage6)
                                                .setType(EntityType.STORAGE_VALUE))
                                        .addResource(ActionEntity.newBuilder()
                                                .setId(volume3)
                                                .setType(EntityType.VIRTUAL_VOLUME_VALUE)))))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.getDefaultInstance()))
            .build();

    private static final CommodityType VMEM = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
            .build();

    private static final CommodityType VCPU_REQUEST = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE)
            .build();

    private static final ActionDTO.ActionSpec ATOMIC_RESIZE = ActionDTO.ActionSpec.newBuilder()
        .setRecommendation(ActionDTO.Action.newBuilder()
            .setId(1002L)
            .setInfo(ActionInfo.newBuilder()
                    .setAtomicResize(AtomicResize.newBuilder()
                            .setExecutionTarget(ActionEntity.newBuilder()
                                    .setId(workloadController1)
                                    .setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                            .addResizes(ResizeInfo.newBuilder()
                                    .setTarget(ActionEntity.newBuilder()
                                            .setId(containerSpec1)
                                            .setType(EntityType.CONTAINER_SPEC_VALUE))
                                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                    .setCommodityType(VMEM)
                                    .setOldCapacity(100)
                                    .setNewCapacity(200))
                            .addResizes(ResizeInfo.newBuilder()
                                    .setTarget(ActionEntity.newBuilder()
                                            .setId(containerSpec1)
                                            .setType(EntityType.CONTAINER_SPEC_VALUE))
                                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                    .setCommodityType(VCPU_REQUEST)
                                    .setOldCapacity(500)
                                    .setNewCapacity(400))))
            .setExecutable(true)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.getDefaultInstance()))
        .build();

    private static final ActionDTO.ActionSpec NORMAL_RESIZE = ActionDTO.ActionSpec.newBuilder()
            .setRecommendation(ActionDTO.Action.newBuilder()
                    .setId(1002L)
                    .setInfo(ActionInfo.newBuilder()
                            .setResize(Resize.newBuilder()
                                            .setTarget(ActionEntity.newBuilder()
                                                    .setId(containerSpec1)
                                                    .setType(EntityType.CONTAINER_SPEC_VALUE))
                                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                            .setCommodityType(VMEM)
                                            .setOldCapacity(100)
                                            .setNewCapacity(200)))
                    .setExecutable(true)
                    .setSupportingLevel(SupportLevel.SUPPORTED)
                    .setDeprecatedImportance(0)
                    .setExplanation(Explanation.getDefaultInstance()))
            .build();

    private static final ActionDTO.ActionSpec DELETE = ActionDTO.ActionSpec.newBuilder()
            .setRecommendation(ActionDTO.Action.newBuilder()
                .setId(10003L)
                    .setDeprecatedImportance(0)
                    .setInfo(ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                .setId(volume1)
                                .setType(EntityType.VIRTUAL_VOLUME_VALUE))
                            .setFilePath("foo/bar")))
                    .setExplanation(Explanation.newBuilder()
                            .setDelete(DeleteExplanation.newBuilder()
                                    .setModificationTimeMs(NOW.toInstant().toEpochMilli())
                                    .setSizeKb(Units.NUM_OF_KB_IN_MB * 2))))
            .build();

    private static final ActionDTO.ActionSpec BUY_RI = ActionSpec.newBuilder()
            .setRecommendation(ActionDTO.Action.newBuilder()
                    .setId(10004L)
                    .setDeprecatedImportance(0)
                    .setInfo(ActionInfo.newBuilder()
                            .setBuyRi(BuyRI.newBuilder()
                                    .setBuyRiId(51234L)
                                    .setCount(2)
                                    .setComputeTier(ActionEntity.newBuilder()
                                            .setId(computeTier1)
                                            .setType(EntityType.COMPUTE_TIER_VALUE)
                                            .build())
                                    .setMasterAccount(ActionEntity.newBuilder()
                                            .setId(account1)
                                            .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                            .build())
                                    .setRegion(ActionEntity.newBuilder()
                                            .setId(region1)
                                            .setType(EntityType.REGION_VALUE)
                                            .build())
                                    .setTargetEntity(ActionEntity.newBuilder()
                                            .setId(vm1)
                                            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                            .build())
                                    .build()))
                    .setExplanation(Explanation.getDefaultInstance()))
            .build();

    private static final ActionInfo SCALE_ACTION_INFO = ActionInfo.newBuilder().setScale(
            Scale.newBuilder()
                    .addCommodityResizes(ResizeInfo.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(0).build())
                            .setNewCapacity(20)
                            .setOldCapacity(10)
                            .build())
                    .setTarget(ActionEntity.newBuilder()
                            .setId(vm1)
                            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                            .build())
                    .addChanges(ChangeProvider.newBuilder()
                            .setSource(ActionEntity.newBuilder()
                                    .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                    .setId(host1)
                                    .build())
                            .setDestination(ActionEntity.newBuilder()
                                    .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                    .setId(host2)
                                    .build())
                            .build())
                    .build()).build();

    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private ActionCommodityDataRetriever actionCommodityDataRetriever = mock(
            ActionCommodityDataRetriever.class);

    private ActionCommodityData actionCommodityData = mock(ActionCommodityData.class);

    private ExtractorFeatureFlags featureFlags = mock(ExtractorFeatureFlags.class);

    private DataProvider dataProvider = mock(DataProvider.class);
    private VolumeAttachmentHistoryRetriever volumeAttachmentHistoryRetriever = mock(VolumeAttachmentHistoryRetriever.class);

    private ActionAttributeExtractor actionAttributeExtractor = new ActionAttributeExtractor(actionCommodityDataRetriever,
            dataProvider,
            volumeAttachmentHistoryRetriever);

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        // capture actions sent to kafka
        mockEntity(vm1, EntityType.VIRTUAL_MACHINE_VALUE);
        mockEntity(vm2, EntityType.VIRTUAL_MACHINE_VALUE);
        mockEntity(host1, EntityType.PHYSICAL_MACHINE_VALUE);
        mockEntity(host2, EntityType.PHYSICAL_MACHINE_VALUE);
        mockEntity(storage1, EntityType.STORAGE_VALUE);
        mockEntity(storage2, EntityType.STORAGE_VALUE);
        mockEntity(storage3, EntityType.STORAGE_VALUE);
        mockEntity(storage4, EntityType.STORAGE_VALUE);
        mockEntity(storage5, EntityType.STORAGE_VALUE);
        mockEntity(storage6, EntityType.STORAGE_VALUE);
        mockEntity(volume1, EntityType.VIRTUAL_VOLUME_VALUE);
        mockEntity(volume2, EntityType.VIRTUAL_VOLUME_VALUE);
        mockEntity(volume3, EntityType.VIRTUAL_VOLUME_VALUE);
        mockEntity(workloadController1, EntityType.WORKLOAD_CONTROLLER_VALUE);
        mockEntity(containerSpec1, EntityType.CONTAINER_SPEC_VALUE);
        mockEntity(computeTier1, EntityType.COMPUTE_TIER_VALUE);
        mockEntity(region1, EntityType.REGION_VALUE);
        mockEntity(account1, EntityType.BUSINESS_ACCOUNT_VALUE);
        when(actionCommodityDataRetriever.getActionCommodityData(any())).thenReturn(
                actionCommodityData);
    }

    ReportingActionAttributes extractSingleActionForReporting(ActionSpec actionSpec) {
        return actionAttributeExtractor.extractReportingAttributes(
                Collections.singletonList(actionSpec),
                topologyGraph).get(actionSpec.getRecommendation().getId());
    }

    /**
     * Test extracting {@link ActionAttributes} from a move action.
     */
    @Test
    public void testMoveExtraction() {
        ReportingActionAttributes actionAttributes = extractSingleActionForReporting(COMPOUND_MOVE);
        validateMoveInfo(actionAttributes.getMoveInfo());
    }

    /**
     * Test extracting {@link ActionAttributes} from an atomic resize action.
     */
    @Test
    public void testAtomicResizeExtraction() {
        ReportingActionAttributes actionAttributes = extractSingleActionForReporting(ATOMIC_RESIZE);
        validateAtomicResizeInfo(actionAttributes.getResizeInfo());
    }

    /**
     * Test that atomic resize action is flattened into multiple resize actions with same oid.
     */
    @Test
    public void testPopulateAtomicResizeAttributesForExporter() {
        final Action exportedAction = new Action();
        final long actionId = ATOMIC_RESIZE.getRecommendation().getId();
        exportedAction.setOid(actionId);

        final Long2ObjectMap<Action> actions = new Long2ObjectOpenHashMap<>();
        actions.put(actionId, exportedAction);

        // populate
        List<Action> exportedActions = actionAttributeExtractor.populateExporterAttributes(
                Collections.singletonList(ATOMIC_RESIZE), topologyGraph, actions);

        // expect two flattened actions
        assertThat(exportedActions.size(), is(2));
        assertThat(exportedActions.get(0).getOid(), is(exportedActions.get(1).getOid()));

        // verify resizeInfo in each action
        Map<String, CommodityChange> resizeInfos = exportedActions.stream()
                .map(Action::getResizeInfo)
                .collect(Collectors.toMap(CommodityChange::getCommodityType, r -> r));
        final CommodityChange resizeInfo1 = resizeInfos.get(MetricType.VMEM.getLiteral());
        final CommodityChange resizeInfo2 = resizeInfos.get(MetricType.VCPU_REQUEST.getLiteral());

        assertThat(resizeInfo1.getFrom(), is(100f));
        assertThat(resizeInfo1.getTo(), is(200f));
        assertThat(resizeInfo1.getUnit(), is("KB"));
        assertThat(resizeInfo1.getTarget().getOid(), is(containerSpec1));
        assertThat(resizeInfo1.getAttribute(), is(CommodityAttribute.CAPACITY.name()));

        assertThat(resizeInfo2.getFrom(), is(500f));
        assertThat(resizeInfo2.getTo(), is(400f));
        assertThat(resizeInfo2.getUnit(), is("mCores"));
        assertThat(resizeInfo2.getTarget().getOid(), is(containerSpec1));
        assertThat(resizeInfo2.getAttribute(), is(CommodityAttribute.CAPACITY.name()));
    }

    /**
     * Test that for completed actions we don't populate impact.
     */
    @Test
    public void testNotPopulatingMetricImpactForCompletedAction() {
        // ARRANGE
        final ActionSpec scaleAction = createAction(SCALE_ACTION_INFO, ActionState.SUCCEEDED);
        final Action exportedAction = new Action();
        final long actionId = scaleAction.getRecommendation().getId();
        exportedAction.setOid(actionId);

        final Long2ObjectMap<Action> actions = new Long2ObjectOpenHashMap<>();
        actions.put(actionId, exportedAction);

        // ACT
        List<Action> exportedActions = actionAttributeExtractor.populateExporterAttributes(
                Collections.singletonList(scaleAction), topologyGraph, actions);

        // ASSERT
        Assert.assertEquals(1, exportedActions.size());
        final MoveChange scaleInfo = exportedActions.get(0).getScaleInfo();
        Assert.assertNull(exportedActions.get(0).getTarget().getAffectedMetrics());
        Assert.assertNull(scaleInfo.getFrom().getAffectedMetrics());
        Assert.assertNull(scaleInfo.getTo().getAffectedMetrics());
    }

    /**
     * Test that for completed actions we populate cost impact.
     */
    @Test
    public void testPopulatingCostImpactForCompletedAction() {
        // ARRANGE
        ActionInfo.Builder scaleWithCloudSavingDetails = SCALE_ACTION_INFO.toBuilder();
        scaleWithCloudSavingDetails.getScaleBuilder()
                .setCloudSavingsDetails(CloudSavingsDetails.newBuilder()
                        .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(4))
                                .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(6))
                                .setCloudCommitmentCoverage(CloudCommitmentCoverage.newBuilder()
                                        .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(5))
                                        .setCapacity(CloudCommitmentAmount.newBuilder().setCoupons(10))))
                        .setProjectedTierCostDetails(TierCostDetails.newBuilder()
                                .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(3))
                                .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(5))
                                .setCloudCommitmentCoverage(CloudCommitmentCoverage.newBuilder()
                                        .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(4))
                                        .setCapacity(CloudCommitmentAmount.newBuilder().setCoupons(10)))));

        final ActionSpec scaleAction = createAction(scaleWithCloudSavingDetails.build(), ActionState.SUCCEEDED);

        final Action exportedAction = new Action();
        final long actionId = scaleAction.getRecommendation().getId();
        exportedAction.setOid(actionId);

        final Long2ObjectMap<Action> actions = new Long2ObjectOpenHashMap<>();
        actions.put(actionId, exportedAction);

        // ACT
        List<Action> exportedActions = actionAttributeExtractor.populateExporterAttributes(
                Collections.singletonList(scaleAction), topologyGraph, actions);

        // ASSERT
        Assert.assertEquals(1, exportedActions.size());
        ActionImpactedCosts affectedCosts = exportedActions.get(0).getTarget().getAffectedCosts();
        Assert.assertNotNull(affectedCosts);
        assertThat(affectedCosts.getBeforeActions().getOnDemandRate(), is(4F));
        assertThat(affectedCosts.getBeforeActions().getOnDemandCost(), is(6F));
        assertThat(affectedCosts.getBeforeActions().getRiCoveragePercentage(), is(50F));
        assertThat(affectedCosts.getAfterActions().getOnDemandRate(), is(3F));
        assertThat(affectedCosts.getAfterActions().getOnDemandCost(), is(5F));
        assertThat(affectedCosts.getAfterActions().getRiCoveragePercentage(), is(40F));
    }

    /**
     * Test populating impact for pending action.
     */
    @Test
    public void testPopulatingImpactForPendingAction() {
        // ARRANGE
        final float vmVmemUsed = 5f;
        final float vmVmemCapacityBefore = 10f;
        final float vmVmemCapacityAfter = 20f;
        final float vmVcpuUsed = 6f;
        final float vmVcpuCapacityBefore = 100f;
        final float vmVcpuCapacityAfter = 200f;
        final float sourceHostMemCapacity = 1000f;
        final float sourceHostMemUsedBefore = 850f;
        final float sourceHostMemUsedAfter = 800f;
        final float sourceHostCpuCapacity = 1000f;
        final float sourceHostCpuUsedBefore = 150f;
        final float sourceHostCpuUsedAfter = 130f;
        final float destHostMemCapacity = 2000f;
        final float destHostMemUsedBefore = 450f;
        final float destHostMemUsedAfter = 500f;
        final float destHostCpuCapacity = 500f;
        final float destHostCpuUsedBefore = 150f;
        final float destHostCpuUsedAfter = 180f;

        final ActionSpec scaleAction = createAction(SCALE_ACTION_INFO, ActionState.READY);
        final Action exportedAction = new Action();
        final long actionId = scaleAction.getRecommendation().getId();
        exportedAction.setOid(actionId);

        final Long2ObjectMap<Action> actions = new Long2ObjectOpenHashMap<>();
        actions.put(actionId, exportedAction);

        final Map<String, ImpactedMetric> vmImpact = new HashMap<>();
        vmImpact.put(CommodityDTO.CommodityType.VMEM.name(),
                createImpactedMetric(vmVmemUsed, vmVmemCapacityBefore, vmVmemUsed, vmVmemCapacityAfter));
        vmImpact.put(CommodityDTO.CommodityType.VCPU.name(),
                createImpactedMetric(vmVcpuUsed, vmVcpuCapacityBefore, vmVcpuUsed, vmVcpuCapacityAfter));
        final Map<String, ImpactedMetric> sourceHostImpact = new HashMap<>();
        sourceHostImpact.put(CommodityDTO.CommodityType.MEM.name(),
                createImpactedMetric(sourceHostMemUsedBefore, sourceHostMemCapacity, sourceHostMemUsedAfter, sourceHostMemCapacity));
        sourceHostImpact.put(CommodityDTO.CommodityType.CPU.name(),
                createImpactedMetric(sourceHostCpuUsedBefore, sourceHostCpuCapacity, sourceHostCpuUsedAfter, sourceHostCpuCapacity));
        final Map<String, ImpactedMetric> destinationHostImpact = new HashMap<>();
        destinationHostImpact.put(CommodityDTO.CommodityType.MEM.name(),
                createImpactedMetric(destHostMemUsedBefore, destHostMemCapacity, destHostMemUsedAfter, destHostMemCapacity));
        destinationHostImpact.put(CommodityDTO.CommodityType.CPU.name(),
                createImpactedMetric(destHostCpuUsedBefore, destHostCpuCapacity, destHostCpuUsedAfter, destHostCpuCapacity));
        when(actionCommodityData.getEntityImpact(vm1)).thenReturn(vmImpact);
        when(actionCommodityData.getEntityImpact(host1)).thenReturn(sourceHostImpact);
        when(actionCommodityData.getEntityImpact(host2)).thenReturn(destinationHostImpact);

        // mock cost
        final BottomUpCostData currentCostData = mock(BottomUpCostData.class);
        final BottomUpCostData projectedCostData = mock(BottomUpCostData.class);
        final RICoverageData currentRiCoverageData = mock(RICoverageData.class);
        final RICoverageData projectedRiCoverageData = mock(RICoverageData.class);

        when(currentCostData.getOnDemandCost(vm1, EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(Optional.of(5F));
        when(currentCostData.getOnDemandRate(vm1, EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(Optional.of(3F));
        when(projectedCostData.getOnDemandCost(vm1, EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(Optional.of(4F));
        when(projectedCostData.getOnDemandRate(vm1, EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(Optional.of(2F));
        when(currentCostData.getOnDemandCost(host1, EntityType.PHYSICAL_MACHINE_VALUE)).thenReturn(Optional.empty());
        when(currentCostData.getOnDemandCost(host2, EntityType.PHYSICAL_MACHINE_VALUE)).thenReturn(Optional.empty());

        when(dataProvider.getBottomUpCostData()).thenReturn(currentCostData);
        when(dataProvider.getProjectedBottomUpCostData()).thenReturn(projectedCostData);
        when(dataProvider.getCurrentRiCoverageData()).thenReturn(currentRiCoverageData);
        when(dataProvider.getProjectedRiCoverageData()).thenReturn(projectedRiCoverageData);

        // ACT
        List<Action> exportedActions = actionAttributeExtractor.populateExporterAttributes(
                Collections.singletonList(scaleAction), topologyGraph, actions);

        // ASSERT
        Assert.assertEquals(1, exportedActions.size());
        final MoveChange scaleInfo = exportedActions.get(0).getScaleInfo();

        // validate affected metrics
        validateImpactedMetric(exportedActions.get(0)
                        .getTarget()
                        .getAffectedMetrics()
                        .get(CommodityDTO.CommodityType.VMEM.name()), vmVmemUsed, vmVmemCapacityBefore,
                vmVmemUsed, vmVmemCapacityAfter);
        validateImpactedMetric(exportedActions.get(0)
                        .getTarget()
                        .getAffectedMetrics()
                        .get(CommodityDTO.CommodityType.VCPU.name()), vmVcpuUsed, vmVcpuCapacityBefore,
                vmVcpuUsed, vmVcpuCapacityAfter);
        validateImpactedMetric(
                scaleInfo.getFrom().getAffectedMetrics().get(CommodityDTO.CommodityType.MEM.name()),
                sourceHostMemUsedBefore, sourceHostMemCapacity, sourceHostMemUsedAfter,
                sourceHostMemCapacity);
        validateImpactedMetric(
                scaleInfo.getFrom().getAffectedMetrics().get(CommodityDTO.CommodityType.CPU.name()),
                sourceHostCpuUsedBefore, sourceHostCpuCapacity, sourceHostCpuUsedAfter,
                sourceHostCpuCapacity);
        validateImpactedMetric(
                scaleInfo.getTo().getAffectedMetrics().get(CommodityDTO.CommodityType.MEM.name()),
                destHostMemUsedBefore, destHostMemCapacity, destHostMemUsedAfter,
                destHostMemCapacity);
        validateImpactedMetric(
                scaleInfo.getTo().getAffectedMetrics().get(CommodityDTO.CommodityType.CPU.name()),
                destHostCpuUsedBefore, destHostCpuCapacity, destHostCpuUsedAfter,
                destHostCpuCapacity);

        ActionImpactedCosts affectedCosts = exportedActions.get(0).getTarget().getAffectedCosts();

        assertThat(affectedCosts.getBeforeActions().getOnDemandCost(), is(5F));
        assertThat(affectedCosts.getBeforeActions().getOnDemandRate(), is(3F));
        assertThat(affectedCosts.getAfterActions().getOnDemandCost(), is(4F));
        assertThat(affectedCosts.getAfterActions().getOnDemandRate(), is(2F));
    }

    /**
     * Test that atomic resize attribute extractor takes percentiles into account.
     */
    @Test
    public void testAtomicResizePercentile() {
        CommodityPercentileChange vmemPercentileChange = new CommodityPercentileChange();
        vmemPercentileChange.setAggressiveness(1);
        CommodityPercentileChange vcpuPercentileChange = new CommodityPercentileChange();
        vcpuPercentileChange.setAggressiveness(2);
        when(actionCommodityData.getPercentileChange(containerSpec1, VMEM))
                .thenReturn(vmemPercentileChange);
        when(actionCommodityData.getPercentileChange(containerSpec1, VCPU_REQUEST))
                .thenReturn(vmemPercentileChange);

        ReportingActionAttributes actionAttributes = extractSingleActionForReporting(ATOMIC_RESIZE);
        assertThat(actionAttributes.getResizeInfo().get(MetricType.VMEM.getLiteral()).getPercentileChange(),
                is(vmemPercentileChange));
        assertThat(actionAttributes.getResizeInfo().get(MetricType.VCPU_REQUEST.getLiteral()).getPercentileChange(),
                is(vmemPercentileChange));
    }

    /**
     * Test extracting {@link ActionAttributes} from an normal resize action.
     */
    @Test
    public void testNormalResizeExtraction() {
        ReportingActionAttributes actionAttributes = extractSingleActionForReporting(NORMAL_RESIZE);
        validateNormalResizeInfo(actionAttributes.getResizeInfo());
    }

    /**
     * Test that normal resize attribute extractor takes percentiles into account.
     */
    @Test
    public void testNormalResizePercentile() {
        CommodityPercentileChange percentileChange = new CommodityPercentileChange();
        percentileChange.setAggressiveness(1);
        when(actionCommodityData.getPercentileChange(containerSpec1, VMEM))
                .thenReturn(percentileChange);
        ReportingActionAttributes actionAttributes = extractSingleActionForReporting(NORMAL_RESIZE);
        assertThat(actionAttributes.getResizeInfo().get(MetricType.VMEM.getLiteral()).getPercentileChange(),
                is(percentileChange));
    }

    /**
     * Test extracting {@link ActionAttributes} from a delete action.
     */
    @Test
    public void testDeleteExtraction() {
        final ActionAttributes actionAttributes = extractSingleActionForReporting(DELETE);
        final DeleteInfo deleteInfo = actionAttributes.getDeleteInfo();
        assertThat(deleteInfo.getFilePath(), is("foo/bar"));
        assertThat(deleteInfo.getFileSize(), is(2.0));
        assertThat(deleteInfo.getUnit(), is("MB"));
        assertThat(deleteInfo.getLastModifiedTimestamp(), is(ExportUtils.getFormattedDate(NOW.toInstant().toEpochMilli())));
    }

    /**
     * Test extracting {@link ActionAttributes} from a buyRI action.
     */
    @Test
    public void testBuyRiInfo() {
        ActionAttributes actionAttributes = extractSingleActionForReporting(BUY_RI);
        BuyRiInfo buyRiInfo = actionAttributes.getBuyRiInfo();
        BuyRI expectedBuyRI = BUY_RI.getRecommendation().getInfo().getBuyRi();
        assertThat(buyRiInfo.getCount(), is(expectedBuyRI.getCount()));
        assertThat(buyRiInfo.getComputeTier().getOid(), is(expectedBuyRI.getComputeTier().getId()));
        assertThat(buyRiInfo.getComputeTier().getName(), is(String.valueOf(computeTier1)));
        assertThat(buyRiInfo.getMasterAccount().getOid(), is(expectedBuyRI.getMasterAccount().getId()));
        assertThat(buyRiInfo.getMasterAccount().getName(), is(String.valueOf(account1)));
        assertThat(buyRiInfo.getRegion().getOid(), is(expectedBuyRI.getRegion().getId()));
        assertThat(buyRiInfo.getRegion().getName(), is(String.valueOf(region1)));
        assertThat(buyRiInfo.getTarget().getOid(), is(expectedBuyRI.getTargetEntity().getId()));
        assertThat(buyRiInfo.getTarget().getName(), is(String.valueOf(vm1)));
    }

    private ImpactedMetric createImpactedMetric(float beforeUsed, float beforeCapacity, float afterUsed, float afterCapacity) {
        final ImpactedMetric impactedMetric = new ImpactedMetric();
        final ActionCommodity beforeActionCommodity = new ActionCommodity();
        beforeActionCommodity.addUsed(beforeUsed);
        beforeActionCommodity.addCapacity(beforeCapacity);
        final ActionCommodity afterActionCommodity = new ActionCommodity();
        afterActionCommodity.addUsed(afterUsed);
        afterActionCommodity.addCapacity(afterCapacity);
        impactedMetric.setBeforeActions(beforeActionCommodity);
        impactedMetric.setAfterActions(afterActionCommodity);
        return impactedMetric;
    }

    private ActionSpec createAction(@Nonnull ActionInfo actionInfo, @Nonnull ActionState actionState) {
        return ActionSpec.newBuilder()
                .setRecommendationId(7L)
                .setRecommendationTime(1_000_000)
                .setRecommendation(ActionDTO.Action.newBuilder()
                        .setDeprecatedImportance(123)
                        .setId(1023L)
                        .setSavingsPerHour(CurrencyAmount.getDefaultInstance())
                        .setExplanation(Explanation.getDefaultInstance())
                        .setInfo(actionInfo))
                .setDescription("Action description")
                .setSeverity(ActionDTO.Severity.CRITICAL)
                .setCategory(ActionDTO.ActionCategory.COMPLIANCE)
                .setActionState(actionState)
                .setDecision(ActionDecision.newBuilder()
                        .setExecutionDecision(ExecutionDecision.newBuilder()
                                .setReason(Reason.MANUALLY_ACCEPTED)
                                .setUserUuid("me")))
                .build();
    }

    /**
     * Mock a {@link SupplyChainEntity}.
     *
     * @param entityId   Given entity ID.
     * @param entityType Given entity Type.
     */
    private void mockEntity(long entityId, int entityType) {
        SupplyChainEntity entity = mock(SupplyChainEntity.class);
        when(entity.getOid()).thenReturn(entityId);
        when(entity.getEntityType()).thenReturn(entityType);
        when(entity.getDisplayName()).thenReturn(String.valueOf(entityId));
        doReturn(Optional.of(entity)).when(topologyGraph).getEntity(entityId);
    }

    private void validateMoveInfo(Map<String, MoveChange> moveInfo) {
        assertThat(moveInfo.size(), is(4));
        MoveChange moveChange = moveInfo.get(PHYSICAL_MACHINE.getLiteral());
        assertThat(moveChange.getFrom().getOid(), is(host1));
        assertThat(moveChange.getFrom().getName(), is(String.valueOf(host1)));
        assertThat(moveChange.getFrom().getType(), is(nullValue()));
        assertThat(moveChange.getTo().getOid(), is(host2));
        assertThat(moveChange.getTo().getName(), is(String.valueOf(host2)));
        assertThat(moveChange.getTo().getType(), is(nullValue()));

        MoveChange moveChange2 = moveInfo.get(STORAGE.getLiteral());
        assertThat(moveChange2.getResource().size(), is(1));
        assertThat(moveChange2.getResource().get(0).getOid(), is(volume1));
        assertThat(moveChange2.getResource().get(0).getName(), is(String.valueOf(volume1)));
        assertThat(moveChange2.getResource().get(0).getType(), is(VIRTUAL_VOLUME.getLiteral()));
        assertThat(moveChange2.getFrom().getOid(), is(storage1));
        assertThat(moveChange2.getFrom().getName(), is(String.valueOf(storage1)));
        assertThat(moveChange2.getFrom().getType(), is(nullValue()));
        assertThat(moveChange2.getTo().getOid(), is(storage2));
        assertThat(moveChange2.getTo().getName(), is(String.valueOf(storage2)));
        assertThat(moveChange2.getTo().getType(), is(nullValue()));

        assertThat(moveInfo.get(STORAGE.getLiteral() + "_1"), is(notNullValue()));
        assertThat(moveInfo.get(STORAGE.getLiteral() + "_2"), is(notNullValue()));
    }

    private void validateAtomicResizeInfo(Map<String, CommodityChange> resizeInfo) {
        CommodityChange change = resizeInfo.get(MetricType.VMEM.getLiteral());

        assertThat(change.getFrom(), is(100F));
        assertThat(change.getTo(), is(200F));
        assertThat(change.getUnit(), is("KB"));
        assertThat(change.getAttribute(), is("CAPACITY"));
        assertThat(change.getTarget().getOid(), is(containerSpec1));
        assertThat(change.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));

        CommodityChange change2 = resizeInfo.get(MetricType.VCPU_REQUEST.getLiteral());

        assertThat(change2.getFrom(), is(500F));
        assertThat(change2.getTo(), is(400F));
        assertThat(change2.getUnit(), is("mCores"));
        assertThat(change2.getAttribute(), is("CAPACITY"));
        assertThat(change2.getTarget().getOid(), is(containerSpec1));
        assertThat(change2.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change2.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));

    }

    private void validateNormalResizeInfo(Map<String, CommodityChange> resizeInfo) {
        CommodityChange change = resizeInfo.get(MetricType.VMEM.getLiteral());

        assertThat(change.getFrom(), is(100F));
        assertThat(change.getTo(), is(200F));
        assertThat(change.getUnit(), is("KB"));
        assertThat(change.getAttribute(), is("CAPACITY"));
        assertThat(change.getTarget(), nullValue());
    }

    private void validateImpactedMetric(@Nonnull ImpactedMetric impactedMetric, float beforeUsed, float beforeCapacity, float afterUsed, float afterCapacity) {
        assertThat(impactedMetric.getBeforeActions().getUsed(), is(beforeUsed));
        assertThat(impactedMetric.getBeforeActions().getCapacity(), is(beforeCapacity));
        assertThat(impactedMetric.getAfterActions().getUsed(), is(afterUsed));
        assertThat(impactedMetric.getAfterActions().getCapacity(), is(afterCapacity));
    }
}
