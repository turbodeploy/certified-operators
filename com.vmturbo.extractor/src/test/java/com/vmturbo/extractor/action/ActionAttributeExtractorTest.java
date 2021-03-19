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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.extractor.action.percentile.ActionPercentileData;
import com.vmturbo.extractor.action.percentile.ActionPercentileDataRetriever;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.BuyRiInfo;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link ActionAttributeExtractor}.
 */
public class ActionAttributeExtractorTest {

    private static final long vm1 = 10L;
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
                            .setFilePath("foo/bar")))
                    .setExplanation(Explanation.newBuilder()
                            .setDelete(DeleteExplanation.newBuilder()
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

    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private ActionPercentileDataRetriever actionPercentileDataRetriever = mock(
            ActionPercentileDataRetriever.class);

    private ActionPercentileData actionPercentileData = mock(ActionPercentileData.class);

    private ActionAttributeExtractor actionAttributeExtractor = new ActionAttributeExtractor(
            actionPercentileDataRetriever);

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        // capture actions sent to kafka
        mockEntity(vm1, EntityType.VIRTUAL_MACHINE_VALUE);
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
        when(actionPercentileDataRetriever.getActionPercentiles(any())).thenReturn(
                actionPercentileData);
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
        assertThat(resizeInfo2.getUnit(), is("millicore"));
        assertThat(resizeInfo2.getTarget().getOid(), is(containerSpec1));
        assertThat(resizeInfo2.getAttribute(), is(CommodityAttribute.CAPACITY.name()));
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
        when(actionPercentileData.getChange(containerSpec1, VMEM))
                .thenReturn(vmemPercentileChange);
        when(actionPercentileData.getChange(containerSpec1, VCPU_REQUEST))
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
        when(actionPercentileData.getChange(containerSpec1, VMEM))
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
        ActionAttributes actionAttributes = extractSingleActionForReporting(DELETE);
        validateDeleteInfo(actionAttributes.getDeleteInfo());
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
        assertThat(change2.getUnit(), is("millicore"));
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

    private void validateDeleteInfo(DeleteInfo deleteInfo) {
        assertThat(deleteInfo.getFilePath(), is("foo/bar"));
        assertThat(deleteInfo.getFileSize(), is(2.0));
        assertThat(deleteInfo.getUnit(), is("MB"));
    }
}