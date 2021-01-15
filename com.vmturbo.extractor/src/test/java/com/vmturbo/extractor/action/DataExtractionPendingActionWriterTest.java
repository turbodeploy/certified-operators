package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.schema.enums.EntityType.CONTAINER_SPEC;
import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_VOLUME;
import static com.vmturbo.extractor.schema.enums.EntityType.WORKLOAD_CONTROLLER;
import static com.vmturbo.extractor.schema.enums.MetricType.VCPU_REQUEST;
import static com.vmturbo.extractor.schema.enums.MetricType.VMEM;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.export.schema.Action;
import com.vmturbo.extractor.export.schema.CommodityChange;
import com.vmturbo.extractor.export.schema.ExportedObject;
import com.vmturbo.extractor.export.schema.MoveChange;
import com.vmturbo.extractor.export.schema.RelatedEntity;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test for {@link DataExtractionPendingActionWriter}.
 */
public class DataExtractionPendingActionWriterTest {

    private static final Logger logger = LogManager.getLogger();

    private static final MultiStageTimer timer = new MultiStageTimer(logger);

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

    private static final ActionOrchestratorAction COMPOUND_MOVE = ActionOrchestratorAction.newBuilder()
            .setActionId(1001L)
            .setActionSpec(ActionSpec.newBuilder()
                    .setActionState(ActionState.READY)
                    .setActionMode(ActionMode.MANUAL)
                    .setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .setSeverity(Severity.MAJOR)
                    .setDescription("Move vm1 from host1 to host2")
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
                                                    .setResource(ActionEntity.newBuilder()
                                                            .setId(volume1)
                                                            .setType(EntityType.VIRTUAL_VOLUME_VALUE)))
                                            .addChanges(ChangeProvider.newBuilder()
                                                    .setSource(ActionEntity.newBuilder()
                                                            .setId(storage3)
                                                            .setType(EntityType.STORAGE_VALUE))
                                                    .setDestination(ActionEntity.newBuilder()
                                                            .setId(storage4)
                                                            .setType(EntityType.STORAGE_VALUE))
                                                    .setResource(ActionEntity.newBuilder()
                                                            .setId(volume2)
                                                            .setType(EntityType.VIRTUAL_VOLUME_VALUE)))
                                            .addChanges(ChangeProvider.newBuilder()
                                                    .setSource(ActionEntity.newBuilder()
                                                            .setId(storage5)
                                                            .setType(EntityType.STORAGE_VALUE))
                                                    .setDestination(ActionEntity.newBuilder()
                                                            .setId(storage6)
                                                            .setType(EntityType.STORAGE_VALUE))
                                                    .setResource(ActionEntity.newBuilder()
                                                            .setId(volume3)
                                                            .setType(EntityType.VIRTUAL_VOLUME_VALUE)))))
                            .setExecutable(true)
                            .setSupportingLevel(SupportLevel.SUPPORTED)
                            .setDeprecatedImportance(0)
                            .setExplanation(Explanation.getDefaultInstance())))
            .build();

    private static final ActionOrchestratorAction ATOMIC_RESIZE = ActionOrchestratorAction.newBuilder()
            .setActionId(1002L)
            .setActionSpec(ActionSpec.newBuilder()
                    .setActionState(ActionState.READY)
                    .setActionMode(ActionMode.AUTOMATIC)
                    .setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .setSeverity(Severity.MAJOR)
                    .setDescription("atomic resize")
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
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                                    .setOldCapacity(100)
                                                    .setNewCapacity(200))
                                            .addResizes(ResizeInfo.newBuilder()
                                                    .setTarget(ActionEntity.newBuilder()
                                                            .setId(containerSpec1)
                                                            .setType(EntityType.CONTAINER_SPEC_VALUE))
                                                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                                                    .setOldCapacity(500)
                                                    .setNewCapacity(400))))
                            .setExecutable(true)
                            .setSupportingLevel(SupportLevel.SUPPORTED)
                            .setDeprecatedImportance(0)
                            .setExplanation(Explanation.getDefaultInstance())))
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);

    private DataExtractionFactory dataExtractionFactory = mock(DataExtractionFactory.class);

    private RelatedEntitiesExtractor relatedEntitiesExtractor = mock(RelatedEntitiesExtractor.class);

    private DataProvider dataProvider = mock(DataProvider.class);
    private final SupplyChain supplyChain = mock(SupplyChain.class);
    private final TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private MutableLong lastWrite = new MutableLong(0);

    private DataExtractionPendingActionWriter writer;

    private List<Action> actionsCapture;

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        // capture actions sent to kafka
        this.actionsCapture = new ArrayList<>();
        doAnswer(inv -> {
            Collection<ExportedObject> exportedObjects = inv.getArgumentAt(0, Collection.class);
            if (exportedObjects != null) {
                actionsCapture.addAll(exportedObjects.stream()
                        .map(ExportedObject::getAction)
                        .collect(Collectors.toList()));
            }
            return null;
        }).when(extractorKafkaSender).send(any());

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

        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        when(dataProvider.getSupplyChain()).thenReturn(supplyChain);

        doReturn(relatedEntitiesExtractor).when(dataExtractionFactory).newRelatedEntitiesExtractor(any(), any(), any());

        RelatedEntity relatedEntity1 = new RelatedEntity();
        relatedEntity1.setId(host1);
        relatedEntity1.setName(String.valueOf(host1));
        RelatedEntity relatedEntity2 = new RelatedEntity();
        relatedEntity2.setId(storage1);
        relatedEntity2.setName(String.valueOf(storage1));
        doReturn(ImmutableMap.of(
                PHYSICAL_MACHINE.getLiteral(), Lists.newArrayList(relatedEntity1),
                STORAGE.getLiteral(), Lists.newArrayList(relatedEntity2)
        )).when(relatedEntitiesExtractor).extractRelatedEntities(vm1);

        writer = spy(new DataExtractionPendingActionWriter(extractorKafkaSender, dataExtractionFactory,
                dataProvider, clock, lastWrite));
    }

    /**
     * Test that compound move action is extracted correctly. The action contains 4 providers
     * changes: 1 host move + 3 storage moves.
     */
    @Test
    public void testExtractCompoundMoveAction() {
        // extract
        writer.recordAction(COMPOUND_MOVE);
        writer.write(timer);

        // verify
        assertThat(actionsCapture.size(), is(1));

        final Action action = actionsCapture.get(0);
        // common fields
        verifyCommonFields(action, COMPOUND_MOVE.getActionSpec());
        // target
        assertThat(action.getTarget().getId(), is(vm1));
        assertThat(action.getTarget().getName(), is(String.valueOf(vm1)));
        assertThat(action.getTarget().getType(), is(VIRTUAL_MACHINE.getLiteral()));
        // related
        assertThat(action.getRelated().size(), is(2));
        assertThat(action.getRelated().get(PHYSICAL_MACHINE.getLiteral()).get(0).getId(), is(host1));
        assertThat(action.getRelated().get(STORAGE.getLiteral()).get(0).getId(), is(storage1));

        // type specific info
        assertThat(action.getMoveInfo().size(), is(4));
        MoveChange moveChange = action.getMoveInfo().get(PHYSICAL_MACHINE.getLiteral());
        assertThat(moveChange.getFrom().getId(), is(host1));
        assertThat(moveChange.getFrom().getName(), is(String.valueOf(host1)));
        assertThat(moveChange.getFrom().getType(), is(nullValue()));
        assertThat(moveChange.getTo().getId(), is(host2));
        assertThat(moveChange.getTo().getName(), is(String.valueOf(host2)));
        assertThat(moveChange.getTo().getType(), is(nullValue()));

        MoveChange moveChange2 = action.getMoveInfo().get(STORAGE.getLiteral());
        assertThat(moveChange2.getResource().size(), is(1));
        assertThat(moveChange2.getResource().get(0).getId(), is(volume1));
        assertThat(moveChange2.getResource().get(0).getName(), is(String.valueOf(volume1)));
        assertThat(moveChange2.getResource().get(0).getType(), is(VIRTUAL_VOLUME.getLiteral()));
        assertThat(moveChange2.getFrom().getId(), is(storage1));
        assertThat(moveChange2.getFrom().getName(), is(String.valueOf(storage1)));
        assertThat(moveChange2.getFrom().getType(), is(nullValue()));
        assertThat(moveChange2.getTo().getId(), is(storage2));
        assertThat(moveChange2.getTo().getName(), is(String.valueOf(storage2)));
        assertThat(moveChange2.getTo().getType(), is(nullValue()));

        assertThat(action.getMoveInfo().get(STORAGE.getLiteral() + "_1"), is(notNullValue()));
        assertThat(action.getMoveInfo().get(STORAGE.getLiteral() + "_2"), is(notNullValue()));
    }

    /**
     * Test that atomic resize action is extracted correctly.
     */
    @Test
    public void testExtractAtomicResizeAction() {
        // extract
        writer.recordAction(ATOMIC_RESIZE);
        writer.write(timer);

        // verify
        assertThat(actionsCapture.size(), is(1));

        final Action action = actionsCapture.get(0);
        // common fields
        verifyCommonFields(action, ATOMIC_RESIZE.getActionSpec());
        // target
        assertThat(action.getTarget().getId(), is(workloadController1));
        assertThat(action.getTarget().getName(), is(String.valueOf(workloadController1)));
        assertThat(action.getTarget().getType(), is(WORKLOAD_CONTROLLER.getLiteral()));

        // type specific info
        assertThat(action.getResizeInfo().size(), is(2));
        CommodityChange change = action.getResizeInfo().get(VMEM.getLiteral());

        assertThat(change.getFrom(), is(100F));
        assertThat(change.getTo(), is(200F));
        assertThat(change.getUnit(), is("KB"));
        assertThat(change.getAttribute(), is("CAPACITY"));
        assertThat(change.getTarget().getId(), is(containerSpec1));
        assertThat(change.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));

        CommodityChange change2 = action.getResizeInfo().get(VCPU_REQUEST.getLiteral());

        assertThat(change2.getFrom(), is(500F));
        assertThat(change2.getTo(), is(400F));
        assertThat(change2.getUnit(), is("millicore"));
        assertThat(change2.getAttribute(), is("CAPACITY"));
        assertThat(change2.getTarget().getId(), is(containerSpec1));
        assertThat(change2.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change2.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));
    }

    /**
     * Verify the common fields in {@link Action}.
     *
     * @param action action sent to Kafka
     * @param actionSpec action from AO
     */
    private void verifyCommonFields(Action action, ActionSpec actionSpec) {
        assertThat(action.getTimestamp(), is(ExportUtils.getFormattedDate(clock.millis())));
        assertThat(action.getCreationTime(), is(ExportUtils.getFormattedDate(actionSpec.getRecommendationTime())));
        assertThat(action.getId(), is(actionSpec.getRecommendation().getId()));
        assertThat(action.getState(), is(actionSpec.getActionState().name()));
        assertThat(action.getCategory(), is(actionSpec.getCategory().name()));
        assertThat(action.getMode(), is(actionSpec.getActionMode().name()));
        assertThat(action.getSeverity(), is(actionSpec.getSeverity().name()));
        assertThat(action.getDetails(), is(actionSpec.getDescription()));
        assertThat(action.getType(), is(ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation()).name()));
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
}