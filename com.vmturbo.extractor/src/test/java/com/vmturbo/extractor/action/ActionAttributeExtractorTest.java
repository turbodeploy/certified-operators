package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.schema.enums.EntityType.CONTAINER_SPEC;
import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_VOLUME;
import static com.vmturbo.extractor.schema.enums.MetricType.VCPU_REQUEST;
import static com.vmturbo.extractor.schema.enums.MetricType.VMEM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
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
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.reporting.ActionAttributes;
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
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.getDefaultInstance()))
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
                                            .setCommodityType(CommodityType.newBuilder()
                                                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
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

    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private ActionAttributeExtractor actionAttributeExtractor = new ActionAttributeExtractor();

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
    }

    /**
     * Test extracting {@link ActionAttributes} from a move action.
     */
    @Test
    public void testMoveExtraction() {
        ActionAttributes actionAttributes = actionAttributeExtractor.extractAttributes(COMPOUND_MOVE, topologyGraph);
        validateMoveInfo(actionAttributes.getMoveInfo());
    }

    /**
     * Test populating an {@link Action} with move attributes.
     */
    @Test
    public void testMovePopulation() {
        Action action = new Action();
        actionAttributeExtractor.populateActionAttributes(COMPOUND_MOVE, topologyGraph, action);
        validateMoveInfo(action.getMoveInfo());
    }

    /**
     * Test extracting {@link ActionAttributes} from an atomic resize action.
     */
    @Test
    public void testAtomicResizeExtraction() {
        ActionAttributes actionAttributes = actionAttributeExtractor.extractAttributes(ATOMIC_RESIZE, topologyGraph);
        validateAtomicResizeInfo(actionAttributes.getResizeInfo());
    }

    /**
     * Test populating an {@link Action} with atomic resize attributes.
     */
    @Test
    public void testAtomicResizePopulation() {
        Action action = new Action();
        actionAttributeExtractor.populateActionAttributes(ATOMIC_RESIZE, topologyGraph, action);
        validateAtomicResizeInfo(action.getResizeInfo());
    }

    /**
     * Test extracting {@link ActionAttributes} from an normal resize action.
     */
    @Test
    public void testNormalResizeExtraction() {
        ActionAttributes actionAttributes = actionAttributeExtractor.extractAttributes(NORMAL_RESIZE, topologyGraph);
        validateNormalResizeInfo(actionAttributes.getResizeInfo());
    }

    /**
     * Test populating an {@link Action} with normal resize attributes.
     */
    @Test
    public void testNormalResizePopulation() {
        Action action = new Action();
        actionAttributeExtractor.populateActionAttributes(NORMAL_RESIZE, topologyGraph, action);
        validateNormalResizeInfo(action.getResizeInfo());
    }

    /**
     * Test extracting {@link ActionAttributes} from a delete action.
     */
    @Test
    public void testDeleteExtraction() {
        ActionAttributes actionAttributes = actionAttributeExtractor.extractAttributes(DELETE, topologyGraph);
        validateDeleteInfo(actionAttributes.getDeleteInfo());
    }

    /**
     * Test populating an {@link Action} with delete attributes.
     */
    @Test
    public void testDeletePopulation() {
        Action action = new Action();
        actionAttributeExtractor.populateActionAttributes(DELETE, topologyGraph, action);
        validateDeleteInfo(action.getDeleteInfo());
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
        CommodityChange change = resizeInfo.get(VMEM.getLiteral());

        assertThat(change.getFrom(), is(100F));
        assertThat(change.getTo(), is(200F));
        assertThat(change.getUnit(), is("KB"));
        assertThat(change.getAttribute(), is("CAPACITY"));
        assertThat(change.getTarget().getOid(), is(containerSpec1));
        assertThat(change.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));

        CommodityChange change2 = resizeInfo.get(VCPU_REQUEST.getLiteral());

        assertThat(change2.getFrom(), is(500F));
        assertThat(change2.getTo(), is(400F));
        assertThat(change2.getUnit(), is("millicore"));
        assertThat(change2.getAttribute(), is("CAPACITY"));
        assertThat(change2.getTarget().getOid(), is(containerSpec1));
        assertThat(change2.getTarget().getName(), is(String.valueOf(containerSpec1)));
        assertThat(change2.getTarget().getType(), is(CONTAINER_SPEC.getLiteral()));

    }

    private void validateNormalResizeInfo(Map<String, CommodityChange> resizeInfo) {
        CommodityChange change = resizeInfo.get(VMEM.getLiteral());

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