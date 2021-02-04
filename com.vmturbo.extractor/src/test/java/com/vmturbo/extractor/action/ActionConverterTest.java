package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.ActionModel.PendingAction;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.schema.json.reporting.ActionAttributes;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link ActionConverter}.
 */
public class ActionConverterTest {

    private final String description = "My action description.";
    private final double savings = 8;
    private final long actionOid = 870;

    private final ActionSpec actionSpec = ActionSpec.newBuilder()
            .setRecommendationId(7L)
            .setRecommendationTime(1_000_000)
            .setRecommendation(Action.newBuilder()
                    .setDeprecatedImportance(123)
                    .setId(actionOid)
                    .setSavingsPerHour(CurrencyAmount.newBuilder()
                            .setAmount(savings))
                    .setExplanation(Explanation.newBuilder()
                            .setMove(MoveExplanation.newBuilder()
                                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                            .setCompliance(Compliance.getDefaultInstance()))))
                    .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                    .addChanges(ChangeProvider.newBuilder()
                                            .setSource(ActionEntity.newBuilder()
                                                    .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                                    .setId(234))
                                            .setDestination(ActionEntity.newBuilder()
                                                    .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                                    .setId(345)))
                                    .setTarget(ActionEntity.newBuilder()
                                            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                            .setId(123)))))
            .setDescription(description)
            .setSeverity(ActionDTO.Severity.CRITICAL)
            .setCategory(ActionDTO.ActionCategory.COMPLIANCE)
            .setActionState(ActionDTO.ActionState.IN_PROGRESS)
            .setDecision(ActionDecision.newBuilder()
                .setExecutionDecision(ExecutionDecision.newBuilder()
                    .setReason(Reason.MANUALLY_ACCEPTED)
                    .setUserUuid("me")))
            .build();

    private final ActionSpec succeededActionSpec = actionSpec.toBuilder()
            .setActionState(ActionState.SUCCEEDED)
            .setDecision(ActionDecision.newBuilder()
                    .setDecisionTime(1_100_000))
            .setExecutionStep(ExecutionStep.newBuilder()
                    .setCompletionTime(1_200_000))
            .build();

    private final ActionSpec failedActionSpec = succeededActionSpec.toBuilder()
            .setActionState(ActionState.FAILED)
            .build();

    private ActionAttributeExtractor actionAttributeExtractor = mock(ActionAttributeExtractor.class);

    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private ObjectMapper objectMapper = mock(ObjectMapper.class);

    private final ActionAttributes attrs = mock(ActionAttributes.class);

    private final String attrsJson = "{ \"foo\" : \"bar\" }";

    private ActionConverter actionConverter = new ActionConverter(actionAttributeExtractor, objectMapper);

    private RelatedEntitiesExtractor relatedEntitiesExtractor = mock(RelatedEntitiesExtractor.class);

    /**
     * Setup before each test.
     */
    @Before
    public void setup() {
        mockEntity(123, EntityType.VIRTUAL_MACHINE_VALUE);
        mockEntity(234, EntityType.PHYSICAL_MACHINE_VALUE);
        mockEntity(345, EntityType.PHYSICAL_MACHINE_VALUE);

        RelatedEntity relatedEntity1 = new RelatedEntity();
        relatedEntity1.setOid(234L);
        relatedEntity1.setName(String.valueOf(234L));
        RelatedEntity relatedEntity2 = new RelatedEntity();
        relatedEntity2.setOid(456L);
        relatedEntity2.setName(String.valueOf(456L));
        doReturn(ImmutableMap.of(
                PHYSICAL_MACHINE.getLiteral(), Lists.newArrayList(relatedEntity1),
                STORAGE.getLiteral(), Lists.newArrayList(relatedEntity2)
        )).when(relatedEntitiesExtractor).extractRelatedEntities(123);
    }

    /**
     * Test converting an {@link ActionSpec} for a pending actionto a database record.
     *
     * @throws Exception If there is an issue.
     */
    @Test
    public void testPendingActionRecord() throws Exception {
        when(actionAttributeExtractor.extractAttributes(actionSpec, topologyGraph)).thenReturn(attrs);
        when(objectMapper.writeValueAsString(attrs)).thenReturn(attrsJson);
        Record record = actionConverter.makePendingActionRecord(actionSpec, topologyGraph);
        assertThat(record.get(PendingAction.RECOMMENDATION_TIME),
                is(new Timestamp(actionSpec.getRecommendationTime())));
        assertThat(record.get(ActionModel.PendingAction.TYPE), is(ActionType.MOVE));
        assertThat(record.get(ActionModel.PendingAction.CATEGORY), is(ActionCategory.COMPLIANCE));
        assertThat(record.get(ActionModel.PendingAction.SEVERITY), is(Severity.CRITICAL));
        assertThat(record.get(ActionModel.PendingAction.TARGET_ENTITY), is(123L));
        Set<Long> s = new HashSet<>();
        for (long l : record.get(ActionModel.PendingAction.INVOLVED_ENTITIES)) {
            s.add(l);
        }
        assertThat(s, containsInAnyOrder(123L, 234L, 345L));
        assertThat(record.get(ActionModel.PendingAction.DESCRIPTION), is(description));
        assertThat(record.get(ActionModel.PendingAction.SAVINGS), is(savings));
        assertThat(record.get(PendingAction.ACTION_OID), is(actionOid));
        assertThat(record.get(PendingAction.ATTRS).toString(), is(attrsJson));
    }

    /**
     * Test converting an {@link ActionSpec} for a succeeded action to a database record.
     *
     * @throws Exception If there is an issue.
     */
    @Test
    public void testExecutedSucceededActionRecord() throws Exception {
        when(actionAttributeExtractor.extractAttributes(succeededActionSpec, topologyGraph)).thenReturn(attrs);
        when(objectMapper.writeValueAsString(attrs)).thenReturn(attrsJson);
        Record record = actionConverter.makeExecutedActionSpec(succeededActionSpec, "SUCCESS!", topologyGraph);
        assertThat(record.get(CompletedAction.RECOMMENDATION_TIME),
                is(new Timestamp(actionSpec.getRecommendationTime())));
        assertThat(record.get(CompletedAction.TYPE), is(ActionType.MOVE));
        assertThat(record.get(CompletedAction.CATEGORY), is(ActionCategory.COMPLIANCE));
        assertThat(record.get(CompletedAction.SEVERITY), is(Severity.CRITICAL));
        assertThat(record.get(CompletedAction.TARGET_ENTITY), is(123L));
        Set<Long> s = new HashSet<>();
        for (long l : record.get(CompletedAction.INVOLVED_ENTITIES)) {
            s.add(l);
        }
        assertThat(s, containsInAnyOrder(123L, 234L, 345L));
        assertThat(record.get(CompletedAction.DESCRIPTION), is(description));
        assertThat(record.get(CompletedAction.SAVINGS), is(savings));
        assertThat(record.get(CompletedAction.ACTION_OID), is(actionOid));

        assertThat(record.get(CompletedAction.ACCEPTANCE_TIME),
                is(new Timestamp(succeededActionSpec.getDecision().getDecisionTime())));
        assertThat(record.get(CompletedAction.COMPLETION_TIME),
                is(new Timestamp(succeededActionSpec.getExecutionStep().getCompletionTime())));
        assertThat(record.get(CompletedAction.FINAL_STATE), is(TerminalState.SUCCEEDED));
        assertThat(record.get(CompletedAction.FINAL_MESSAGE), is("SUCCESS!"));
        assertThat(record.get(CompletedAction.ATTRS).toString(), is(attrsJson));
    }

    /**
     * Test converting an {@link ActionSpec} for a failed action to a database record.
     */
    @Test
    public void testExecutedFailedActionRecord() {
        Record record = actionConverter.makeExecutedActionSpec(failedActionSpec, "FAILURE!", topologyGraph);
        assertThat(record.get(CompletedAction.FINAL_STATE), is(TerminalState.FAILED));
        assertThat(record.get(CompletedAction.FINAL_MESSAGE), is("FAILURE!"));
    }

    /**
     * Test converting action spec to an exported action.
     */
    @Test
    public void testMakeExportedAction() {
        com.vmturbo.extractor.schema.json.export.Action action =
                actionConverter.makeExportedAction(actionSpec, topologyGraph, new HashMap<>(),
                        Optional.of(relatedEntitiesExtractor));
        // common fields
        Assert.assertThat(action.getOid(), is(actionSpec.getRecommendation().getId()));
        Assert.assertThat(action.getState(), is(actionSpec.getActionState().name()));
        Assert.assertThat(action.getCategory(), is(actionSpec.getCategory().name()));
        Assert.assertThat(action.getMode(), is(actionSpec.getActionMode().name()));
        Assert.assertThat(action.getSeverity(), is(actionSpec.getSeverity().name()));
        Assert.assertThat(action.getDescription(), is(actionSpec.getDescription()));
        Assert.assertThat(action.getType(), is(
                ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation()).name()));
        // target
        Assert.assertThat(action.getTarget().getOid(), is(123L));
        Assert.assertThat(action.getTarget().getName(), is(String.valueOf(123)));
        Assert.assertThat(action.getTarget().getType(), is(VIRTUAL_MACHINE.getLiteral()));
        // related
        Assert.assertThat(action.getRelated().size(), is(2));
        Assert.assertThat(action.getRelated().get(PHYSICAL_MACHINE.getLiteral()).get(0).getOid(), is(234L));
        Assert.assertThat(action.getRelated().get(STORAGE.getLiteral()).get(0).getOid(), is(456L));
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
