package com.vmturbo.extractor.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

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
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.ActionModel.PendingAction;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

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

    private ActionConverter actionConverter = new ActionConverter();

    /**
     * Test converting an {@link ActionSpec} for a pending actionto a database record.
     */
    @Test
    public void testPendingActionRecord() {
        Record record = actionConverter.makePendingActionRecord(actionSpec);
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
    }

    /**
     * Test converting an {@link ActionSpec} for a succeeded action to a database record.
     */
    @Test
    public void testExecutedSucceededActionRecord() {
        Record record = actionConverter.makeExecutedActionSpec(succeededActionSpec, "SUCCESS!");
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
    }

    /**
     * Test converting an {@link ActionSpec} for a failed action to a database record.
     */
    @Test
    public void testExecutedFailedActionRecord() {
        Record record = actionConverter.makeExecutedActionSpec(failedActionSpec, "FAILURE!");
        assertThat(record.get(CompletedAction.FINAL_STATE), is(TerminalState.FAILED));
        assertThat(record.get(CompletedAction.FINAL_MESSAGE), is("FAILURE!"));
    }
}
