package com.vmturbo.extractor.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

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
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.ActionMetric;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Unit tests for {@link ActionConverter}.
 */
public class ActionConverterTest {

    private final String description = "My action description.";
    private final double savings = 8;
    private final long specOid = 870;
    private final ActionSpec actionSpec = ActionSpec.newBuilder()
            .setRecommendationId(specOid)
            .setRecommendation(Action.newBuilder()
                    .setDeprecatedImportance(123)
                    .setId(7L)
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

    private ActionConverter actionConverter = new ActionConverter();

    /**
     * Test converting an {@link ActionSpec} to a database record.
     */
    @Test
    public void testActionSpecRecord() {
        Record record = actionConverter.makeActionSpecRecord(actionSpec);
        assertThat(record.get(ActionModel.ActionSpec.TYPE), is(ActionType.MOVE));
        assertThat(record.get(ActionModel.ActionSpec.CATEGORY), is(ActionCategory.COMPLIANCE));
        assertThat(record.get(ActionModel.ActionSpec.SEVERITY), is(Severity.CRITICAL));
        assertThat(record.get(ActionModel.ActionSpec.TARGET_ENTITY), is(123L));
        Set<Long> s = new HashSet<>();
        for (long l : record.get(ActionModel.ActionSpec.INVOLVED_ENTITIES)) {
            s.add(l);
        }
        assertThat(s, containsInAnyOrder(123L, 234L, 345L));
        assertThat(record.get(ActionModel.ActionSpec.DESCRIPTION), is(description));
        assertThat(record.get(ActionModel.ActionSpec.SAVINGS), is(savings));
        assertThat(record.get(ActionModel.ActionSpec.SPEC_OID), is(specOid));
    }

    /**
     * Test converting an {@link ActionSpec} to a metric record.
     */
    @Test
    public void testActionRecord() {
        Record actionSpecRecord = new Record(ActionMetric.TABLE);
        actionSpecRecord.set(ActionModel.ActionSpec.SPEC_OID, specOid);

        Record actionRecord = actionConverter.makeActionRecord(actionSpec, actionSpecRecord);
        assertThat(actionRecord.get(ActionMetric.STATE), is(ActionState.IN_PROGRESS));
        assertThat(actionRecord.get(ActionMetric.ACTION_SPEC_OID), is(specOid));
        assertThat(actionRecord.get(ActionMetric.ACTION_OID),
            is(actionSpec.getRecommendation().getId()));
        assertThat(actionRecord.get(ActionMetric.USER), is("me"));
    }

}
