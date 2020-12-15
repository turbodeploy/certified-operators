package com.vmturbo.action.orchestrator.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

public class ActionStatTest {

    private static final ActionDTO.Action SAVINGS_ACTION = ActionDTO.Action.newBuilder()
        .setId(1)
        .setInfo(ActionInfo.getDefaultInstance())
        .setDeprecatedImportance(1)
        .setExplanation(Explanation.getDefaultInstance())
        .setSavingsPerHour(CurrencyAmount.newBuilder()
            .setAmount(1.0))
        .build();

    private static final ActionDTO.Action INVESTMENT_ACTION = ActionDTO.Action.newBuilder()
        .setId(1)
        .setInfo(ActionInfo.getDefaultInstance())
        .setDeprecatedImportance(1)
        .setExplanation(Explanation.getDefaultInstance())
        .setSavingsPerHour(CurrencyAmount.newBuilder()
                .setAmount(-3.0))
        .build();

    @Test
    public void testActionRecord() {
        final ActionStat actionStat = new ActionStat();
        actionStat.recordAction(SAVINGS_ACTION, Collections.singleton(ActionEntity.newBuilder()
                .setId(7)
                .setType(10)
                .build()),
            true);
        actionStat.recordAction(INVESTMENT_ACTION, Collections.singleton(ActionEntity.newBuilder()
                .setId(8)
                .setType(10)
                .build()),
            false);

        final ActionStatsLatestRecord record = new ActionStatsLatestRecord();
        actionStat.addToRecord(record);

        assertThat(record.getTotalSavings().doubleValue(), is(1.0));
        assertThat(record.getTotalInvestment().doubleValue(), is(3.0));
        assertThat(record.getTotalActionCount(), is(2));
        assertThat(record.getTotalEntityCount(), is(2));
        assertThat(record.getNewActionCount(), is(1));
    }

    @Test
    public void testActionRecordRemoveEntityDuplicates() {
        final ActionStat actionStat = new ActionStat();
        // Note - we reuse the same action object to mean two different individual actions affecting
        // the same entity.
        actionStat.recordAction(SAVINGS_ACTION, Collections.singleton(ActionEntity.newBuilder()
                .setId(7)
                .setType(10)
                .build()),
            true);
        // Same ID, different action.
        actionStat.recordAction(SAVINGS_ACTION, Collections.singleton(ActionEntity.newBuilder()
                .setId(7)
                .setType(10)
                .build()),
            false);

        final ActionStatsLatestRecord record = new ActionStatsLatestRecord();
        actionStat.addToRecord(record);

        // Entity count should just be one, because it was the same entity.
        assertThat(record.getTotalEntityCount(), is(1));

        // Both actions reflected in action count.
        assertThat(record.getTotalActionCount(), is(2));
        // Only one of the two actions are "new"
        assertThat(record.getNewActionCount(), is(1));
        // Savings from both actions should be considered.
        assertThat(record.getTotalSavings().doubleValue(), is(2.0));
    }
}
