package com.vmturbo.action.orchestrator.stats.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.ImmutableStatsActionView;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.BusinessAccountActionAggregator.BusinessAccountActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Unit tests for {@link BusinessAccountActionAggregator}.
 */
public class BusinessAccountActionAggregatorTest {

    private static final ActionGroupKey ACTION_GROUP_KEY = mock(ActionGroupKey.class);

    private static final ActionGroup ACTION_GROUP = ImmutableActionGroup.builder()
        .id(888)
        .key(ACTION_GROUP_KEY)
        .build();

    private static final ActionDTO.Action SAVINGS_ACTION = ActionDTO.Action.newBuilder()
        .setId(1)
        .setInfo(ActionInfo.getDefaultInstance())
        .setDeprecatedImportance(1)
        .setExplanation(Explanation.getDefaultInstance())
        .setSavingsPerHour(CurrencyAmount.newBuilder()
            .setAmount(1.0))
        .build();

    private Clock clock = new MutableFixedClock(1_000_000);

    private BusinessAccountActionAggregatorFactory factory;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        factory = new BusinessAccountActionAggregatorFactory();
    }

    /**
     * Test aggregation of actions owned by business accounts.
     */
    @Test
    public void testAggregation() {
        // Account 1 owns entity 10, account 2 owns entity 11
        // ARRANGE
        final long acct1Id = 1L;
        final long acct2Id = 2L;
        final long vm1Id = 10L;
        final long vm2Id = 11L;

        final MgmtUnitSubgroup b1Subgroup = makeBu(acct1Id, 123);
        final MgmtUnitSubgroup b2Subgroup = makeBu(acct2Id, 234);


        // Actions involving entity 10 and entity 11.
        final ActionEntity e10 = makeVm(vm1Id);
        final StatsActionView e10Snapshot = fakeSnapshot(acct1Id, e10);
        final ActionEntity e11 = makeVm(vm2Id);
        final StatsActionView e11Snapshot = fakeSnapshot(acct2Id, e11);

        // ACT
        final BusinessAccountActionAggregator aggregator = factory.newAggregator(LocalDateTime.now(clock));
        aggregator.start();

        aggregator.processAction(e10Snapshot, new PreviousBroadcastActions());
        aggregator.processAction(e11Snapshot, new PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> recordsByMu =
            aggregator.createRecords(ImmutableMap.of(b1Subgroup.key(), b1Subgroup,
                b2Subgroup.key(), b2Subgroup), ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        // ASSERT
        assertThat(recordsByMu.keySet(), containsInAnyOrder(b1Subgroup.id(), b2Subgroup.id()));

        final ActionStatsLatestRecord b1Record = recordsByMu.get(b1Subgroup.id());
        assertThat(b1Record.getTotalActionCount(), is(1));
        assertThat(b1Record.getTotalEntityCount(), is(1));
        assertThat(b1Record.getMgmtUnitSubgroupId(), is(b1Subgroup.id()));
        assertThat(b1Record.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(b1Record.getTotalSavings().doubleValue(), closeTo(SAVINGS_ACTION.getSavingsPerHour().getAmount(), 0.001));
        assertThat(b1Record.getTotalInvestment().doubleValue(), is(0.0));
        assertThat(b1Record.getActionSnapshotTime(), is(LocalDateTime.now(clock)));

        final ActionStatsLatestRecord b2Record = recordsByMu.get(b2Subgroup.id());
        assertThat(b2Record.getTotalActionCount(), is(1));
        assertThat(b2Record.getTotalEntityCount(), is(1));
        assertThat(b2Record.getMgmtUnitSubgroupId(), is(b2Subgroup.id()));
        assertThat(b2Record.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(b2Record.getTotalSavings().doubleValue(), closeTo(SAVINGS_ACTION.getSavingsPerHour().getAmount(), 0.001));
        assertThat(b2Record.getTotalInvestment().doubleValue(), is(0.0));
        assertThat(b2Record.getActionSnapshotTime(), is(LocalDateTime.now(clock)));
    }

    private StatsActionView fakeSnapshot(final long accountId, @Nonnull final ActionEntity... involvedEntities) {
        final ImmutableStatsActionView.Builder actionSnapshotBuilder = ImmutableStatsActionView.builder()
            .actionGroupKey(ACTION_GROUP_KEY)
            .recommendation(SAVINGS_ACTION)
            .businessAccountId(accountId);
        actionSnapshotBuilder.addInvolvedEntities(involvedEntities);
        return actionSnapshotBuilder.build();
    }

    private ActionEntity makeVm(final long vmId) {
        return ActionEntity.newBuilder()
            .setType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setId(vmId)
            .build();
    }

    private MgmtUnitSubgroup makeBu(final long acctId, final int muId) {
        return ImmutableMgmtUnitSubgroup.builder()
            .id(muId)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(acctId)
                .mgmtUnitType(ManagementUnitType.BUSINESS_ACCOUNT)
                .environmentType(EnvironmentType.CLOUD)
                .build())
            .build();
    }

}
