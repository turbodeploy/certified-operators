package com.vmturbo.action.orchestrator.stats.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.ImmutableStatsActionView;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.sdk.common.CloudCostDTO;

/**
 * Tests the functionality of {@link ResourceGroupActionAggregator}.
 */
public class ResourceGroupActionAggregatorTest {
    private static final ActionGroup.ActionGroupKey ACTION_GROUP_KEY = mock(ActionGroup.ActionGroupKey.class);

    private static final ActionGroup ACTION_GROUP = ImmutableActionGroup.builder()
        .id(888)
        .key(ACTION_GROUP_KEY)
        .build();

    private static final ActionDTO.Action SAVINGS_ACTION = ActionDTO.Action.newBuilder()
        .setId(1)
        .setInfo(ActionDTO.ActionInfo.getDefaultInstance())
        .setDeprecatedImportance(1)
        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
        .setSavingsPerHour(CloudCostDTO.CurrencyAmount.newBuilder()
            .setAmount(1.0))
        .build();

    private Clock clock = new MutableFixedClock(1_000_000);

    private ResourceGroupActionAggregator.ResourceGroupActionAggregatorFactory factory;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        factory = new ResourceGroupActionAggregator.ResourceGroupActionAggregatorFactory();
    }

    /**
     * Test aggregation of actions owned by resource groups.
     */
    @Test
    public void testAggregation() {
        // Account 1 owns entity 10, account 2 owns entity 11
        // ARRANGE
        final long rg1Id = 1L;
        final long rg2Id = 2L;
        final long vm1Id = 10L;
        final long vm2Id = 11L;
        final GroupAndMembers rg1GroupAndMembers = mock(GroupAndMembers.class);
        when(rg1GroupAndMembers.group()).thenReturn(GroupDTO.Grouping.newBuilder().setId(rg1Id).build());
        when(rg1GroupAndMembers.members()).thenReturn(Collections.singletonList(vm1Id));
        final GroupAndMembers rg2GroupAndMembers = mock(GroupAndMembers.class);
        when(rg2GroupAndMembers.group()).thenReturn(GroupDTO.Grouping.newBuilder().setId(rg2Id).build());
        when(rg2GroupAndMembers.members()).thenReturn(Collections.singletonList(vm2Id));
        final MgmtUnitSubgroup rg1Subgroup = makeRg(rg1Id, 123);
        final MgmtUnitSubgroup rg2Subgroup = makeRg(rg2Id, 234);


        // Actions involving entity 10 and entity 11.
        final ActionDTO.ActionEntity e10 = makeVm(vm1Id);
        final StatsActionViewFactory.StatsActionView e10Snapshot = fakeSnapshot(rg1Id, e10);
        final ActionDTO.ActionEntity e11 = makeVm(vm2Id);
        final StatsActionViewFactory.StatsActionView e11Snapshot = fakeSnapshot(rg2Id, e11);

        // ACT
        final ResourceGroupActionAggregator aggregator = factory.newAggregator(LocalDateTime.now(clock));
        aggregator.start();

        aggregator.processAction(e10Snapshot, new LiveActionsStatistician.PreviousBroadcastActions());
        aggregator.processAction(e11Snapshot, new LiveActionsStatistician.PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> recordsByMu =
            aggregator.createRecords(ImmutableMap.of(rg1Subgroup.key(), rg1Subgroup,
                rg2Subgroup.key(), rg2Subgroup), ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
                .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        // ASSERT
        assertThat(recordsByMu.keySet(), containsInAnyOrder(rg1Subgroup.id(), rg2Subgroup.id()));

        final ActionStatsLatestRecord b1Record = recordsByMu.get(rg1Subgroup.id());
        assertThat(b1Record.getTotalActionCount(), is(1));
        assertThat(b1Record.getTotalEntityCount(), is(1));
        assertThat(b1Record.getMgmtUnitSubgroupId(), is(rg1Subgroup.id()));
        assertThat(b1Record.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(b1Record.getTotalSavings().doubleValue(), closeTo(SAVINGS_ACTION.getSavingsPerHour().getAmount(), 0.001));
        assertThat(b1Record.getTotalInvestment().doubleValue(), is(0.0));
        assertThat(b1Record.getActionSnapshotTime(), is(LocalDateTime.now(clock)));

        final ActionStatsLatestRecord b2Record = recordsByMu.get(rg2Subgroup.id());
        assertThat(b2Record.getTotalActionCount(), is(1));
        assertThat(b2Record.getTotalEntityCount(), is(1));
        assertThat(b2Record.getMgmtUnitSubgroupId(), is(rg2Subgroup.id()));
        assertThat(b2Record.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(b2Record.getTotalSavings().doubleValue(), closeTo(SAVINGS_ACTION.getSavingsPerHour().getAmount(), 0.001));
        assertThat(b2Record.getTotalInvestment().doubleValue(), is(0.0));
        assertThat(b2Record.getActionSnapshotTime(), is(LocalDateTime.now(clock)));
    }

    private StatsActionViewFactory.StatsActionView fakeSnapshot(final long resourceGroupId, @Nonnull final ActionDTO.ActionEntity... involvedEntities) {
        final ImmutableStatsActionView.Builder actionSnapshotBuilder = ImmutableStatsActionView.builder()
            .actionGroupKey(ACTION_GROUP_KEY)
            .recommendation(SAVINGS_ACTION)
            .resourceGroupId(resourceGroupId);
        actionSnapshotBuilder.addInvolvedEntities(involvedEntities);
        return actionSnapshotBuilder.build();
    }

    private ActionDTO.ActionEntity makeVm(final long vmId) {
        return ActionDTO.ActionEntity.newBuilder()
            .setType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
            .setId(vmId)
            .build();
    }

    private MgmtUnitSubgroup makeRg(final long rgId, final int muId) {
        return ImmutableMgmtUnitSubgroup.builder()
            .id(muId)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(rgId)
                .mgmtUnitType(ManagementUnitType.RESOURCE_GROUP)
                .environmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build())
            .build();
    }
}