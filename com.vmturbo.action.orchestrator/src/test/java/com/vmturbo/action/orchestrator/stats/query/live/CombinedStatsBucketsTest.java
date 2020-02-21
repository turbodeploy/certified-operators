package com.vmturbo.action.orchestrator.stats.query.live;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.getCommodityDisplayName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.query.live.CombinedStatsBuckets.CombinedStatsBucketsFactory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat.StatGroup;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

public class CombinedStatsBucketsTest {

    private static final int VM = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int PM = EntityType.PHYSICAL_MACHINE_VALUE;

    private static final ActionEntity CLOUD_VM = ActionEntity.newBuilder()
        .setId(7)
        .setType(VM)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    private static final ActionEntity ON_PREM_VM = ActionEntity.newBuilder()
        .setId(8)
        .setType(VM)
        .setEnvironmentType(EnvironmentType.ON_PREM)
        .build();

    private static final ActionEntity ON_PREM_PM = ActionEntity.newBuilder()
        .setId(9)
        .setType(PM)
        .setEnvironmentType(EnvironmentType.ON_PREM)
        .build();

    private CombinedStatsBucketsFactory factory = new CombinedStatsBucketsFactory();

    @Test
    public void testNoGroupBy() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        // No group by
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.getDefaultInstance());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);

        final SingleActionInfo savingsAction = actionInfo(
            bldr -> bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(1)),
            view -> {},
            Sets.newHashSet(CLOUD_VM, ON_PREM_VM));
        final SingleActionInfo investmentAction = actionInfo(
            bldr -> bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(-1)),
            view -> {},
            // One of the same entities involved.
            Sets.newHashSet(ON_PREM_VM));

        buckets.addActionInfo(savingsAction);
        buckets.addActionInfo(investmentAction);
        final List<CurrentActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats, contains(CurrentActionStat.newBuilder()
            .setStatGroup(StatGroup.getDefaultInstance())
            // Two total entities.
            .setEntityCount(2)
            // Two total actions.
            .setActionCount(2)
            .setInvestments(1.0)
            .setSavings(1.0)
            .build()));
    }

    @Test
    public void testGroupBy() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.ACTION_STATE)
            .addGroupBy(GroupBy.ACTION_CATEGORY)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo inProgressCompliance = actionInfo(
            bldr -> {},
            view -> {
                when(view.getState()).thenReturn(ActionState.IN_PROGRESS);
                when(view.getActionCategory()).thenReturn(ActionCategory.COMPLIANCE);
            },
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo inProgressPerformance = actionInfo(
            bldr -> {},
            view -> {
                when(view.getState()).thenReturn(ActionState.IN_PROGRESS);
                when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            },
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo readyCompliance = actionInfo(
            bldr -> {},
            view -> {
                when(view.getState()).thenReturn(ActionState.READY);
                when(view.getActionCategory()).thenReturn(ActionCategory.COMPLIANCE);
            },
            // One of the same entities involved.
            Sets.newHashSet(ON_PREM_VM));

        buckets.addActionInfo(inProgressCompliance);
        buckets.addActionInfo(inProgressPerformance);
        buckets.addActionInfo(readyCompliance);
        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.COMPLIANCE)
                    .setActionState(ActionState.IN_PROGRESS))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                    .setActionState(ActionState.IN_PROGRESS))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.COMPLIANCE)
                    .setActionState(ActionState.READY))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build()));
    }

    /**
     * Test that Grouping by CostType appropriately buckets the investment/savings/no savings.
     */
    @Test
    public void testGroupByCostType() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.ACTION_STATE)
            .addGroupBy(GroupBy.ACTION_CATEGORY)
            .addGroupBy(GroupBy.COST_TYPE)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo savingsAction = actionInfo(
            bldr -> {
                bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(500.00).build());
            },
            view -> {
                when(view.getState()).thenReturn(ActionState.READY);
                when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            },
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo investmentAction1 = actionInfo(
            bldr -> {
                bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(-200.00).build());
            },
            view -> {
                when(view.getState()).thenReturn(ActionState.READY);
                when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            },
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo investmentAction2 = actionInfo(
            bldr -> {
                bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(-100.00).build());
            },
            view -> {
                when(view.getState()).thenReturn(ActionState.READY);
                when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            },
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo noSavingsOrInvestment = actionInfo(
            bldr -> {
                bldr.setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(0.00).build());
            },
            view -> {
                when(view.getState()).thenReturn(ActionState.READY);
                when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            },
            Sets.newHashSet(ON_PREM_VM));

        buckets.addActionInfo(savingsAction);
        buckets.addActionInfo(investmentAction1);
        buckets.addActionInfo(investmentAction2);
        buckets.addActionInfo(noSavingsOrInvestment);
        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                    .setActionState(ActionState.READY)
                    .setCostType(ActionCostType.INVESTMENT))
                .setActionCount(2)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(300.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                    .setActionState(ActionState.READY)
                    .setCostType(ActionCostType.SAVINGS))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(500.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                    .setActionState(ActionState.READY)
                    .setCostType(ActionCostType.ACTION_COST_TYPE_NONE))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build()));
    }

    @Test
    public void testGroupByReasonCommodity() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.REASON_COMMODITY)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo reason1 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder()
                                .setTarget(CLOUD_VM)
                                .addTriggeringCommodities(CommodityType.newBuilder().setType(1))
                                .addTriggeringCommodities(CommodityType.newBuilder().setType(3))));
            },
            view -> {},
            Sets.newHashSet(CLOUD_VM));
        final SingleActionInfo reason2 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_VM)
                        .addTriggeringCommodities(CommodityType.newBuilder()
                            .setType(2))));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_VM));
        buckets.addActionInfo(reason1);
        buckets.addActionInfo(reason2);
        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(getTestBuild(1), getTestBuild(2), getTestBuild(3)));
    }

    private CurrentActionStat getTestBuild(int reasonCommodityType) {
        return CurrentActionStat.newBuilder()
                .setStatGroup(
                        StatGroup.newBuilder().setReasonCommodityBaseType(reasonCommodityType))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build();
    }

    @Test
    public void testGroupByTargetEntityType() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.TARGET_ENTITY_TYPE)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo vmTarget = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(CLOUD_VM)
                        .addTriggeringCommodities(CommodityType.newBuilder()
                            .setType(1))));
            },
            view -> {},
            Sets.newHashSet(CLOUD_VM));
        final SingleActionInfo pmTarget = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_PM)));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_PM));

        buckets.addActionInfo(vmTarget);
        buckets.addActionInfo(pmTarget);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetEntityType(VM))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetEntityType(PM))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build()));
    }

    @Test
    public void testGroupByTargetEntityId() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.TARGET_ENTITY_ID)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo vmTarget1 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_VM)));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo vmTarget2 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_VM)));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo pmTarget = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_PM)));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_PM));

        buckets.addActionInfo(pmTarget);
        // two actions for same vm
        buckets.addActionInfo(vmTarget1);
        buckets.addActionInfo(vmTarget2);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetEntityId(ON_PREM_VM.getId()))
                .setActionCount(2)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetEntityId(ON_PREM_PM.getId()))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build()));
    }

    @Test
    public void testGroupByActionExplanation() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.ACTION_EXPLANATION)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo pmTarget1 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_PM)))
                    .setExplanation(Explanation.newBuilder()
                        .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(
                                ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodityInfo(
                                            ActionOrchestratorTestUtils.createReasonCommodity(
                                                CommodityDTO.CommodityType.CPU_VALUE, null)))
                        .build()));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_PM));
        final SingleActionInfo pmTarget2 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_PM)))
                    .setExplanation(Explanation.newBuilder()
                        .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(
                                ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodityInfo(
                                            ActionOrchestratorTestUtils.createReasonCommodity(
                                                CommodityDTO.CommodityType.MEM_VALUE, null)))
                        .build()));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_PM));
        final SingleActionInfo pmTarget3 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(ON_PREM_PM)))
                    .setExplanation(Explanation.newBuilder()
                        .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(
                                ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodityInfo(
                                        ActionOrchestratorTestUtils.createReasonCommodity(
                                                CommodityDTO.CommodityType.MEM_VALUE, null)))
                        .build()));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_PM));

        buckets.addActionInfo(pmTarget1);
        // two actions of same explanation
        buckets.addActionInfo(pmTarget2);
        buckets.addActionInfo(pmTarget3);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionExplanation(getCommodityDisplayName(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE).build()) + " congestion"))
                .setActionCount(2)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionExplanation(getCommodityDisplayName(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build()) + " congestion"))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build()));
    }

    @Test
    public void testEntityPredicate() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.getDefaultInstance());
        // Only match the cloud VM, not the on-prem VM.
        when(queryInfo.entityPredicate()).thenReturn(entity -> entity.equals(CLOUD_VM));
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);

        final SingleActionInfo savingsAction = actionInfo(
            bldr -> {},
            view -> {},
            Sets.newHashSet(CLOUD_VM, ON_PREM_VM));

        buckets.addActionInfo(savingsAction);
        final List<CurrentActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats, contains(CurrentActionStat.newBuilder()
            .setStatGroup(StatGroup.getDefaultInstance())
            // Just one entity - the ON_PREM vm was involved, but didn't match the predicate.
            .setEntityCount(1)
            .setActionCount(1)
            .setInvestments(0.0)
            .setSavings(0.0)
            .build()));
    }

    @Nonnull
    private SingleActionInfo actionInfo(@Nonnull final Consumer<Builder> actionCustomizer,
                                        @Nonnull final Consumer<ActionView> actionViewConsumer,
                                        @Nonnull final Set<ActionEntity> involvedEntities) {
        final ActionDTO.Action.Builder builder = Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(232)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
            .setExplanation(Explanation.getDefaultInstance())
            .setDeprecatedImportance(1);
        actionCustomizer.accept(builder);

        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(builder.build());
        actionViewConsumer.accept(actionView);

        final SingleActionInfo singleActionInfo = ImmutableSingleActionInfo.builder()
            .action(actionView)
            .addAllInvolvedEntities(involvedEntities)
            .build();
        return singleActionInfo;
    }
}
