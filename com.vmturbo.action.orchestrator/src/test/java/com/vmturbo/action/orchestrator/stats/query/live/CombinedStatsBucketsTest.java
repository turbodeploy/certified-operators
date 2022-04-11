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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionResourceImpact;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
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
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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

    /**
     * Test Group-by Action Severity properly buckets the numActions for each action severity category.
     */
    @Test
    public void testGroupByActionSeverity() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
                .addGroupBy(GroupBy.SEVERITY)
                .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo criticalActionPerfmance = actionInfo(
                bldr -> { },
                view -> {
                    when(view.getState()).thenReturn(ActionState.READY);
                    when(view.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
                    when(view.getActionSeverity()).thenReturn(Severity.CRITICAL);
                },
                Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo criticalActionCompliance = actionInfo(
                bldr -> { },
                view -> {
                    when(view.getState()).thenReturn(ActionState.READY);
                    when(view.getActionCategory()).thenReturn(ActionCategory.COMPLIANCE);
                    when(view.getActionSeverity()).thenReturn(Severity.CRITICAL);
                },
                Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo majorActionPrevention = actionInfo(
                bldr -> { },
                view -> {
                    when(view.getState()).thenReturn(ActionState.READY);
                    when(view.getActionCategory()).thenReturn(ActionCategory.PREVENTION);
                    when(view.getActionSeverity()).thenReturn(Severity.MAJOR);
                },
                Sets.newHashSet(ON_PREM_VM));
        final SingleActionInfo minorActionEfficiency = actionInfo(
                bldr -> { },
                view -> {
                    when(view.getState()).thenReturn(ActionState.READY);
                    when(view.getActionCategory()).thenReturn(ActionCategory.EFFICIENCY_IMPROVEMENT);
                    when(view.getActionSeverity()).thenReturn(Severity.MINOR);
                },
                Sets.newHashSet(ON_PREM_VM));

        buckets.addActionInfo(criticalActionPerfmance);
        buckets.addActionInfo(criticalActionCompliance);
        buckets.addActionInfo(majorActionPrevention);
        buckets.addActionInfo(minorActionEfficiency);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                        .setSeverity(Severity.CRITICAL))
                .setActionCount(2)
                    .setEntityCount(1)
                    .setSavings(0.0)
                    .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder().setSeverity(Severity.MAJOR))
                    .setActionCount(1)
                    .setEntityCount(1)
                    .setSavings(0.0)
                    .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                        .setSeverity(Severity.MINOR))
                .setActionCount(1)
                    .setEntityCount(1)
                    .setSavings(0.0)
                    .setInvestments(0.0)
                .build()
        ));
    }

    @Test
    public void testGroupByReasonCommodity() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
            .addGroupBy(GroupBy.REASON_COMMODITY)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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
            .addGroupBy(GroupBy.ACTION_RELATED_RISK)
            .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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

        final SingleActionInfo pmTarget4 = actionInfo(
            bldr -> {
                bldr.setInfo(ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(ON_PREM_VM)))
                    .setExplanation(Explanation.newBuilder()
                        .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionByDemandExplanation(
                                ProvisionByDemandExplanation.newBuilder()
                                    .setBuyerId(1)
                                    .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                                        .setCommodityBaseType(CommodityDTO.CommodityType.MEM_VALUE)
                                        .setRequestedAmount(10).setMaxAmountAvailable(20))
                                    .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                                        .setCommodityBaseType(CommodityDTO.CommodityType.CPU_VALUE)
                                        .setRequestedAmount(10).setMaxAmountAvailable(20)))
                            .build()));
            },
            view -> {},
            Sets.newHashSet(ON_PREM_VM));

        buckets.addActionInfo(pmTarget1);
        buckets.addActionInfo(pmTarget4);
        // two actions of same explanation
        buckets.addActionInfo(pmTarget2);
        buckets.addActionInfo(pmTarget3);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionRelatedRisk(getCommodityDisplayName(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE).build()) + " Congestion"))
                .setActionCount(3)
                .setEntityCount(2)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build(),
            CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionRelatedRisk(getCommodityDisplayName(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build()) + " Congestion"))
                .setActionCount(2)
                .setEntityCount(2)
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
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
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

    @Test
    public void testResourceImpact() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
                .addGroupBy(GroupBy.TARGET_ENTITY_TYPE)
                .addGroupBy(GroupBy.ACTION_TYPE)
                .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);

        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);

        final ActionEntity vmEntity1 = ActionEntity.newBuilder()
                .setId(111)
                .setType(VM)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final SingleActionInfo vmTarget = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setResize(Resize.newBuilder()
                                    .setTarget(vmEntity1)
                                    .setNewCapacity(20.0f)
                                    .setOldCapacity(10.0f)
                                    .setCommodityType(CommodityType.newBuilder()
                                            .setType(53)))); // VMem
                },
                view -> {},
                Sets.newHashSet(vmEntity1));

        final ActionEntity vmEntity2 = ActionEntity.newBuilder()
                .setId(112)
                .setType(VM)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();
        final SingleActionInfo vmTarget2 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setResize(Resize.newBuilder()
                                    .setTarget(vmEntity2)
                                    .setNewCapacity(10.0f)
                                    .setOldCapacity(22.0f)
                                    .setCommodityType(CommodityType.newBuilder()
                                            .setType(53)))); // VMem
                },
                view -> {},
                Sets.newHashSet(vmEntity2));

        final ActionEntity vmEntity3 = ActionEntity.newBuilder()
                .setId(113)
                .setType(VM)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        final SingleActionInfo vmTarget3 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setResize(Resize.newBuilder()
                                    .setTarget(vmEntity3)
                                    .setNewCapacity(5.0f)
                                    .setOldCapacity(30.0f)
                                    .setCommodityType(CommodityType.newBuilder()
                                            .setType(26)))); // VCPU
                },
                view -> {},
                Sets.newHashSet(vmEntity3));

        //This is a LIMIT remove action, the change should not be counted into resourceImpacts.
        final SingleActionInfo vmTarget5 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setResize(Resize.newBuilder()
                                    .setTarget(vmEntity3)
                                    .setNewCapacity(0)
                                    .setOldCapacity(5200)
                                    .setCommodityAttribute(CommodityAttribute.LIMIT)
                                    .setCommodityType(CommodityType.newBuilder()
                                            .setType(26)))); // VCPU
                },
                view -> {},
                Sets.newHashSet(vmEntity3));

        final ActionEntity vmEntity4 = ActionEntity.newBuilder()
                .setId(114)
                .setType(VM)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        final SingleActionInfo vmTarget4 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setDeactivate(Deactivate.newBuilder()
                                    .setTarget(vmEntity4)
                                    .addTriggeringCommodities(CommodityType.newBuilder()
                                    .setType(26)))); // VCPU
                },
                view -> {},
                Sets.newHashSet(vmEntity4));

        final SingleActionInfo pmTarget = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setActivate(Activate.newBuilder()
                                    .setTarget(ON_PREM_PM)));
                },
                view -> {},
                Sets.newHashSet(ON_PREM_PM));

        buckets.addActionInfo(vmTarget);
        buckets.addActionInfo(vmTarget2);
        buckets.addActionInfo(vmTarget3);
        buckets.addActionInfo(vmTarget5);
        buckets.addActionInfo(vmTarget4);
        buckets.addActionInfo(pmTarget);

        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats, containsInAnyOrder(
                CurrentActionStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setTargetEntityType(VM)
                                .setActionType(ActionType.RESIZE))
                        .setActionCount(4)
                        .setEntityCount(3)
                        .setSavings(0.0)
                        .setInvestments(0.0)
                        .addActionResourceImpacts(ActionResourceImpact.newBuilder()
                                .setTargetEntityType(VM)
                                .setActionType(ActionType.RESIZE)
                                .setReasonCommodityBaseType(53)
                                .setAmount(-2.0f) // total VMem Impact
                                .build())
                        .addActionResourceImpacts(ActionResourceImpact.newBuilder()
                                .setTargetEntityType(VM)
                                .setActionType(ActionType.RESIZE)
                                .setReasonCommodityBaseType(26)
                                .setAmount(-25.0f) // total VCPU Impact
                                .build())
                        .build(),
                CurrentActionStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setTargetEntityType(VM)
                                .setActionType(ActionType.DEACTIVATE))
                        .setActionCount(1)
                        .setEntityCount(1)
                        .setSavings(0.0)
                        .setInvestments(0.0)
                        .build(),
                CurrentActionStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setTargetEntityType(PM)
                                .setActionType(ActionType.ACTIVATE))
                        .setActionCount(1)
                        .setEntityCount(1)
                        .setSavings(0.0)
                        .setInvestments(0.0)
                        .build()));
    }

    @Test
    public void testGroupByEnvironmentType() {
        final QueryInfo queryInfo = mock(QueryInfo.class);
        when(queryInfo.query()).thenReturn(CurrentActionStatsQuery.newBuilder()
                .addGroupBy(GroupBy.ENVIRONMENT_TYPE)
                .build());
        when(queryInfo.entityPredicate()).thenReturn(entity -> true);
        when(queryInfo.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
        final CombinedStatsBuckets buckets = factory.bucketsForQuery(queryInfo);
        final SingleActionInfo reason1 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setActivate(Activate.newBuilder().setTarget(CLOUD_VM)));
                },
                view -> {},
                Sets.newHashSet(CLOUD_VM));
        final SingleActionInfo reason2 = actionInfo(
                bldr -> {
                    bldr.setInfo(ActionInfo.newBuilder()
                            .setActivate(Activate.newBuilder()
                                    .setTarget(ON_PREM_VM)));
                },
                view -> {},
                Sets.newHashSet(ON_PREM_VM));
        buckets.addActionInfo(reason1);
        buckets.addActionInfo(reason2);
        final List<CurrentActionStat> stats = buckets.toActionStats().collect(Collectors.toList());

        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder().setEnvironmentType(EnvironmentType.HYBRID))
                .setActionCount(2)
                .setEntityCount(2)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build();

        final CurrentActionStat stat2 = CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder().setEnvironmentType(EnvironmentType.CLOUD))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build();

        final CurrentActionStat stat3 = CurrentActionStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder().setEnvironmentType(EnvironmentType.ON_PREM))
                .setActionCount(1)
                .setEntityCount(1)
                .setSavings(0.0)
                .setInvestments(0.0)
                .build();

        assertThat(stats, containsInAnyOrder(stat1, stat2, stat3));
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
            .putInvolvedEntities(InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES, involvedEntities)
            .build();
        return singleActionInfo;
    }
}
