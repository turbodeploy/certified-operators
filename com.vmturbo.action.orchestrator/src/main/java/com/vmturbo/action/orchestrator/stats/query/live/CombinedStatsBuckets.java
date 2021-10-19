package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExplanationComposer;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionResourceImpact;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat.StatGroup;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Represents a set of "buckets", where each bucket contains stats for action groups that
 * share a particular criteria.
 * <p>
 * The intended workflow:
 *    - Create the buckets using {@link CombinedStatsBucketsFactory}.
 *    - Call {@link CombinedStatsBuckets#addActionInfo(SingleActionInfo)} for each relevant action.
 *    - Call {@link CombinedStatsBuckets#toActionStats()} to get the grouped stats.
 */
class CombinedStatsBuckets {
    private static final Logger logger = LogManager.getLogger();

    private final Map<GroupByBucketKey, CombinedStatsBucket> bucketForGroup = new HashMap<>();

    private final List<GroupBy> groupBy;

    private final Predicate<ActionEntity> entityPredicate;

    private final InvolvedEntityCalculation involvedEntityCalculation;

    private final ActionGroupFilter actionGroupFilter;

    /**
     * Intentionally private. Use: {@link CombinedStatsBucketsFactory}.
     *
     * @param entityPredicate predicate used for filtering enitites before we combine.
     * @param groupByCriteria what we combine the buckets on
     * @param involvedEntityCalculation indicates which involved entities to extract from
     *                                  {@link SingleActionInfo#involvedEntities()}.
     */
    private CombinedStatsBuckets(@Nonnull final List<GroupBy> groupByCriteria,
                                 @Nonnull final Predicate<ActionEntity> entityPredicate,
                                 @Nonnull final InvolvedEntityCalculation involvedEntityCalculation,
                                 @Nonnull final ActionGroupFilter actionGroupFilter) {
        this.groupBy = groupByCriteria;
        this.entityPredicate = entityPredicate;
        this.involvedEntityCalculation = involvedEntityCalculation;
        this.actionGroupFilter = actionGroupFilter;
    }

    /**
     * Adds an action to the right bucket.
     *
     * @param actionInfo The {@link SingleActionInfo} for the action.
     */
    public void addActionInfo(@Nonnull final SingleActionInfo actionInfo) {
        final Collection<GroupByBucketKey> groupByBucketKeys = bucketKeysForAction(actionInfo);
        for (GroupByBucketKey bucketKey : groupByBucketKeys) {
            final CombinedStatsBucket bucket = bucketForGroup.computeIfAbsent(bucketKey, k -> {
                final StatGroup.Builder bldr = StatGroup.newBuilder();
                bucketKey.state().ifPresent(bldr::setActionState);
                bucketKey.category().ifPresent(bldr::setActionCategory);
                bucketKey.risk().ifPresent(bldr::setActionRelatedRisk);
                bucketKey.type().ifPresent(bldr::setActionType);
                bucketKey.costType().ifPresent(bldr::setCostType);
                bucketKey.csp().ifPresent(bldr::setCsp);
                bucketKey.environmentType().ifPresent(bldr::setEnvironmentType);
                bucketKey.severity().ifPresent(bldr::setSeverity);
                bucketKey.targetEntityType().ifPresent(bldr::setTargetEntityType);
                bucketKey.targetEntityId().ifPresent(bldr::setTargetEntityId);
                bucketKey.reasonCommodityBaseType().ifPresent(bldr::setReasonCommodityBaseType);
                bucketKey.businessAccountId().ifPresent(bldr::setBusinessAccountId);
                bucketKey.resourceGroupId().ifPresent(bldr::setResourceGroupId);
                bucketKey.nodePoolId().ifPresent(bldr::setNodePoolId);

                return new CombinedStatsBucket(
                    entityPredicate,
                    bldr.build(),
                    involvedEntityCalculation);
            });
            bucket.add(actionInfo);
        }
    }

    @Nonnull
    private Collection<GroupByBucketKey> bucketKeysForAction(
        @Nonnull final SingleActionInfo actionInfo) {
        // When processing lots of actions creating all of these keys may be inefficient.
        // If necessary we can consider a "faster" implementation, or recycling the keys.
        final ImmutableGroupByBucketKey.Builder keyBuilder = ImmutableGroupByBucketKey.builder();
        final ActionView actionView = actionInfo.action();
        final ActionDTO.Action action = actionView.getTranslationResultOrOriginal();
        if (groupBy.contains(GroupBy.ACTION_CATEGORY)) {
            keyBuilder.category(actionView.getActionCategory());
        }
        if (groupBy.contains(GroupBy.ACTION_STATE)) {
            keyBuilder.state(actionView.getState());
        }
        if (groupBy.contains(GroupBy.ACTION_TYPE)) {
            keyBuilder.type(ActionDTOUtil.getActionInfoActionType(action));
        }
        if (groupBy.contains(GroupBy.COST_TYPE)) {
            keyBuilder.costType(ActionDTOUtil.getActionCostTypeFromAction(action));
        }
        if (groupBy.contains(GroupBy.SEVERITY)) {
            keyBuilder.severity(actionView.getActionSeverity());
        }
        if (groupBy.contains(GroupBy.TARGET_ENTITY_TYPE)) {
            try {
                keyBuilder.targetEntityType(ActionDTOUtil.getPrimaryEntity(action).getType());
            } catch (UnsupportedActionException e) {
                // This shouldn't really happen unless we add a new action type and forget
                // to update the code.
                logger.error("Failed to get primary entity of action with unsupported type: {}",
                    e.getMessage());
            }
        }
        if (groupBy.contains(GroupBy.TARGET_ENTITY_ID)) {
            try {
                keyBuilder.targetEntityId(ActionDTOUtil.getPrimaryEntityId(action));
            } catch (UnsupportedActionException e) {
                logger.error("Failed to get the id of the primary entity of action: {}",
                    e.getMessage());
            }
        }

        if (groupBy.contains(GroupBy.BUSINESS_ACCOUNT_ID)) {
            keyBuilder.businessAccountId(actionView.getAssociatedAccount()
                // Use an explicit 0 for actions not associated with accounts.
                .orElse(0L));
        }

        if (groupBy.contains(GroupBy.RESOURCE_GROUP_ID)) {
            keyBuilder.resourceGroupId(
                actionInfo.action().getAssociatedResourceGroupId().orElse(0L));
        }

        if (groupBy.contains(GroupBy.CSP)) {
            keyBuilder.csp(actionView.getAssociatedAccount()
                    // Use an explicit 0 for actions not associated with accounts.
                    .orElse(0L).toString());
        }

        Set<ImmutableGroupByBucketKey> bucketKeys = new HashSet<>();
        bucketKeys.add(keyBuilder.build());
        bucketKeys = groupByReasonCommodities(bucketKeys, action);
        bucketKeys = groupByActionRelatedRisk(bucketKeys, action);
        bucketKeys = groupByEnvironmentType(bucketKeys, action);
        bucketKeys = groupByNodePoolIds(bucketKeys, actionView);

        return bucketKeys.stream().collect(Collectors.toSet());
    }

    /**
     * Group by reason commodities.
     * Create a new bucket key for each combination of a reason commodity and an existing bucket key.
     *
     * @param bucketKeys existing bucket keys
     * @param action the action to put into buckets
     * @return a set of bucket keys
     */
    private Set<ImmutableGroupByBucketKey> groupByReasonCommodities(
            @Nonnull final Set<ImmutableGroupByBucketKey> bucketKeys,
            @Nonnull final Action action) {
        if (!groupBy.contains(GroupBy.REASON_COMMODITY)) {
            return bucketKeys;
        }

        final Set<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toSet());
        if (reasonCommodities.isEmpty()) {
            return bucketKeys;
        }

        final Set<ImmutableGroupByBucketKey> newBucketKeys =
            new HashSet<>(bucketKeys.size() * reasonCommodities.size());
        for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {
            for (ReasonCommodity reasonCommodity : reasonCommodities) {
                newBucketKeys.add(bucketKey.withReasonCommodityBaseType(
                    reasonCommodity.getCommodityType().getType()));
            }
        }

        return newBucketKeys;
    }

    /**
     * Group by action related risk.
     * Create a new bucket key for each combination of a short action explanation and an existing bucket key.
     *
     * @param bucketKeys existing bucket keys
     * @param action the action to put into buckets
     * @return a set of bucket keys
     */
    private Set<ImmutableGroupByBucketKey> groupByActionRelatedRisk(
            @Nonnull final Set<ImmutableGroupByBucketKey> bucketKeys,
            @Nonnull final Action action) {
        if (!groupBy.contains(GroupBy.ACTION_RELATED_RISK)) {
            return bucketKeys;
        }

        final Set<String> relatedRisks = ExplanationComposer.composeRelatedRisks(action);
        if (relatedRisks.isEmpty()) {
            return bucketKeys;
        }

        final Set<ImmutableGroupByBucketKey> newBucketKeys =
            new HashSet<>(bucketKeys.size() * relatedRisks.size());
        for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {
            for (String relatedRisk : relatedRisks) {
                newBucketKeys.add(bucketKey.withRisk(relatedRisk));
            }
        }

        return newBucketKeys;
    }

    private Set<ImmutableGroupByBucketKey> groupByNodePoolIds(
            @Nonnull final Set<ImmutableGroupByBucketKey> bucketKeys,
            @Nonnull final ActionView actionView) {
        if (!groupBy.contains(GroupBy.NODE_POOL_ID)) {
            return bucketKeys;
        }


        final Collection<Long> relatedNodePools = actionView.getAssociatedNodePoolIds();
        if (relatedNodePools.isEmpty()) {
            return bucketKeys;
        }

        final Set<ImmutableGroupByBucketKey> newBucketKeys =
                new HashSet<>(bucketKeys.size() * relatedNodePools.size());
        for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {
            for (Long nodePoolId : relatedNodePools) {
                newBucketKeys.add(bucketKey.withNodePoolId(nodePoolId));
            }
        }

        return newBucketKeys;
    }

    /**
     * Group by action target entity environmentType.
     * Create a new bucket key for each combination of action target entity environmentType and an
     * existing bucket key.
     *
     * EnvironmentType grouping accounts for action filters and returns Hybrid counts only if
     * filter is Hybrid or undefined.
     *
     * @param bucketKeys existing bucket keys
     * @param action the action to put into buckets
     * @return a set of bucket keys
     */
    private Set<ImmutableGroupByBucketKey> groupByEnvironmentType(
            @Nonnull final Set<ImmutableGroupByBucketKey> bucketKeys,
            @Nonnull final Action action) {
        if (!groupBy.contains(GroupBy.ENVIRONMENT_TYPE)) {
            return bucketKeys;
        }

        final Set<ImmutableGroupByBucketKey> newBucketKeys = new HashSet<>(bucketKeys.size());
        try {
            EnvironmentType environmentType = ActionDTOUtil.getPrimaryEntity(action).getEnvironmentType();

            // case 1 - filter is undefined or hybrid
            if (!actionGroupFilter.hasEnvironmentType() || actionGroupFilter.getEnvironmentType().equals(EnvironmentType.HYBRID)) {
                for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {

                    // if action target entity's environment type is not Hybrid, then add hybrid
                    // to ensure grouping always contains Hybrid.
                    // eg: if environmentType is ONPREM, then counts for both ONPREM and HYBRID will
                    // be increased by 1.
                    //
                    // Hybrid will provide total number of actions at present in your environment.
                    if (environmentType != EnvironmentType.HYBRID) {
                        newBucketKeys.add(bucketKey.withEnvironmentType(EnvironmentType.HYBRID));
                    }

                    // add the specific environmentType.
                    newBucketKeys.add(bucketKey.withEnvironmentType(environmentType));
                }
                return newBucketKeys;
            }

            // case 2 - filter is Cloud or OnPrem
            for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {
                // only add the specific environmentType if it was requested.
                if (actionGroupFilter.getEnvironmentType().equals(environmentType)) {
                    newBucketKeys.add(bucketKey.withEnvironmentType(environmentType));
                }
            }
        } catch (UnsupportedActionException e) {
            logger.error("Failed to get the environment type of the primary entity of action: {}",
                    e.getMessage());
        }

        return newBucketKeys;
    }

    /**
     * Identifies the "key" of a bucket used to group action stats.
     * The "key" is the unique combination of bucket identifiers.
     */
    @Value.Immutable
    public interface GroupByBucketKey {

        /**
         * The {@link ActionState} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the state.
         */
        Optional<ActionState> state();

        /**
         * The {@link ActionCategory} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the category.
         */
        Optional<ActionCategory> category();

        /**
         * The action risk (short-form explanation) for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the explanation.
         */
        Optional<String> risk();

        /**
         * The {@link ActionType} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the type.
         */
        Optional<ActionType> type();

        /**
         * The {@link ActionCostType} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the cost type.
         */
        Optional<ActionCostType> costType();

        /**
         * The cloud service provider for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the csp.
         */
        Optional<String> csp();

        /**
         * The target entity environment type for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the environment type.
         */
        Optional<EnvironmentType> environmentType();

        /**
         * The {@link Severity} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the severity.
         */
        Optional<Severity> severity();

        /**
         * The type of the primary/target entity for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the target entity type.
         */
        Optional<Integer> targetEntityType();

        /**
         * The id of the primary/target entity for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the target entity id.
         */
        Optional<Long> targetEntityId();

        /**
         * The type of the reason commodity for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the reason commodity.
         */
        Optional<Integer> reasonCommodityBaseType();

        /**
         * The business account associated with the actions in this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the business account.
         */
        Optional<Long> businessAccountId();

        /**
         * The resource group associated with the actions in this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the resource group.
         */
        Optional<Long> resourceGroupId();

        /**
         * The node pool associated with the actions in this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the node pool.
         */
        Optional<Long> nodePoolId();
    }

    /**
     * @return the {@link ActionDTO.ActionStat} equivalent for each bucket.
     */
    @Nonnull
    Stream<CurrentActionStat> toActionStats() {
        return bucketForGroup.values().stream()
            .map(CombinedStatsBucket::toStat);
    }

    private static class CombinedStatsBucket {
        private final StatGroup statGroup;

        private final Predicate<ActionEntity> entityPredicate;

        private final InvolvedEntityCalculation involvedEntityCalculation;

        private int actionCount = 0;

        private Set<Long> involvedEntities = new HashSet<>();

        private double savings = 0;

        private double investment = 0;

        private Map<String, ResourceImpact> resourceImpact = new HashMap<>();

        CombinedStatsBucket(@Nonnull final Predicate<ActionEntity> entityPredicate,
                            @Nonnull final StatGroup statGroup,
                            @Nonnull final InvolvedEntityCalculation involvedEntityCalculation) {
            this.entityPredicate = Objects.requireNonNull(entityPredicate);
            this.statGroup = Objects.requireNonNull(statGroup);
            this.involvedEntityCalculation = Objects.requireNonNull(involvedEntityCalculation);
        }

        public void add(@Nonnull final SingleActionInfo actionInfo) {
            actionInfo.involvedEntities().get(involvedEntityCalculation).stream()
                .filter(entityPredicate)
                .map(ActionEntity::getId)
                .forEach(this.involvedEntities::add);
            this.actionCount++;
            double savingAmount = actionInfo.action().getTranslationResultOrOriginal()
                    .getSavingsPerHour().getAmount();
            if (savingAmount >= 0) {
                this.savings += savingAmount;
            } else {
                // Subtracting a negative = addition.
                this.investment -= savingAmount;
            }

            // only compute resource impacts if action grouping is by actionTypes and targetType
            if (statGroup.hasActionType() && statGroup.hasTargetEntityType()) {
                addToResourceImpact(actionInfo);
            }
        }

        @Nonnull
        CurrentActionStat toStat() {

            List<ActionResourceImpact> actionResourceImpactList = this.resourceImpact.values()
                    .stream()
                    .map(ResourceImpact::toActionResourceImpact)
                    .collect(Collectors.toList());

            return CurrentActionStat.newBuilder()
                .setStatGroup(statGroup)
                .setActionCount(actionCount)
                .setEntityCount(involvedEntities.size())
                .setSavings(savings)
                .setInvestments(investment)
                .addAllActionResourceImpacts(actionResourceImpactList)
                .build();
        }

        private void addToResourceImpact(@Nonnull final SingleActionInfo actionInfo) {
            Action action = actionInfo.action().getRecommendation();

            // For resize action collect all reason commodities that impact actions.
            if (action.getInfo().hasResize()) {
                addToResourceImpactForResize(action);
            }
        }

        private void addToResourceImpactForResize(@Nonnull final Action action) {
            // Since Action only have information about the first reason commodity we can only compute
            // the resource impact for that reason commodity
            ReasonCommodity reasonCommodity = ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList()).get(0);
            int reasonCommodityType = reasonCommodity.getCommodityType().getType();
            String key = statGroup.getTargetEntityType() + "_" + statGroup.getActionType() + "_" + reasonCommodityType;

            // the following two lines can be moved to a separate method if we support
            // different action types in the future
            Resize resize = action.getInfo().getResize();
            double newValue = resize.getNewCapacity() - resize.getOldCapacity();

            ResourceImpact impact;
            if (!this.resourceImpact.containsKey(key)) {
                impact = new ResourceImpact(
                        statGroup.getTargetEntityType(),
                        statGroup.getActionType(),
                        reasonCommodityType,
                        newValue
                );
            } else {
                impact = this.resourceImpact.get(key);
                impact.updateAmount(newValue);
            }
            this.resourceImpact.put(key, impact);
        }
    }

    /**
     * Factory class for {@link CombinedStatsBuckets}, mainly for dependency injection and
     * unit testing purposes.
     */
    static class CombinedStatsBucketsFactory {
        @Nonnull
        CombinedStatsBuckets bucketsForQuery(@Nonnull final QueryInfo query) {
            return new CombinedStatsBuckets(
                query.query().getGroupByList(),
                query.entityPredicate(),
                query.involvedEntityCalculation(),
                query.query().getActionGroupFilter());
        }
    }

    /**
     * ResourceImpact class for {@link CombinedStatsBuckets}, mainly for aggregating resource impacts
     * for a unique combination of targetEntity, actionType and reasonCommodity.
     */
    static class ResourceImpact {

        private final int targetEntityType;
        private final ActionType actionType;
        private final int reasonCommodityType;
        private double amount;

        public ResourceImpact(int targetEntityType, ActionType actionType, int reasonCommodityType, double amount) {
            this.targetEntityType = targetEntityType;
            this.actionType = actionType;
            this.reasonCommodityType = reasonCommodityType;
            this.amount = amount;
        }

        public void updateAmount(double newValue) {
            this.amount += newValue;
        }

        @Nonnull
        public ActionResourceImpact toActionResourceImpact() {
            return ActionResourceImpact.newBuilder()
                    .setTargetEntityType(this.targetEntityType)
                    .setActionType(this.actionType)
                    .setReasonCommodityBaseType(this.reasonCommodityType)
                    .setAmount(this.amount)
                    .build();
        }
    }
}
