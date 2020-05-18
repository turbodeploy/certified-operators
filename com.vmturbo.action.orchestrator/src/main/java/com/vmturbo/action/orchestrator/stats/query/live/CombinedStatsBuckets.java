package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat.StatGroup;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

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
                                 @Nonnull final InvolvedEntityCalculation involvedEntityCalculation) {
        this.groupBy = groupByCriteria;
        this.entityPredicate = entityPredicate;
        this.involvedEntityCalculation = involvedEntityCalculation;
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
                bucketKey.explanation().ifPresent(bldr::setActionExplanation);
                bucketKey.type().ifPresent(bldr::setActionType);
                bucketKey.costType().ifPresent(bldr::setCostType);
                bucketKey.csp().ifPresent(bldr::setCsp);
                bucketKey.severity().ifPresent(bldr::setSeverity);
                bucketKey.targetEntityType().ifPresent(bldr::setTargetEntityType);
                bucketKey.targetEntityId().ifPresent(bldr::setTargetEntityId);
                bucketKey.reasonCommodityBaseType().ifPresent(bldr::setReasonCommodityBaseType);
                bucketKey.businessAccountId().ifPresent(bldr::setBusinessAccountId);
                bucketKey.resourceGroupId().ifPresent(bldr::setResourceGroupId);
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
        bucketKeys = groupByActionExplanation(bucketKeys, action);

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
     * Group by reason commodities.
     * Create a new bucket key for each combination of a short action explanation and an existing bucket key.
     *
     * @param bucketKeys existing bucket keys
     * @param action the action to put into buckets
     * @return a set of bucket keys
     */
    private Set<ImmutableGroupByBucketKey> groupByActionExplanation(
            @Nonnull final Set<ImmutableGroupByBucketKey> bucketKeys,
            @Nonnull final Action action) {
        if (!groupBy.contains(GroupBy.ACTION_EXPLANATION)) {
            return bucketKeys;
        }

        final Set<String> explanations = ExplanationComposer.composeShortExplanation(action);
        if (explanations.isEmpty()) {
            return bucketKeys;
        }

        final Set<ImmutableGroupByBucketKey> newBucketKeys =
            new HashSet<>(bucketKeys.size() * explanations.size());
        for (ImmutableGroupByBucketKey bucketKey : bucketKeys) {
            for (String explanation : explanations) {
                newBucketKeys.add(bucketKey.withExplanation(explanation));
            }
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
         * The short-form explanation for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the explanation.
         */
        Optional<String> explanation();

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

        private int       actionCount = 0;

        private Set<Long> involvedEntities = new HashSet<>();

        private double    savings = 0;

        private double    investment = 0;

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
        }

        @Nonnull
        CurrentActionStat toStat() {
            return CurrentActionStat.newBuilder()
                .setStatGroup(statGroup)
                .setActionCount(actionCount)
                .setEntityCount(involvedEntities.size())
                .setSavings(savings)
                .setInvestments(investment)
                .build();
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
                query.involvedEntityCalculation());
        }
    }
}
