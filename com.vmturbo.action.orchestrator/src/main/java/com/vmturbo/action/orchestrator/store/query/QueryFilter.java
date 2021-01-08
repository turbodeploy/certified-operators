package com.vmturbo.action.orchestrator.store.query;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDisruptiveness;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionReversibility;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSavingsAmountRangeFilter;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionEnvironmentType;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * A optionalFilter that can be used to test whether an action passes or fails an
 * {@link ActionQueryFilter}.
 */
public class QueryFilter {
    private final ActionQueryFilter filter;

    private Pattern riskQueryPattern;

    private Pattern descriptionQueryPattern;

    /**
     * If present, this is a set of the OIDs from the involved entities list in the
     * ActionQueryFilter to support constant-time lookups.
     */
    @Nullable
    private final Set<Long> entitiesRestriction;

    private final InvolvedEntityCalculation involvedEntityCalculation;

    private final Predicate<ActionView> visibilityPredicate;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new query filter.
     *
     * @param actionQueryFilter The filter to test against.
     * @param visibilityPredicate The filter used for checking if action is visible.
     */
    public QueryFilter(final ActionQueryFilter actionQueryFilter, final Predicate<ActionView> visibilityPredicate) {
        this(
            actionQueryFilter,
            visibilityPredicate,
            actionQueryFilter.hasInvolvedEntities() ?
                new HashSet<>(actionQueryFilter.getInvolvedEntities().getOidsList())
                    : Collections.emptySet(),
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
    }

    /**
     * Create a new query filter.
     *
     * @param filter The filter to test against.
     * @param visibilityPredicate The filter used for checking if action is visible.
     * @param entitiesRestriction the entities to search for participating in the action.
     * @param involvedEntityCalculation How the entities must participate in the action.
     */
    public QueryFilter(@Nonnull final ActionQueryFilter filter,
                       @Nonnull final Predicate<ActionView> visibilityPredicate,
                       @Nonnull Set<Long> entitiesRestriction,
                       @Nonnull InvolvedEntityCalculation involvedEntityCalculation) {
        this.filter = filter;
        this.visibilityPredicate = visibilityPredicate;
        this.entitiesRestriction = Objects.requireNonNull(entitiesRestriction);
        this.involvedEntityCalculation = Objects.requireNonNull(involvedEntityCalculation);
        this.descriptionQueryPattern = filter.hasDescriptionQuery() ? Pattern.compile(filter.getDescriptionQuery()) : null;
        this.riskQueryPattern = filter.hasRiskQuery() ? Pattern.compile(filter.getRiskQuery()) : null;
    }

    /**
     * Test whether an action passes the filter.
     *
     * @param actionView The action to test against the filter.
     * @return true if the action passes the filter, false if the action does not pass the filter.
     */
    public boolean test(@Nonnull final ActionView actionView) {
        return test(filter, actionView);
    }

    private boolean test(@Nonnull final ActionQueryFilter filter,
                         @Nonnull final ActionView actionView) {
        if (filter.hasVisible() && filter.getVisible() != visibilityPredicate.test(actionView)) {
            return false;
        }

        if (filter.hasAssociatedScheduleId()) {
            final Optional<ActionSchedule> scheduleOpt = actionView.getSchedule();
            if (!scheduleOpt.isPresent()
                    || scheduleOpt.get().getScheduleId() != filter.getAssociatedScheduleId()) {
                return false;
            }
        }

        if (filter.hasEnvironmentType()) {
            try {
                final ActionEnvironmentType envType =
                    ActionEnvironmentType.forAction(actionView.getTranslationResultOrOriginal());
                if (!envType.matchesEnvType(filter.getEnvironmentType())) {
                    return false;
                }
            } catch (UnsupportedActionException e) {
                logger.error("Unsupported action found in action store: {}", e.getMessage());
                return false;
            }
        }

        // Return false if the action is not related to the specified entities.
        if (filter.hasInvolvedEntities() || filter.getEntityTypeCount() > 0) {
            try {
                // Get the involved entity once.
                List<ActionEntity> actionInvolvedEntities = ActionDTOUtil.getInvolvedEntities(
                    actionView.getTranslationResultOrOriginal(), involvedEntityCalculation);
                // If the caller specified an explicit list of OID to look for, we look in that
                // list and ignore the specified list of entity types.
                if (entitiesRestriction.size() > 0) {
                    final boolean containsInvolved = actionInvolvedEntities.stream()
                        .anyMatch(actionInvolvedEntity ->
                            this.entitiesRestriction.contains(actionInvolvedEntity.getId()));
                    if (!containsInvolved) {
                        return false;
                    }
                } else if (filter.hasInvolvedEntities() && actionInvolvedEntities.isEmpty()) {
                    // If the filter is explicitly looking for actions that involve no entities,
                    // and this action has no involved entities (e.g. buy RI) then this action
                    // matches the filter.
                    return true;
                } else if (filter.hasInvolvedEntities()) {
                    // If the filter has an explicitly set "InvolvedEntities" message containing
                    // nothing, then no actions match.
                    return false;
                }

                if (filter.getEntityTypeCount() > 0) {
                    // If the caller DID NOT specify an explicit list of OIDs, but DID specify a
                    // list of entity types, check the involved entities to see if they match the
                    // types.
                    final boolean containsType = actionInvolvedEntities.stream()
                        .anyMatch(actionInvolvedEntity ->
                            // This is a "contains" on a list, but the size of the list will be
                            // small.
                            filter.getEntityTypeList()
                                .contains(actionInvolvedEntity.getType()));
                    if (!containsType) {
                        return false;
                    }
                }
            } catch (UnsupportedActionException e) {
                return false;
            }
        }

        if (!filterByCollection(filter.getSeveritiesList(), actionView.getActionSeverity())) {
            return false;
        }

        if (!filterByCollection(filter.getTypesList(), ActionDTOUtil.getActionInfoActionType(
                                                            actionView.getTranslationResultOrOriginal()))) {
            return false;
        }

        if (!filterByCollection(filter.getModesList(), actionView.getMode())) {
            return false;
        }

        if (!filterByCollection(filter.getStatesList(), actionView.getState())) {
            return false;
        }

        if (!filterByCollection(filter.getCategoriesList(), actionView.getActionCategory())) {
            return false;
        }

        if (filter.hasDisruptiveness() && !testDisruptiveness(actionView, filter.getDisruptiveness())) {
            return false;
        }

        if (filter.hasReversibility() && !testReversibility(actionView, filter.getReversibility())) {
            return false;
        }

        if (filter.hasSavingsAmountRange() && !testSavingsAmountRange(actionView, filter.getSavingsAmountRange())) {
            return false;
        }

        if (filter.hasHasSchedule() && !testSchedule(actionView, filter.getHasSchedule())) {
            return false;
        }

        if (filter.hasHasPrerequisites() && !testPrerequisites(actionView, filter.getHasPrerequisites())) {
            return false;
        }

        if (!testDescriptionQuery(actionView)) {
            return false;
        }

        if (!testRiskQuery(actionView)) {
            return false;
        }

        if (filter.hasCostType()) {
            final double amount = actionView.getTranslationResultOrOriginal().getSavingsPerHour().getAmount();

            if (filter.getCostType() == ActionCostType.ACTION_COST_TYPE_NONE) {
                return amount == 0.0;
            }

            if (filter.getCostType() == ActionCostType.INVESTMENT) {
                return amount < 0.0;
            }

            if (filter.getCostType() == ActionCostType.SAVINGS) {
                return amount >= 0.0;
            }
        }

        return true;
    }

    private static <T> boolean filterByCollection(@Nonnull Collection<T> collection, @Nonnull T element) {
        return collection.isEmpty() || collection.contains(element);
    }

    private boolean testDescriptionQuery(@Nonnull final ActionView actionView) {
        return descriptionQueryPattern == null || descriptionQueryPattern.matcher(actionView.getDescription()).matches();
    }

    private boolean testRiskQuery(@Nonnull final ActionView actionView) {
        return riskQueryPattern == null || riskQueryPattern.matcher(actionView.getCombinedRisksString()).find();
    }

    private static boolean testDisruptiveness(@Nonnull final ActionView actionView, @Nonnull ActionDisruptiveness disruptiveness) {
        if (disruptiveness == ActionDisruptiveness.DISRUPTIVE) {
            return actionView.getRecommendation().getDisruptive();
        }

        if (disruptiveness == ActionDisruptiveness.NON_DISRUPTIVE) {
            return !actionView.getRecommendation().getDisruptive();
        }

        return true;
    }

    private static boolean testReversibility(@Nonnull final ActionView actionView, @Nonnull ActionReversibility reversibility) {
        if (reversibility == ActionReversibility.REVERSIBLE) {
            return actionView.getRecommendation().getReversible();
        }

        if (reversibility == ActionReversibility.IRREVERSIBLE) {
            return !actionView.getRecommendation().getReversible();
        }

        return true;
    }

    private static boolean testSavingsAmountRange(@Nonnull final ActionView actionView, @Nonnull ActionSavingsAmountRangeFilter savingsAmountRangeFilter) {
        final double amount = actionView.getTranslationResultOrOriginal().getSavingsPerHour().getAmount();

        if (savingsAmountRangeFilter.hasMinValue() && savingsAmountRangeFilter.hasMaxValue()) {
            return amount >= savingsAmountRangeFilter.getMinValue() && amount <= savingsAmountRangeFilter.getMaxValue();
        }

        if (savingsAmountRangeFilter.hasMinValue()) {
            return amount >= savingsAmountRangeFilter.getMinValue();
        }

        if (savingsAmountRangeFilter.hasMaxValue()) {
            return amount <= savingsAmountRangeFilter.getMaxValue();
        }

        return true;
    }

    private static boolean testSchedule(@Nonnull final ActionView actionView, boolean hasSchedule) {
        if (hasSchedule) {
            return actionView.getSchedule().isPresent();
        }

        return !actionView.getSchedule().isPresent();
    }

    private static boolean testPrerequisites(@Nonnull final ActionView actionView, boolean hasPrerequisites) {
        if (hasPrerequisites) {
            return !actionView.getRecommendation().getPrerequisiteList().isEmpty();
        }

        return actionView.getRecommendation().getPrerequisiteList().isEmpty();
    }
}
