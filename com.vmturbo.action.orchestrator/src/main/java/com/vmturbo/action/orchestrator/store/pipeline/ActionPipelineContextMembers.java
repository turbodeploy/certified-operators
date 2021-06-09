package com.vmturbo.action.orchestrator.store.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.reflect.TypeToken;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.LiveActionStore.ActionSource;
import com.vmturbo.action.orchestrator.store.LiveActionStore.RecommendationTracker;
import com.vmturbo.action.orchestrator.store.atomic.AggregatedAction;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionDifference;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;

/**
 * Definitions for {@link PipelineContextMemberDefinition}s for the {@link ActionPipelineContext}.
 */
public class ActionPipelineContextMembers {

    /**
     * Hide constructor for utility class.
     */
    private ActionPipelineContextMembers() {

    }

    /**
     * The set of OIDs for entities involved in an action plan.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Set<Long>> INVOLVED_ENTITY_IDS =
        PipelineContextMemberDefinition.member(
            (Class<Set<Long>>)(new TypeToken<Set<Long>>(){}).getRawType(),
            () -> "involvedEntityIds",
            set -> "size=" + set.size());

    /**
     * Map of OID of the aggregation target and the AggregatedAction.
     * See {@link com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory#aggregate(List)}
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Map<Long, AggregatedAction>> AGGREGATED_ACTIONS =
        PipelineContextMemberDefinition.member(
            (Class<Map<Long, AggregatedAction>>)(new TypeToken<Map<Long, AggregatedAction>>(){}).getRawType(),
            () -> "aggregatedActions",
            map -> "size=" + map.size());

    /**
     * Map of the id of the market action DTO that was merged and the corresponding AggregateAction.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Map<Long, AggregatedAction>> ACTION_ID_TO_AGGREGATE_ACTION =
        PipelineContextMemberDefinition.member(
            (Class<Map<Long, AggregatedAction>>)(new TypeToken<Map<Long, AggregatedAction>>(){}).getRawType(),
            () -> "actionIdToAggregateAction",
            map -> "size=" + map.size());

    /**
     * A snapshot of entities and settings for the topology whose actions are being processed.
     */
    public static final PipelineContextMemberDefinition<EntitiesAndSettingsSnapshot> ENTITIES_AND_SETTINGS_SNAPSHOT =
        PipelineContextMemberDefinition.member(
            EntitiesAndSettingsSnapshot.class,
            () -> "entitiesAndSettingsSnapshot",
            snapshot -> "numEntities=" + snapshot.getEntityMap().size());

    /**
     * An acceleration structure to look up actions that have already been recently executed. Actions
     * contained in this tracker that are re-recommended by the market should be discarded because
     * they were already executed recently.
     */
    public static final PipelineContextMemberDefinition<RecommendationTracker> LAST_EXECUTED_RECOMMENDATIONS_TRACKER =
        PipelineContextMemberDefinition.member(
            RecommendationTracker.class,
            () -> "lastExecutedRecommendationsTracker",
            tracker -> "size=" + tracker.size());

    /**
     * A list of actions that have been completed since the last time an action plan was processed.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<List<ActionView>> COMPLETED_SINCE_LAST_POPULATE =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<List<ActionView>>)(new TypeToken<List<ActionView>>(){}).getRawType(),
            () -> "completedSinceLastPopulate",
            ArrayList::new,
            list -> "size=" + list.size()
        );

    /**
     * Market actions that were merged to create the atomic actions. These actions should be removed
     * from the action store because we retain the atomic actions in favor of the original market
     * actions.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<List<Action>> MERGED_MARKET_ACTIONS_FOR_ATOMIC =
        PipelineContextMemberDefinition.member(
            (Class<List<Action>>)(new TypeToken<List<Action>>(){}).getRawType(),
            () -> "mergedMarketActionsForAtomic",
            map -> "size=" + map.size()
        );

    /**
     * {@link ActionCounts} for the actions in the action store before processing a new action plan.
     */
    public static final PipelineContextMemberDefinition<ActionCounts> ACTION_STORE_STARTING_COUNTS =
        PipelineContextMemberDefinition.member(
            ActionCounts.class,
            () -> "actionStoreStartingCounts",
            size -> ""
        );

    /**
     * {@link ActionCounts} for the actions in the previous action plan processed. If no previous
     * action plan was processed, this will be an empty {@link ActionCounts}.
     */
    public static final PipelineContextMemberDefinition<ActionCounts> PREVIOUS_ACTION_PLAN_COUNTS =
        PipelineContextMemberDefinition.member(
            ActionCounts.class,
            () -> "previousActionPlanCounts",
            size -> ""
        );

    /**
     * {@link ActionCounts} for the action plan currently being processed.
     */
    public static final PipelineContextMemberDefinition<ActionCounts> CURRENT_ACTION_PLAN_COUNTS =
        PipelineContextMemberDefinition.member(
            ActionCounts.class,
            () -> "currentActionPlanCounts",
            size -> ""
        );

    /**
     * Bundles together context members used by a particular action processing segment
     * (either market or atomic).
     */
    public static class ProcessingContextMembers {
        /**
         * Maintains the difference of actions across what is in the store and what needs to be added/removed
         * from the store, etc. The contents of the difference changes as the action pipeline runs.
         */
        private final PipelineContextMemberDefinition<ActionDifference> actionDifference;

        /**
         * The number of actions input to a the action processing segment. Used
         * to compute statistics when logging information about actions added, removed, etc.
         */
        private final PipelineContextMemberDefinition<Integer> inputActionCount;

        /**
         * The number of new actions that we will attempt to store.
         */
        private final PipelineContextMemberDefinition<Integer> newActionCount;

        /**
         * Actions that should be added to the live actions we store.
         */
        private final PipelineContextMemberDefinition<List<Action>> actionsToAdd;

        @SuppressWarnings("unchecked")
        private ProcessingContextMembers(@Nonnull final ActionSource actionSource) {
            actionDifference = PipelineContextMemberDefinition.member(
                ActionDifference.class,
                () -> actionSource.getSourceName() + "ActionDifference",
                ActionDifference::sizeDescription
            );

            inputActionCount = PipelineContextMemberDefinition.member(
                Integer.class,
                () -> actionSource.getSourceName() + "InputActionCount",
                size -> ""
            );

            newActionCount = PipelineContextMemberDefinition.member(
                Integer.class,
                () -> actionSource.getSourceName() + "NewActionCount",
                size -> ""
            );

            actionsToAdd = PipelineContextMemberDefinition.member(
                (Class<List<Action>>)(new TypeToken<List<Action>>(){}).getRawType(),
                () -> actionSource.getSourceName() + "ActionsToAdd",
                list -> "size=" + list.size()
            );
        }

        /**
         * Get the action difference context member.
         *
         * @return The action difference context member.
         */
        public PipelineContextMemberDefinition<ActionDifference> getActionDifference() {
            return actionDifference;
        }

        /**
         * Get the input action count.
         *
         * @return the input action count.
         */
        public PipelineContextMemberDefinition<Integer> getInputActionCount() {
            return inputActionCount;
        }

        /**
         * Get the new action count.
         *
         * @return the new action count.
         */
        public PipelineContextMemberDefinition<Integer> getNewActionCount() {
            return newActionCount;
        }

        /**
         * Get the actions to add.
         *
         * @return the actions to add.
         */
        public PipelineContextMemberDefinition<List<Action>> getActionsToAdd() {
            return actionsToAdd;
        }
    }

    /**
     * {@link ProcessingContextMembers} for the market action population segment of the
     * live processing pipeline. We repeat very similar pipeline segments for market
     * and atomic actions.
     */
    public static final ProcessingContextMembers MARKET = new ProcessingContextMembers(ActionSource.MARKET);

    /**
     * {@link ProcessingContextMembers} for the atomic action population segment of the
     * live processing pipeline. We repeat very similar pipeline segments for market
     * and atomic actions.
     */
    public static final ProcessingContextMembers ATOMIC = new ProcessingContextMembers(ActionSource.ATOMIC);
}
