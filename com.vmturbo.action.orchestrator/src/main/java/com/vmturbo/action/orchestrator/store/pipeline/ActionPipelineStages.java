package com.vmturbo.action.orchestrator.store.pipeline;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline.PassthroughStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline.Stage;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;

/**
 * A wrapper class for the various {@link ActionPipeline.Stage} and {@link ActionPipeline.PassthroughStage}
 * implementations. Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class ActionPipelineStages {
    /**
     * Stores an action plan in a store created in the ActionStorehouse. Returns the
     * {@link ActionStore} where the actions in the {@link ActionPlan} were stored.
     */
    public static class PopulateActionStoreStage extends Stage<ActionPlan, ActionStore> {
        private final ActionStorehouse storehouse;

        /**
         * Create a new {@link PopulateActionStoreStage}.
         *
         * @param storehouse The {@link ActionStorehouse}.
         */
        public PopulateActionStoreStage(@Nonnull final ActionStorehouse storehouse) {
            this.storehouse = Objects.requireNonNull(storehouse);
        }

        @Nonnull
        @Override
        public StageResult<ActionStore> executeStage(@Nonnull final ActionPlan actionPlan) throws InterruptedException {
            final ActionStore actionStore = storehouse.storeActions(actionPlan);

            return StageResult.withResult(actionStore)
                .andStatus(Status.success());
        }
    }

    /**
     * A class that gathers helpful statistics about action processing from the pipeline.
     */
    public static class ActionProcessingInfoStage extends Stage<ActionStore, ActionProcessingInfo> {
        @Nonnull
        @Override
        public StageResult<ActionProcessingInfo> executeStage(@Nonnull ActionStore input) {
            return StageResult.withResult(new ActionProcessingInfo(input.size()))
                .andStatus(Status.success());
        }
    }

    /**
     * Stage to update automated actions.
     */
    public static class UpdateAutomationStage extends PassthroughStage<ActionStore> {

        private final ActionAutomationManager automationManager;

        /**
         * Create a new UpdateAutomationStage.
         *
         * @param automationManager The automation manager to use to update automation.
         */
        public UpdateAutomationStage(@Nonnull final ActionAutomationManager automationManager) {
            this.automationManager = Objects.requireNonNull(automationManager);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionStore actionStore) throws InterruptedException {
            automationManager.updateAutomation(actionStore);
            return Status.success();
        }
    }

    /**
     * Stage to update automated actions.
     */
    public static class UpdateSeverityCacheStage extends PassthroughStage<ActionStore> {

        @Nonnull
        @Override
        public Status passthrough(ActionStore actionStore)  {
            // severity cache must be refreshed after actions change (see EntitySeverityCache javadoc)
            actionStore.getEntitySeverityCache().ifPresent(cache -> cache.refresh(actionStore));
            return Status.success();
        }
    }
}
