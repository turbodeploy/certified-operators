package com.vmturbo.action.orchestrator.store.atomic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.action.orchestrator.store.pipeline.PlanActionPipelineFactory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;

/**
 * * Builds new ActionDTOs by merging ActionDTOs for different entities
 *  * based on the {@link AtomicActionSpec}.
 *  * When a new action plan is received by the Action Orchestrator, {@link PlanActionPipelineFactory} will
 *  * invoke the AtomicActionFactory to create atomic action DTOs by merging a group of actions
 *  * for entities controlled by the same execution target.
 */
public class PlanAtomicActionFactory extends AtomicActionFactory {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param atomicActionSpecsCache ActionMergeSpecsCache
     */
    public PlanAtomicActionFactory(@NotNull AtomicActionSpecsCache atomicActionSpecsCache) {
        super(atomicActionSpecsCache);
    }

    protected IAtomicActionBuilderFactory newAtomicActionBuilderFactory() {
        return new PlanAtomicActionBuilderFactory();
    }

    /**
     * Factory to create {@link AtomicActionBuilder} for actions belonging to Plan Topology.
     */
    private static class PlanAtomicActionBuilderFactory implements IAtomicActionBuilderFactory {

        PlanAtomicActionBuilderFactory() { }

        /**
         * AtomicActionBuilder that will create the {@link ActionDTO.Action} for the given AggregatedAction.
         *
         * @param aggregatedAction the {@link AggregatedAction}
         * @return AtomicActionBuilder to build {@link ActionDTO.Action} for the aggregated action
         */
        @Nullable
        public AtomicActionBuilder getActionBuilder(@Nonnull final AggregatedAction aggregatedAction) {
            ActionDTO.ActionInfo.ActionTypeCase actionTypeCase = aggregatedAction.getActionTypeCase();
            switch (actionTypeCase) {
                case ATOMICRESIZE:
                    return new PlanAtomicResizeBuilder(aggregatedAction);
                default:
                    return null;
            }
        }
    }
}
