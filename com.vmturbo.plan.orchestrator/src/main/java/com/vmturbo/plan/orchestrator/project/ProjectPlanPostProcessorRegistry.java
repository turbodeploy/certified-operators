package com.vmturbo.plan.orchestrator.project;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.plan.orchestrator.market.ProjectedTopologyProcessor;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener;

/**
 * A class that keeps track of registered {@link ProjectPlanPostProcessor}s, and routes
 * plan status updates to them.
 *
 * TODO (roman, Nov 28 2017): The main purpose of this method is to keep references to the
 * registered {@link ProjectPlanPostProcessor}s to avoid garbage collection. It's currently
 * implemented this way to allow for concurrent work on launching the plan project and
 * processing of the results. Once both parts of the work
 * are completed and checked in, we can re-evaluate whether we need this. It may be better
 * to have a PlanProjectInstance (or similar) to track a single execution of a plan project,
 * and nest the {@link ProjectPlanPostProcessor}s under that instance.
 *
 * TODO (roman, Nov 28 2017): Consider switching this to be a factory class for
 * PlanProjectPostProcessors, which will also manage references.
 */
@ThreadSafe
public class ProjectPlanPostProcessorRegistry implements PlanStatusListener, ProjectedTopologyProcessor {

    private Map<Long, ProjectPlanPostProcessor> projectIdForPlan =
            Collections.synchronizedMap(new HashMap<>());

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        final ProjectPlanPostProcessor postProcessor = projectIdForPlan.get(plan.getPlanId());
        if (postProcessor != null) {
            postProcessor.onPlanStatusChanged(plan);
        }
    }

    /**
     * Register a {@link ProjectPlanPostProcessor} to be called when the plan associated
     * with it changes state.
     *
     * @param postProcessor The {@link ProjectPlanPostProcessor}.
     */
    public void registerPlanPostProcessor(@Nonnull final ProjectPlanPostProcessor postProcessor) {
        postProcessor.registerOnCompleteHandler(this::onPostProcessComplete);
        projectIdForPlan.put(postProcessor.getPlanId(), postProcessor);
    }

    private void onPostProcessComplete(@Nonnull final ProjectPlanPostProcessor postProcessor) {
        projectIdForPlan.remove(postProcessor.getPlanId());
    }

    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
        final ProjectPlanPostProcessor postProcessor = projectIdForPlan.get(sourceTopologyInfo.getTopologyContextId());
        return postProcessor != null && postProcessor.appliesTo(sourceTopologyInfo);
    }

    @Override
    public void handleProjectedTopology(final long projectedTopologyId,
                                        @Nonnull final TopologyInfo sourceTopologyInfo,
                                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> iterator)
            throws InterruptedException, TimeoutException, CommunicationException {
        final ProjectPlanPostProcessor postProcessor = projectIdForPlan.get(sourceTopologyInfo.getTopologyContextId());
        if (postProcessor != null) {
            postProcessor.handleProjectedTopology(projectedTopologyId, sourceTopologyInfo, iterator);
        } else {
            RemoteIteratorDrain.drainIterator(iterator,
                TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo), false);
        }
    }
}
