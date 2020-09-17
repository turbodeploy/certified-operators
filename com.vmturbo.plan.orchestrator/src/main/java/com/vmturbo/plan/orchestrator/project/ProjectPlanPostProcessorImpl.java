package com.vmturbo.plan.orchestrator.project;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.project.PlanProjectStatusListener.PlanProjectStatusListenerException;

/**
 * Coordinates receiving status changes for plan instances that comprise a plan project.
 * Status of plan project gets updated in DB.
 */
@ThreadSafe
public abstract class ProjectPlanPostProcessorImpl implements ProjectPlanPostProcessor {
    protected static final Logger logger = LogManager.getLogger();

    /**
     * Plan instance id.
     */
    protected final long instanceId;

    /**
     * Used for project status update tracking.
     */
    protected final PlanProjectStatusTracker statusTracker;

    /**
     * For plan project completion tracking.
     */
    protected Consumer<ProjectPlanPostProcessor> onCompleteHandler;

    /**
     * Creates a new post-processor.
     *
     * @param instanceId Plan instance id.
     * @param statusTracker Tracker for plan project status.
     */
    protected ProjectPlanPostProcessorImpl(final long instanceId,
                                             @Nonnull final PlanProjectStatusTracker statusTracker) {
        this.instanceId = instanceId;
        this.statusTracker = Objects.requireNonNull(statusTracker);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onPlanStatusChanged(PlanInstance plan) {
        logger.trace("Received status notification of plan {} to {}.", plan.getPlanId(),
                plan.getStatus());
        if (plan.getStatus() == PlanStatus.SUCCEEDED || plan.getStatus() == PlanStatus.FAILED) {
            if (plan.getStatus() == PlanStatus.FAILED) {
                logger.error("Plan with ID {} failed! Error: {}",
                        plan.getPlanId(), plan.getStatusMessage());
            } else {
                logger.info("Plan with ID {} completed!", plan.getPlanId());
            }
            try {
                // Update the project plan status.
                statusTracker.updateStatus(plan.getPlanId(), plan.getStatus());
            } catch (PlanProjectStatusListenerException | IntegrityException
                    | NoSuchObjectException e) {
                logger.error("Error updating plan instance status " + plan.getStatus() + " id "
                    + plan.getPlanId(), e);
            } finally {
                if (onCompleteHandler != null) {
                    onCompleteHandler.accept(this);
                }
            }
        } else {
            logger.info("Plan with ID {} has new status: {}",
                        plan.getPlanId(), plan.getStatus());
        }
    }

    /**
     * Called when plan is deleted.
     *
     * @param plan The {@link PlanDTO.PlanInstance} that got deleted.
     */
    @Override
    public void onPlanDeleted(@Nonnull PlanDTO.PlanInstance plan) {
        logger.trace("Received delete notification of plan {}.", plan.getPlanId());
    }

    /**
     * Gets the plan id.
     *
     * @return Id of plan instance.
     */
    @Override
    public long getPlanId() {
        return instanceId;
    }

    /**
     * Registers completion handler.
     *
     * @param handler The handler, which will consume this {@link ProjectPlanPostProcessor}.
     */
    @Override
    public void registerOnCompleteHandler(@Nonnull Consumer<ProjectPlanPostProcessor> handler) {
        this.onCompleteHandler = handler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean appliesTo(@Nonnull TopologyInfo sourceTopologyInfo);

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleProjectedTopology(final long projectedTopologyId,
                                        @Nonnull final TopologyInfo sourceTopologyInfo,
                                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> it) {
        logger.trace("Received projected topology (id: {}) notification for plan {}.",
                getPlanId(), projectedTopologyId);
    }
}
