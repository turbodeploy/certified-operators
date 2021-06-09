package com.vmturbo.components.common.pipeline;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.SegmentStageResult;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;

/**
 * A {@link SegmentStage} where the internal pipeline segment is executed within the scope of an
 * exclusively held lock (prevents multiple segments that share the lock from executing concurrently).
 *
 * @param <StageInputT> The stage input type.
 * @param <SegmentInputT> The segment input type.
 * @param <SegmentOutputT> The segment output type.
 * @param <StageOutputT> The stage output type.
 * @param <ContextT> The pipeline context type.
 */
public abstract class ExclusiveLockedSegmentStage<
    StageInputT, SegmentInputT,
    SegmentOutputT, StageOutputT,
    ContextT extends PipelineContext>
    extends SegmentStage<StageInputT, SegmentInputT, SegmentOutputT, StageOutputT, ContextT> {

    private final ReentrantLock exclusiveLock;
    private final long maxLockAcquireTimeMinutes;
    private final TimeUnit timeUnit;

    /**
     * Construct a new {@link ExclusiveLockedSegmentStage}.
     *
     *  @param exclusiveLock All stages in the interior segment are executed while holding exclusive
     *                       access to this shared lock. This prevents multiple segments that
     *                       share data from stepping on each other when using some of the shared
     *                       data structures used in the segment.
     * @param maxLockAcquireTime Maximum amount of time to wait in the provided time unit when attempting to acquire the
     *                          lock. If unable to acquire the lock in this time, we throw a
     *                          {@link PipelineStageException}.
     * @param timeUnit The timeUnit (ie minutes, seconds, etc.) associated with the acquire time.
     * @param segmentDefinition The definition for the stages in the interior pipeline segment to be run when
     *                          this stage runs.
     */
    public ExclusiveLockedSegmentStage(@Nonnull final ReentrantLock exclusiveLock,
                                       final long maxLockAcquireTime,
                                       @Nonnull final TimeUnit timeUnit,
                                       @Nonnull SegmentDefinition<SegmentInputT, SegmentOutputT, ContextT> segmentDefinition) {
        super(segmentDefinition);
        Preconditions.checkArgument(maxLockAcquireTime > 0,
            "Illegal value %s for maxLockAcquireTimeMinutes", maxLockAcquireTime);

        this.exclusiveLock = exclusiveLock;
        this.maxLockAcquireTimeMinutes = maxLockAcquireTime;
        this.timeUnit = Objects.requireNonNull(timeUnit);
    }

    /**
     * Execute the stage within the scope of an exclusively held lock.
     * If the lock cannot be acquired within the timeout, throws a {@link PipelineStageException}.
     * We guarantee that if the lock is acquired, it will be unlocked after
     * {@link #finalizeExecution(boolean)} is called.
     *
     * @param input The input to the stage.
     * @return The output of the stage.
     * @throws PipelineStageException If there is an error executing this stage.
     *                                Also thrown if the lock cannot be acquired within the given timeout.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    @Override
    protected StageResult<StageOutputT> executeStage(@Nonnull StageInputT input)
        throws PipelineStageException, InterruptedException {
        boolean executionCompleted = false;

        if (!exclusiveLock.tryLock(maxLockAcquireTimeMinutes, timeUnit)) {
            throw new PipelineStageException("Unable to acquire the exclusiveLock after "
                + maxLockAcquireTimeMinutes + " " + timeUnit + ".");
        }

        try {
            final SegmentInputT segmentInput = setupExecution(input);
            final StageResult<SegmentOutputT> segmentResult = runInteriorPipelineSegment(segmentInput);
            final StageResult<StageOutputT> stageResult = completeExecution(segmentResult);
            executionCompleted = true;

            return SegmentStageResult.forFinalStageResult(stageResult, segmentSummary);
        } finally {
            finalizeExecution(executionCompleted);
            exclusiveLock.unlock();
        }
    }
}
