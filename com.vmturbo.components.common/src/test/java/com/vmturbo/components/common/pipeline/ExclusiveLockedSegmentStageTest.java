package com.vmturbo.components.common.pipeline;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipeline;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipelineContext;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;

/**
 * Tests for {@link ExclusiveLockedSegmentStageTest}.
 */
public class ExclusiveLockedSegmentStageTest {

    private final TestPipelineContext context = new TestPipelineContext();

    /**
     * ExpectedException.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    /**
     * A stage for the {@link TestPipeline}.
     */
    public static class TestLockingSegmentStage extends
        ExclusiveLockedSegmentStage<Long, Long, Long, Long, TestPipelineContext> {
        /**
         * Constructor.
         *
         * @param lock The lock to use.
         * @param segmentDefinition The segment definition.
         */
        public TestLockingSegmentStage(@Nonnull final ReentrantLock lock,
                                       @Nonnull SegmentDefinition<Long, Long, TestPipelineContext> segmentDefinition) {
            super(lock, 1, TimeUnit.HOURS, segmentDefinition);
        }

        @Nonnull
        @Override
        protected Long setupExecution(@Nonnull Long input) {
            return input;
        }

        @Nonnull
        @Override
        protected StageResult<Long> completeExecution(@Nonnull StageResult<Long> segmentResult) {
            return segmentResult;
        }

        @Override
        protected void finalizeExecution(boolean executionCompleted) {
            // do nothing
        }
    }

    final ReentrantLock lock = Mockito.spy(new ReentrantLock(true));

    /**
     * Test that lock is acquired when the stage is run.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testLockIsAcquired() throws PipelineStageException, InterruptedException {
        final TestPassthroughStage passthroughStage = Mockito.spy(new TestPassthroughStage());
        final TestLockingSegmentStage stage = new TestLockingSegmentStage(lock,
            SegmentDefinition.finalStage(passthroughStage));
        stage.setContext(context);
        stage.execute(1L);

        final InOrder inOrder = Mockito.inOrder(lock, passthroughStage);
        inOrder.verify(lock).tryLock(eq(1L), eq(TimeUnit.HOURS));
        inOrder.verify(passthroughStage).passthrough(eq(1L));
    }

    /**
     * Test that the lock is released when the pipeline executes successfully.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testLockIsReleasedOnSuccess() throws PipelineStageException, InterruptedException {
        final TestLockingSegmentStage stage = new TestLockingSegmentStage(lock,
            SegmentDefinition.finalStage(new TestPassthroughStage()));
        stage.setContext(context);
        stage.execute(1L);

        verify(lock).tryLock(eq(1L), eq(TimeUnit.HOURS));
        verify(lock).unlock();
    }

    /**
     * Test that even when the pipeline stages throw an exception, we release the lock.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testLockIsReleasedOnException() throws PipelineStageException, InterruptedException {
        final TestPassthroughStage passthroughStage = Mockito.spy(new TestPassthroughStage());
        when(passthroughStage.passthrough(eq(1L))).thenThrow(new PipelineStageException("blow up"));
        final TestLockingSegmentStage stage = new TestLockingSegmentStage(lock,
            SegmentDefinition.finalStage(passthroughStage));
        stage.setContext(context);
        stage.execute(1L);

        verify(lock).tryLock(eq(1L), eq(TimeUnit.HOURS));
        verify(lock).unlock();
    }

    /**
     * Test that if we fail to acquire the lock, we do NOT attempt to unlock it.
     * Unlocking a lock that we do not own will result in a different exception than the one we would
     * intend to throw.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testExceptionOnFailureToAcquireLock() throws PipelineStageException, InterruptedException {
        final ReentrantLock lock = Mockito.mock(ReentrantLock.class);
        when(lock.tryLock(eq(1L), eq(TimeUnit.HOURS)))
            .thenReturn(false);

        final TestLockingSegmentStage stage = new TestLockingSegmentStage(lock,
            SegmentDefinition.finalStage(new TestPassthroughStage()));
        stage.setContext(context);
        expectedException.expect(PipelineStageException.class);
        expectedException.expectMessage("Unable to acquire the exclusiveLock after 1 HOURS.");
        stage.execute(1L);

        verify(lock, never()).unlock();
    }
}
