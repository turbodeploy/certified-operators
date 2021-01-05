package com.vmturbo.components.common.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.InOrder;

import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline.PassthroughStage;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.Stage;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Unit tests for the {@link Pipeline}.
 *
 * <p/>These tests use a test implementation of a {@link Pipeline} - {@link TestPipeline}.
 */
public class PipelineTest {

    private final TestPipelineContext context = new TestPipelineContext();

    /**
     * Test running a multi-stage pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testTwoStagePipeline() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        final TestPassthroughStage stage2 = spy(new TestPassthroughStage());
        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                        .addStage(stage1)
                        .finalStage(stage2));


        assertThat(stage1.getContext(), is(context));
        assertThat(stage2.getContext(), is(context));
        assertThat(simplePipeline.run(10L), is(10L));

        // Assert that they were changed in order.
        final InOrder order = inOrder(stage1, stage2);
        order.verify(stage1).execute(any());
        order.verify(stage2).execute(any());
    }

    /**
     * Test that a stage that throws an exception crashes the pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineException() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        when(stage1.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage1));
        simplePipeline.run(10L);
    }

    /**
     * Test that a required passthrough stage that throws an exception crashes the pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineRequiredPassthroughStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));
        when(stage.required()).thenReturn(true);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        simplePipeline.run(10L);
    }

    /**
     * Test that a required passthrough stage that throws a RUNTIME exception crashes the pipeline,
     * and that the exception is wrapped inside a {@link PipelineException}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineRequiredPassthroughStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new RuntimeException("ERROR"));
        when(stage.required()).thenReturn(true);
        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        simplePipeline.run(10L);
    }

    /**
     * Test that a passthrough stage configured for retries attempts to re-run if it encounters
     * an error.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelinePassthroughStageRetry() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        final RuntimeException err = new RuntimeException("ERROR");
        final MutableInt invocationCount = new MutableInt(0);
        doAnswer(invocation -> {
            if (invocationCount.intValue() == 0) {
                invocationCount.increment();
                throw err;
            } else {
                return invocation.callRealMethod();
            }
        }).when(stage).passthrough(eq(10L));

        doReturn(Optional.of(1L)).when(stage).getRetryIntervalMs(anyInt(), any());

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));

        // Shouldn't throw an exception.
        simplePipeline.run(10L);
        // Should have been invoked twice.
        verify(stage, times(2)).passthrough(10L);
    }

    /**
     * Test that a NON-required passthrough stage does not crash the pipeline if it terminates with
     * an error.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineNotRequiredStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new PipelineStageException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        assertThat(simplePipeline.run(10L), is(10L));
    }

    /**
     * Test that a NON-required passthrough stage does not crash the pipeline if it terminates with
     * a RUNTIME exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineNotRequiredStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new RuntimeException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        assertThat(simplePipeline.run(10L), is(10L));
    }

    /**
     * A stage for the {@link TestPipeline}.
     */
    public static class TestStage extends Stage<Long, Long, TestPipelineContext> {
        @NotNull
        @Nonnull
        @Override
        public StageResult<Long> execute(@NotNull @Nonnull final Long input)
                throws PipelineStageException, InterruptedException {
            return StageResult.withResult(input)
                    .andStatus(Status.success());
        }
    }

    /**
     * A passthrough stage for the {@link TestPipeline}.
     */
    public static class TestPassthroughStage extends PassthroughStage<Long, TestPipelineContext> {
        @NotNull
        @Override
        public Status passthrough(final Long input) throws PipelineStageException {
            // Don't do anything.
            return Status.success();
        }
    }

    /**
     * The {@link PipelineContext} for the {@link TestPipeline}.
     */
    public static class TestPipelineContext implements PipelineContext {

        @NotNull
        @Override
        public String getPipelineName() {
            return "test-pipeline";
        }
    }

    /**
     * The {@link PipelineSummary} for the {@link TestPipeline}.
     */
    public static class TestPipelineSummary extends PipelineSummary {

        protected TestPipelineSummary(@NotNull Clock clock,
                @NotNull List<Stage> stages) {
            super(clock, stages);
        }

        @Override
        protected String getPreamble() {
            return "FOO";
        }
    }

    /**
     * A {@link Pipeline} implementation for tests.
     *
     * @param <I> The input type.
     * @param <O> The output type.
     */
    public static class TestPipeline<I, O> extends Pipeline<I, O, TestPipelineContext, TestPipelineSummary> {

        protected TestPipeline(@NotNull PipelineDefinition<I, O, TestPipelineContext> stages) {
            super(stages, new TestPipelineSummary(Clock.systemUTC(), stages.getStages()));
        }

        @Override
        protected DataMetricTimer startPipelineTimer() {
            return null;
        }

        @Override
        protected DataMetricTimer startStageTimer(String stageName) {
            return null;
        }

        @Override
        protected TracingScope startPipelineTrace() {
            return null;
        }

        @Override
        protected TracingScope startStageTrace(String stageName) {
            return null;
        }
    }
}
