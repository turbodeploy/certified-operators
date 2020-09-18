package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.reflect.TypeToken;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline.PassthroughStage;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Utilities for testing the pipeline classes.
 */
public class PipelineTestUtilities {
    private PipelineTestUtilities() {
        // Private constructor for utility class.
    }

    /**
     * A stage for the {@link TestPipeline}.
     */
    public static class TestStage extends Stage<Long, Long, TestPipelineContext> {
        @NotNull
        @Nonnull
        @Override
        public StageResult<Long> executeStage(@NotNull @Nonnull final Long input)
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
    public static class TestPipelineContext extends PipelineContext {

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
     * TestMemberDefinitions.
     */
    public static class TestMemberDefinitions {
        /**
         * Foo.
         */
        public static final PipelineContextMemberDefinition<String> FOO
            = PipelineContextMemberDefinition.member(String.class, "foo", s -> s.length() + " characters");
        /**
         * Bar.
         */
        public static final PipelineContextMemberDefinition<Integer> BAR
            = PipelineContextMemberDefinition.member(Integer.class, "bar", i -> "one object");

        /**
         * Baz.
         */
        @SuppressWarnings("unchecked")
        public static final PipelineContextMemberDefinition<List<Long>> BAZ
            = PipelineContextMemberDefinition.member(
            (Class<List<Long>>)(new TypeToken<List<Long>>() { }).getRawType(),
            () -> "baz", l -> "size=" + l.size());

        /**
         * Quux.
         */
        public static final PipelineContextMemberDefinition<Integer> QUUX
            = PipelineContextMemberDefinition.memberWithDefault(Integer.class, () -> "quux",
            () -> 1, i -> "one object");
    }

    /**
     * TestPipeline.
     *
     * @param <I> Input type.
     * @param <O> Output type.
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
