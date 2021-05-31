package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.SegmentStageResult;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;

/**
 * A {@link SegmentStage} allows a logical grouping of related pipeline stages together into a
 * segment of stages. A {@link SegmentStage} can be leveraged to:
 * <ol>
 *     <li>
 *         Logically group a series of related stages. This allows timing of the overall
 *         group of stages while still getting performance numbers for the individual stages
 *         as well.
 *     </li>
 *     <li>
 *         Perform setup and teardown activities around a series of stages. ie this can be done
 *         to execute a series of stages within a single lock.
 *     </li>
 *     <li>
 *         Create a group of stages that can be reused.
 *     </li>
 *     <li>
 *         etc.
 *     </li>
 * </ol>
 * <p/>
 * Exceptions thrown by interior Segment stages are typically not handled by the Segment, but rather
 * rethrown to be handled by the overall pipeline (usually resulting in the halting of pipeline execution).
 * <p/>
 * The {@link SegmentStage} contains a set of protected methods that can be overridden by subclasses
 * to handle the segment setup and teardown. The control flow for these methods happens in this order:
 * <ol>
 *     <li>{@link #setupExecution(Object)}</li>
 *     <li>interior pipeline runs (this is not overrideable)</li>
 *     <li>{@link #completeExecution(StageResult)}</li>
 *     <li>{@link #finalizeExecution(boolean)}</li>
 * </ol>
 * <p/>
 * Note that circular/recursive {@link SegmentStage}s (ie Segment A contains Segment B, and Segment B
 * also contains Segment A) are impossible because construction of such circular definitions are prevented
 * by the fact that the list of stages for a Segment must be fully specified at the time of the creation
 * of the Segment. In the example above, for instance, during the creation of Segment A, Segment B must
 * already be created for it to be in the list of Segment A's stages. But for Segment A to also be in
 * Segment B's list of stages, it would also already have to exist, and we cannot meet both requirements
 * simultaneously.
 *
 * @param <StageInputT> The input type to the stage. Note that this is distinct from the input to the first stage of
 *                      the interior pipeline segment.
 * @param <SegmentInputT> The input to type to the interior pipeline segment.
 * @param <SegmentOutputT> The output type of the interior pipeline segment.
 * @param <StageOutputT> The output type of the stage. Note that this is distinct from output of the interior
 *                       pipeline segment.
 * @param <ContextT> The type of the pipeline context. Note that this type must match the type of the context
 *                  used by the overall pipeline to which this {@link SegmentStage} belongs.
 */
public abstract class SegmentStage<
    StageInputT, SegmentInputT,
    SegmentOutputT, StageOutputT,
    ContextT extends PipelineContext> extends Stage<StageInputT, StageOutputT, ContextT> implements StageExecutor<ContextT> {

    private static final Logger logger = LogManager.getLogger();

    private final List<Stage> interiorStages;
    protected final SegmentSummary segmentSummary;

    /**
     * Construct a new {@link SegmentStage}.
     *
     * @param segmentDefinition The definition for the stages in the interior pipeline segment to be run when
     *                          this stage runs.
     */
    public SegmentStage(@Nonnull final SegmentDefinition<SegmentInputT, SegmentOutputT, ContextT> segmentDefinition) {
        this(segmentDefinition, new SegmentSummary(Clock.systemUTC(), segmentDefinition.getStages()));
    }

    @VisibleForTesting
    SegmentStage(@Nonnull final SegmentDefinition<SegmentInputT, SegmentOutputT, ContextT> segmentDefinition,
                 @Nonnull final SegmentSummary summary) {
        this.interiorStages = segmentDefinition.getStages();
        this.segmentSummary = Objects.requireNonNull(summary);
    }

    @Override
    @Nonnull
    public AbstractSummary getSummary() {
        return segmentSummary;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setContext(@Nonnull final ContextT context) {
        super.setContext(context);
        interiorStages.forEach(stage -> stage.setContext(context));
    }

    /**
     * Get whether this segment is implemented as an anonymous class. The pipeline tabular description
     * which fetches metadata about stage inputs and outputs will fetch data for anonymous classes
     * differently.
     * <p/>
     * Anonymous segments are usually created via calls to {@link SegmentDefinition#asStage(String)}.
     *
     * @return Whether this segment is implemented as an anonymous class.
     */
    boolean isAnonymousClass() {
        return false;
    }

    /**
     * Setup execution of this stage. Can be used to do any setup before the interior stages are run.
     * Can also be used to transform the input from one data type to another for the stages that
     * are run.
     * <p/>
     * Note that for any setup done here that requires cleanup (ie if you acquire a lock here to
     * be held during the execution of the interior pipeline that needs to be released when the
     * interior stages are done), the cleanup should be done in {@link #finalizeExecution(boolean)}
     * because it will be guaranteed to be called regardless of what exceptions are thrown during
     * execution of interior stages.
     *
     * @param input The input to the stage.
     * @return The input to the interior stages of the pipeline.
     * @throws PipelineStageException If there is an error executing this stage.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    protected abstract SegmentInputT setupExecution(@Nonnull StageInputT input)
        throws PipelineStageException, InterruptedException;

    /**
     * Called on successful completion of the interior pipeline. Can be used to transform the output
     * from the final interior stage into another different data pipe to join it to another part
     * of the pipeline.
     * <p/>
     * {@link #completeExecution(StageResult)} only runs if the stages consisting of the segment
     * were able to complete execution. If the interior stages throw an exception, this method may not be
     * called during stage execution. For any operation that must be executed regardless of the
     * success or failure of the interior pipeline, see {@link #finalizeExecution(boolean)}.
     *
     * @param segmentResult The result of running the entire interior pipeline segment.
     * @return The result of this stage. The method may pass through the result of the interior pipeline,
     *         or if necessary it can transform the stage result into a different form for downstream consumers.
     * @throws PipelineStageException If there is an error executing this stage.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    protected abstract StageResult<StageOutputT> completeExecution(@Nonnull StageResult<SegmentOutputT> segmentResult)
        throws PipelineStageException, InterruptedException;

    /**
     * Complete execution of the overall stage. Any essential cleanup for the stage should be done
     * here and not in {@link #completeExecution(StageResult)} because this method is guaranteed to
     * be called regardless of whether the interior pipeline completed successfully.
     *
     * @param executionCompleted Whether the all the interior stages in the segment successfully completed.
     *                           If true, {@link #completeExecution(StageResult)} will have already been
     *                           called, if false, {@link #completeExecution(StageResult)} will not have
     *                           been called.
     */
    protected void finalizeExecution(boolean executionCompleted) {
        // By default there is nothing to do.
    }

    /**
     * Execute the stage.
     *
     * @param input The input to the stage.
     * @return The output of the stage.
     * @throws PipelineStageException If there is an error executing this stage.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    @Override
    protected StageResult<StageOutputT> executeStage(@Nonnull StageInputT input)
        throws PipelineStageException, InterruptedException {
        boolean executionCompleted = false;

        try {
            final SegmentInputT segmentInput = setupExecution(input);
            final StageResult<SegmentOutputT> segmentResult = runInteriorPipelineSegment(segmentInput);
            final StageResult<StageOutputT> stageResult = completeExecution(segmentResult);
            executionCompleted = true;

            return SegmentStageResult.forFinalStageResult(stageResult, segmentSummary);
        } finally {
            finalizeExecution(executionCompleted);
        }
    }

    /**
     * Run the interior stages of the pipeline segment embedded within this stage
     * in the order they're configured.
     *
     * @param segmentInput The input to the pipeline segment.
     * @return The output of the pipeline segment.
     * @throws PipelineException If a required stage fails.
     * @throws InterruptedException If the pipeline is interrupted.
     */
    @SuppressWarnings("unchecked")
    protected StageResult<SegmentOutputT> runInteriorPipelineSegment(@Nonnull final SegmentInputT segmentInput)
        throws PipelineStageException, InterruptedException {
        Object curStageInput = segmentInput;
        StageResult stageResult = null;
        segmentSummary.start();

        for (final Stage stage : interiorStages) {
            try {
                logger.info("Executing stage {}::{}", getName(), stage.getName());
                stageResult = runStage(stage, curStageInput);
                curStageInput = stageResult.getResult();
            } catch (PipelineStageException | RuntimeException e) {
                getSummary().endStage(Status.failed(e), false);
                getSummary().fail(e.getMessage() == null ? "" : e.getMessage());
                throw new PipelineStageException(e.getMessage(), e, getSummary());
            }
        }

        return (StageResult<SegmentOutputT>)stageResult;
    }

    /**
     * Override the default behavior for configuring context members so that we can also configure
     * context members for interior pipeline stages.
     *
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    void configureContextMembers(@Nonnull final Map<PipelineContextMemberDefinition<?>, String> provided,
                                 @Nonnull final Map<PipelineContextMemberDefinition<?>, Stage<?, ?, ?>> required)
        throws PipelineContextMemberException {
        configureContextMemberRequires(provided, required);
        // Ensure the correct configuration of all stages in the segment in this stage.
        for (final Stage stage : interiorStages) {
            stage.configureContextMembers(provided, required);
        }
        configureContextMemberProvides(provided, required);
    }

    /**
     * Get the list of interior stages in the pipeline segment.
     *
     * @return the list of interior stages in the pipeline segment.
     */
    @Nonnull
    List<Stage> getInteriorStages() {
        return interiorStages;
    }

    /**
     * The definition/spec for a {@link SegmentStage}.
     *
     * @param <I> The initial input type. This will be the input type to the pipeline segment.
     * @param <O> The output of the pipeline segment.
     * @param <C> The {@link PipelineContext} for the segment we are building. The context object
     *            will be the same as used by the surrounding main pipeline.
     */
    public static class SegmentDefinition<I, O, C extends PipelineContext> {
        private final List<Stage> stages;

        /**
         * The constructor is private. Use the builder.
         *
         * @param stages The stages.
         */
        private SegmentDefinition(@Nonnull final List<Stage> stages) {
            this.stages = stages;
        }

        /**
         * Get the stages defined within the segment.
         *
         * @return the stages defined within the segment.
         */
        @Nonnull
        public List<Stage> getStages() {
            return Collections.unmodifiableList(stages);
        }

        /**
         * Convenience method to create a new {@link SegmentStage} that will execute the series of stages
         * defined by this {@link SegmentDefinition}. Useful for logically grouping a series of associated
         * stages together in a segment.
         *
         * @param stageName The name of the stage. Since the segment will be an anonymous class this allows the
         *                  injection of a more meaningful name for the stage.
         * @return A new {@link SegmentStage} that, when executed as a stage within a {@link Pipeline},
         *         will run all the {@link Stage}s within this {@link SegmentDefinition}.
         */
        public SegmentStage<I, I, O, O, C> asStage(@Nonnull final String stageName) {
            return new SegmentStage<I, I, O, O, C>(this) {
                @Override
                @Nonnull
                public String getName() {
                    return stageName;
                }

                @Override
                boolean isAnonymousClass() {
                    return true;
                }

                @Override
                @Nonnull
                protected I setupExecution(@Nonnull I input) {
                    return input;
                }

                @Override
                @Nonnull
                protected StageResult<O> completeExecution(@Nonnull StageResult<O> segmentResult) {
                    return segmentResult;
                }
            };
        }

        /**
         * Create a new {@link SegmentDefinitionBuilder} for a pipeline segment with an initial stage.
         *
         * @param <I> The initial input type. This will be the input type to the entire segment.
         *            Note that the first stage added to the segment must also have input type I.
         * @param <C> The {@link PipelineContext}.
         * @return The {@link PipelineDefinitionBuilder} to use to assemble the stages.
         */
        public static <I, C extends PipelineContext> SegmentDefinitionBuilder<I, I, C> newBuilder() {
            return new SegmentDefinitionBuilder<>();
        }

        /**
         * Create a new {@link SegmentDefinitionBuilder} for a pipeline segment with an initial stage.
         *
         * @param stage The initial stage in the pipeline segment.
         * @param <I> The initial input type. This will be the input type to the entire segment.
         * @param <O> The output of the first stage of the segment.
         * @param <C> The {@link PipelineContext}.
         * @return The {@link PipelineDefinitionBuilder} to use to assemble the stages.
         */
        public static <I, O, C extends PipelineContext> SegmentDefinitionBuilder<I, O, C>
        addStage(@Nonnull final  Stage<I, O, C> stage) {
            return SegmentDefinition.<I, C>newBuilder().addStage(stage);
        }

        /**
         * Create a new {@link SegmentDefinition} for a pipeline segment with a single stage.
         *
         * @param stage The single stage in the {@link PipelineContext}.
         * @param <I> The initial input type. This will be the input type to segment.
         * @param <O> The output of the segment.
         * @param <C> The {@link PipelineContext}.
         * @return The {@link PipelineDefinitionBuilder} to use to assemble the stages.
         */
        public static <I, O, C extends PipelineContext> SegmentDefinition<I, O, C>
        finalStage(@Nonnull final  Stage<I, O, C> stage) {
            return SegmentDefinition.<I, C>newBuilder().finalStage(stage);
        }
    }

    /**
     * A builder for {@link SegmentDefinition}s. Provides method to add stages and a method to create the
     * {@link SegmentDefinition} by supplying the final stage.
     *
     * @param <I> The input type to the first stage in the pipeline segment.
     * @param <O> The output type to the last stage currently in the segment. Note that this type may change
     *            every time you add a stage to the builder. The final output type of the segment will match
     *            the output type of the final stage added to the segment.
     * @param <C> The type of the {@link PipelineContext} for the overall pipeline that this segment will
     *            be part of.
     */
    public static class SegmentDefinitionBuilder<I, O, C extends PipelineContext> {
        private final List<Stage> stages;

        /**
         * Create a new {@link SegmentDefinitionBuilder}.
         */
        private SegmentDefinitionBuilder() {
            this.stages = new ArrayList<>();
        }

        /**
         * Create a new {@link SegmentDefinitionBuilder}.
         *
         * @param stages The stages added so far.
         */
        private SegmentDefinitionBuilder(@Nonnull final List<Stage> stages) {
            this.stages = Objects.requireNonNull(stages);
        }

        /**
         * Add a stage to the list of stages. Using generics to enforce that the input type of the
         * added stage is equal to the output type of the stage added by the previous call to
         * {@link SegmentDefinitionBuilder#addStage(Stage)}.
         *
         * @param stage The stage to add.
         * @param <N> The input type of the next stage. Should align with the output type of the
         *            most recently added stage.
         * @return The {@link SegmentDefinitionBuilder}, for method chaining.
         */
        @Nonnull
        public <N> SegmentDefinitionBuilder<I, N, C> addStage(@Nonnull final Stage<O, N, C> stage) {
            Objects.requireNonNull(stage);
            stages.add(stage);
            // Cast to reinterpret the generic variables. This is safe at runtime
            // because generic parameters don't affect runtime :)
            return new SegmentDefinitionBuilder<>(stages);
        }

        /**
         * Add the final stage to the pipeline. The output of the final stage should be the same
         * type as the output of the pipeline.
         *
         * @param stage The stage.
         * @param <N> The output type of the final stage of this segment. Note that this is also the output
         *            type of the segment overall.
         * @return The full {@link PipelineDefinition}.
         * @throws PipelineContextMemberException if the pipeline stage dependencies have been misconfigured.
         */
        public <N> SegmentDefinition<I, N, C> finalStage(@Nonnull final Stage<O, N, C> stage)
            throws PipelineContextMemberException {
            final SegmentDefinitionBuilder<I, N, C> finalBuilder = addStage(stage);
            return new SegmentDefinition<>(finalBuilder.stages);
        }
    }
}
