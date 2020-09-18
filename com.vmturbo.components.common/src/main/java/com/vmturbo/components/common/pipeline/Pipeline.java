package com.vmturbo.components.common.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.components.common.pipeline.Stage.ContextMemberSummary;
import com.vmturbo.components.common.pipeline.Stage.SupplyToContext;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link Pipeline} consists of a set of {@link Stage}s. The output of one stage becomes the input
 * to the next {@link Stage}. There is some (minimal) shared state between the stages, represented
 * by the {@link PipelineContext}. The stages are executed one at a time. In the future
 * we can add parallel execution of certain subsets of the pipeline, but that's not necessary
 * at the time of this writing (Nov 2017).
 *
 * @param <I> The input to the pipeline. This is the input to the first stage.
 * @param <O> The output of the pipeline. This is the output of the last stage.
 * @param <C> The {@link PipelineContext} for the pipeline.
 * @param <S> The {@link PipelineSummary} for the pipeline.
 */
public abstract class Pipeline<I, O, C extends PipelineContext, S extends PipelineSummary> {

    private static final int MAX_STAGE_RETRIES = 3;

    private static final long MAX_STAGE_RETRY_INTERVAL_MS = 30_000;

    private static final Logger logger = LogManager.getLogger();

    /**
     * The fixed name for pipeline initialization prior to any stage being run.
     */
    public static final String INITIAL_STAGE_NAME = "INITIAL";

    private final List<Stage> stages;

    private final C context;

    private final S pipelineSummary;

    /**
     * Suppliers for initial pipeline context members. The supplied context members are set
     * on the context prior to the execution of any stage when the pipeline is run.
     */
    private final List<SupplyToContext<?>> initialContextMemberSuppliers;

    protected Pipeline(@Nonnull final PipelineDefinition<I, O, C> stages,
                       @Nonnull final S pipelineSummary) {
        this.stages = stages.getStages();
        this.context = stages.getContext();
        this.pipelineSummary = pipelineSummary;
        this.initialContextMemberSuppliers = stages.getInitialContextMemberSuppliers();
    }

    public C getContext() {
        return context;
    }

    protected abstract DataMetricTimer startPipelineTimer();

    protected abstract DataMetricTimer startStageTimer(String stageName);

    protected abstract TracingScope startPipelineTrace();

    protected abstract TracingScope startStageTrace(String stageName);

    /**
     * Run the stages of the pipeline, in the order they're configured.
     *
     * @param input The input to the pipeline.
     * @return The output of the pipeline.
     * @throws PipelineException If a required stage fails.
     * @throws InterruptedException      If the pipeline is interrupted.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public O run(@Nonnull I input)
        throws PipelineException, InterruptedException {
        logger.info("Running pipeline: {}", context.getPipelineName());
        pipelineSummary.start();
        Object curStageInput = input;
        try (DataMetricTimer pipelineTimer = startPipelineTimer();
            TracingScope pipelineScope = startPipelineTrace()) {
            initialContextMemberSuppliers.forEach(supplier -> supplier.runSupplier(context));

            for (final Stage stage : stages) {
                String stageName = getSnakeCaseName(stage);
                try (DataMetricTimer stageTimer = startStageTimer(stageName);
                     TracingScope stageScope = startStageTrace(stageName)) {
                    // TODO (roman, Nov 13) OM-27195: Consider refactoring the builder and pipeline
                    // into more of a linked-list format so that there is better type safety.
                    logger.info("Executing stage {}", stage.getClass().getSimpleName());
                    final StageResult result = stage.execute(curStageInput);
                    result.getStatus().setContextMemberSummary(stage.contextMemberCleanup(context));
                    curStageInput = result.getResult();
                    pipelineSummary.endStage(result.getStatus());
                } catch (PipelineStageException | RuntimeException e) {
                    final String message = "Topology pipeline failed at stage "
                        + stage.getClass().getSimpleName() + " with error: " + e.getMessage();
                    pipelineSummary.fail(message);
                    logger.info("\n{}", pipelineSummary);
                    throw new PipelineException(message, e);
                }
            }
        }
        logger.info("\n{}", pipelineSummary);
        logger.info("Pipeline: {} finished successfully.", context.getPipelineName());
        return (O)curStageInput;
    }

    /**
     * Generate a simple tabular description summarizing the pipeline stages with their
     * inputs, outputs, and context member provides, requires, and drops. This is useful
     * to understand the dataflow within the pipeline at a glance.
     *
     * @param pipelineTitle The title of the pipeline (ie "Live TopologyPipeline") etc.
     * @return An ASCII tabular description of the dataflow of the pipeline suitable for printing to the logs.
     */
    public String tabularDescription(@Nonnull final String pipelineTitle) {
        return PipelineTabularDescription.tabularDescription(this, pipelineTitle);
    }

    /**
     * Get the initial context member suppliers.
     *
     * @return the initial context member suppliers.
     */
    List<SupplyToContext<?>> getInitialContextMemberSuppliers() {
        return initialContextMemberSuppliers;
    }

    /**
     * Get the pipeline stages.
     *
     * @return the pipeline stages.
     */
    List<Stage> getStages() {
        return stages;
    }

    private static String getSnakeCaseName(@Nonnull final Stage stage) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
                stage.getClass().getSimpleName());
    }

    /**
     * A passthrough stage is a pipeline stage where the type of input and output is the same.
     * If the input is mutable, it may change during the stage. The stage may also not change
     * the input, but perform some operations based on the input (e.g. recording the input somewhere).
     *
     * @param <T> The type of the input.
     * @param <C> The {@link PipelineContext} type of the pipeline.
     */
    public abstract static class PassthroughStage<T, C extends PipelineContext> extends Stage<T, T, C> {
        /**
         * Is this stage required?
         *
         * @return whether or not the stage is required. If a required stage fails, the pipeline
         * fails. If a non-required stage fails, the next stage gets the input of the passthrough
         * stage.
         */
        public boolean required() {
            return false;
        }

        /**
         * If the stage should be re-tried after a particular exception,
         * get the number of ms to wait before retrying the stage.
         *
         * <p/>Note - if the stage is to be retried, it is the stage writer's responsibility to
         * make sure it's idemnpotent (i.e. re-running the stage should be safe).
         *
         * @param numAttempts The number of attempts so far. Starts at 1.
         * @param e The exception encountered on this attempt.
         * @return An {@link Optional} containing the ms to wait before retrying. Empty optional if
         *         no retry.
         */
        @Nonnull
        public Optional<Long> getRetryIntervalMs(final int numAttempts, final Exception e) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public StageResult<T> executeStage(@Nonnull T input)
                throws PipelineStageException, InterruptedException {
            Status status = null;
            boolean retry = false;
            int numAttempts = 0;
            Exception terminatingException = null;
            do {
                retry = false;
                numAttempts++;
                try {
                    status = passthrough(input);
                    break;
                } catch (PipelineStageException | RuntimeException e) {
                    // If the stage configured some kind of retry behaviour, we should respect
                    // it (unless it's been retrying too much!).
                    final Optional<Long> retryIntervalOpt = getRetryIntervalMs(numAttempts, e);
                    if (numAttempts <= MAX_STAGE_RETRIES && retryIntervalOpt.isPresent()) {
                        retry = true;
                        final long retryDelayMs =
                            Math.max(0, Math.min(retryIntervalOpt.get(), MAX_STAGE_RETRY_INTERVAL_MS));
                        logger.warn("Pipeline stage {} failed with transient error. "
                            + "Retrying after {}ms. Error: {}",
                            getClass().getSimpleName(), retryDelayMs, e.getMessage());
                        try {
                            Thread.sleep(retryDelayMs);
                        } catch (InterruptedException e1) {
                            terminatingException = e1;
                        }
                    } else {
                        terminatingException = e;
                    }
                }
            } while (retry);

            // We should terminate with either an exception or a status.
            Preconditions.checkArgument(terminatingException != null || status != null);

            if (terminatingException != null) {
                if (terminatingException instanceof PipelineStageException) {
                    if (!required()) {
                        status = Status.failed("Error: " + terminatingException.getLocalizedMessage());
                        logger.warn("Non-required pipeline stage {} failed with error: {}",
                            getClass().getSimpleName(), terminatingException.getMessage());
                    } else {
                        throw (PipelineStageException)terminatingException;
                    }
                } else {
                    if (!required()) {
                        status = Status.failed("Runtime Error: " + terminatingException.getLocalizedMessage());
                        String stageName = getClass().getSimpleName();
                        if (terminatingException instanceof StatusRuntimeException) {
                            // we don't want to print the entire stack trace when it is a grpc exception
                            // as it will pollute the logs with unnecessary details.
                            logger.warn("Non-required pipeline stage {} failed with grpcError: {}",
                                stageName, terminatingException.getMessage());
                        } else {
                            logger.warn("Non-required pipeline stage {} failed with runtime Error: {}",
                                stageName, terminatingException.getMessage(), terminatingException);
                        }
                    } else {
                        throw new PipelineStageException(terminatingException);
                    }
                }
            }

            return StageResult.withResult(input)
                    .andStatus(status);
        }

        /**
         * This is the method that implementing classes should override. This method will get called
         * with the stage input at the appropriate place in the pipeline.
         *
         * @param input The stage input.
         * @return A {@link Status} object giving a short summary of the stage result.
         * @throws PipelineStageException If there is an error running the stage.
         * @throws InterruptedException If interrupted waiting during some operation during the stage.
         */
        @Nonnull
        public abstract Status passthrough(T input) throws PipelineStageException, InterruptedException;
    }

    /**
     * The output of a {@link Stage}. Essentially a semantically meaningful pair.
     *
     * @param <T> The output type of the associated {@link Stage}.
     */
    public static class StageResult<T> {

        private final T result;

        private final Status status;

        /**
         * Use {@link StageResult#withResult(Object)}.
         *
         * @param status The {@link Status} of stage execution.
         * @param result The output of the stage.
         */
        private StageResult(@Nonnull final Status status, @Nonnull final T result) {
            this.status = Objects.requireNonNull(status);
            this.result = Objects.requireNonNull(result);
        }

        @Nonnull
        public T getResult() {
            return result;
        }

        @Nonnull
        public Status getStatus() {
            return status;
        }

        /**
         * Factory method to start the construction of a {@link StageResult}.
         *
         * @param result The result of running the {@link Stage}.
         * @param <T> See {@link StageResult}.
         * @return A {@link Builder} to complete the construction.
         */
        public static <T> Builder<T> withResult(@Nonnull final T result) {
            return new Builder<>(result);
        }

        /**
         *  Builder for the {@link StageResult} to allow for more user-friendly construction.
         *
         * @param <R> The result type.
         */
        public static class Builder<R> {
            private final R result;

            private Builder(@Nonnull final R result) {
                this.result = result;
            }

            /**
             * Complete the construction of a {@link StageResult} by setting the status the stage
             * completed with.
             *
             * @param status The status.
             * @return The {@link StageResult}.
             */
            @Nonnull
            public StageResult<R> andStatus(@Nonnull final Status status) {
                return new StageResult<>(status, result);
            }
        }
    }

    /**
     * The status of a completed pipeline stage.
     */
    public static class Status {

        /**
         * The type of the status.
         */
        public enum Type {
            /**
             * Succeeded - the stage did what it's supposed to do with no major hiccups.
             */
            SUCCEEDED,

            /**
             * Warning - the stage encountered some errors, but the errors were not fatal in
             * the sense that there was no significant impact on the resulting topology or
             * the system.
             *
             * <p/>For example, if there are no discovered groups and we fail to upload discovered
             * groups due to a connection error, that might be a WARNING status instead of
             * FAILED.
             */
            WARNING,

            /**
             * Failed - the stage crashed and burned miserably. There should be no FAILED
             * statuses in the pipeline.
             *
             * <p/>This status means that the stage failed in a way that would have a non-trivial impact
             * on the resulting topology.
             */
            FAILED;
        }

        private Type type;

        private String message;

        /**
         * Summary of context members provided, required, and dropped by a stage.
         */
        private String contextMemberSummary;

        private Status(@Nonnull final String message,
                       final Type type) {
            this.type = type;
            this.message = message;
        }

        /**
         * Return the stage completion message.
         *
         * @return The message the stage completed with. May be empty if the stage completed
         *         successfully with no message.
         */
        @Nonnull
        public String getMessage() {
            return message;
        }

        /**
         * Return the stage message with summary. The summary includes information about
         * context member usage by the stage.
         *
         * @return The message the stage completed with. May be empty if the stage completed
         *         successfully with no message.
         */
        @Nonnull
        public String getMessageWithSummary() {
            if (Strings.isNullOrEmpty(contextMemberSummary)) {
                return "\t" + message;
            } else {
                final String msg = message.endsWith("\n") ? message : (message + "\n");
                return msg + contextMemberSummary;
            }
        }

        /**
         * Get the status type.
         *
         * @return the status type.
         */
        public Type getType() {
            return type;
        }

        /**
         * The stage failed. See {@link Type#FAILED}.
         *
         * @param message The message to display to summarize the failure.
         * @return The failure status.
         */
        public static Status failed(@Nonnull final String message) {
            return new Status(message, Type.FAILED);
        }

        /**
         * The stage succeeded. When possible, use {@link Status#success(String)} instead to
         * communicate useful information about what happened during the stage
         * (e.g. modified 5 entities).
         *
         * @return The successful status.
         */
        public static Status success() {
            return new Status("", Type.SUCCEEDED);
        }

        /**
         * The stage succeeded.
         *
         * @param message A message describing what happened during the stage. This could be
         *                purely informative, or contain relatively harmless errors that occurred
         *                during the process. Use {@link Status#withWarnings(String)} if a stage
         *                completed with serious warnings/errors.
         * @return The successful status.
         */
        public static Status success(@Nonnull final String message) {
            return new Status(message, Type.SUCCEEDED);
        }

        /**
         * The stage succeeded - i.e. it didn't fail - but there were serious issues during the
         * stage execution that may dramatically affect the result of the stage or the result
         * of the pipeline.
         *
         * @param message A summary of the warning. This should be no longer than a few lines.
         * @return The warning status.
         */
        public static Status withWarnings(@Nonnull final String message) {
            return new Status(message, Type.WARNING);
        }

        /**
         * Get the summary of context member usage by the stage.
         *
         * @return the summary of context member usage by the stage.
         */
        public String getContextMemberSummary() {
            return contextMemberSummary;
        }

        /**
         * Set the unprovided context members for the stage associated with this status.
         *
         * @param summary the summary of context member usage by the stage.
         */
        void setContextMemberSummary(@Nonnull final ContextMemberSummary summary) {
            this.contextMemberSummary = summary.toString();
        }
    }

    /**
     * An exception indicating that a {@link Pipeline} failed (due to a failure in
     * a required stage).
     */
    public static class PipelineException extends Exception {
        /**
         * Construct a new exception.
         * @param error The error message.
         * @param cause The cause for the error.
         */
        public PipelineException(@Nonnull final String error, @Nonnull final Throwable cause) {
            super(error, cause);
        }
    }

    /**
     * An exception thrown when a stage of the pipeline fails.
     */
    public static class PipelineStageException extends Exception {
        /**
         * Construct a new exception.
         * @param cause The cause for the error.
         */
        public PipelineStageException(@Nonnull final Throwable cause) {
            super(cause);
        }

        /**
         * Construct a new exception.
         * @param error The error message.
         */
        public PipelineStageException(@Nonnull final String error) {
            super(error);
        }

        /**
         * Construct a new exception.
         * @param error The error message.
         * @param cause The cause for the error.
         */
        public PipelineStageException(@Nonnull final String error, @Nullable final Throwable cause) {
            super(error, cause);
        }
    }

    /**
     * An exception that indicates an error in pipeline dependencies/configuration.
     * Made to be a runtime exception because it indicates an unrecoverable mis-configuration
     * in the pipeline that we want to go uncaught and blow everything up. Should only happen
     * in development when introducing new pipeline stages or modifying stage dependencies.
     */
    public static class PipelineContextMemberException extends RuntimeException {
        /**
         * Create a new {@link PipelineContextMemberException}.
         *
         * @param error A description of cause of the exception.
         */
        public PipelineContextMemberException(@Nonnull final String error) {
            super(error);
        }
    }

    /**
     * The definition/spec for a {@link Pipeline}.
     *
     * @param <I3> The initial input type. This will be the input type to the entire
     *                        pipeline.
     * @param <O3> The output of the pipeline.
     * @param <C2> The {@link PipelineContext} for the pipeline we are building.
     */
    public static class PipelineDefinition<I3, O3, C2 extends PipelineContext> {

        private final List<Stage> stages;

        private final C2 context;

        private final List<SupplyToContext<?>> initialContextMemberSuppliers;

        /**
         * The constructor is private. Use the builder.
         *
         * @param stages The stages.
         * @param context The context.
         * @param initialContextMemberSuppliers Suppliers for ContextMembers initially supplied directly
         *                                      by the pipeline rather than by a particular stage. These
         *                                      suppliers are run prior to the execution of any stages.
         */
        private PipelineDefinition(@Nonnull final List<Stage> stages,
                                   @Nonnull final C2 context,
                                   @Nonnull final List<SupplyToContext<?>> initialContextMemberSuppliers) {
            this.stages = stages;
            this.context = context;
            this.initialContextMemberSuppliers = initialContextMemberSuppliers;
        }

        @Nonnull
        public List<Stage> getStages() {
            return Collections.unmodifiableList(stages);
        }

        @Nonnull
        public C2 getContext() {
            return context;
        }

        @Nonnull
        public List<SupplyToContext<?>> getInitialContextMemberSuppliers() {
            return initialContextMemberSuppliers;
        }

        /**
         * Create a new {@link PipelineDefinitionBuilder} for the pipeline.
         *
         * @param context The {@link PipelineContext}.
         * @param <I> The initial input type. This will be the input type to the entire
         *                        pipeline.
         * @param <O> The output of the pipeline.
         * @param <C> The {@link PipelineContext}.
         * @return The {@link PipelineDefinitionBuilder} to use to assemble the stages.
         */
        public static <I, O, C extends PipelineContext> PipelineDefinitionBuilder<I, O, I, C> newBuilder(C context) {
            return new PipelineDefinitionBuilder<>(context);
        }
    }

    /**
     * A builder class to link {@link Pipeline} stages together in a compile-time-safe way.
     *
     * <p/>NOTE: We are using a separate stage builder instead of a "PipelineBuilder" with a
     * build method so that it's easier to work with in individual pipeline implementations without
     * adding additional generic parameters.
     *
     * <p/>Pipeline implementations can create their own "PipelineBuilder" class, which contains a
     * {@link PipelineDefinitionBuilder} to help assemble the stages, or use the {@link PipelineDefinitionBuilder} directly.
     *
     * @param <I> The initial input type. This will be the input type to the entire
     *                        pipeline.
     * @param <O> The output of the pipeline.
     * @param <N> The input type required for the next stage in the pipeline. This
     *                   changes when the {@link PipelineDefinitionBuilder#addStage(Stage)} method is called in order
     *                   to check that the pipeline stages are compatible at compile time.
     * @param <C> The {@link PipelineContext} for the pipeline we are building.
     */
    public static class PipelineDefinitionBuilder<I, O, N, C extends PipelineContext> {

        private final List<Stage> stages;

        private final C context;

        private final List<SupplyToContext<?>> initialContextMemberSuppliers;

        /**
         * Create a new {@link PipelineDefinitionBuilder}.
         *
         * @param context The {@link PipelineContext}.
         */
        private PipelineDefinitionBuilder(@Nonnull final C context) {
            this.stages = new LinkedList<>();
            this.context = Objects.requireNonNull(context);
            this.initialContextMemberSuppliers = new ArrayList();
        }

        /**
         * Add a stage to the list of stages. Using generics to enforce that the input type of the
         * added stage is equal to the output type of the stage added by the previous call to
         * {@link PipelineDefinitionBuilder#addStage(Stage)}.
         *
         * @param stage The stage to add.
         * @param <X> The input type of the next stage. Should align with the output type of the
         *            most recently added stage.
         * @return The {@link PipelineDefinitionBuilder}, for method chaining.
         */
        @Nonnull
        public <X> PipelineDefinitionBuilder<I, O, X, C> addStage(
                @Nonnull final Stage<N, X, C> stage) {
            Objects.requireNonNull(stage);
            stage.setContext(context);
            stages.add(stage);
            // Cast to reinterpret the generic variables. This is safe at runtime
            // because generic parameters don't affect runtime :)
            return (PipelineDefinitionBuilder<I, O, X, C>)this;
        }

        /**
         * Add a context member to the initial state of the PipelineContext. This is appropriate
         * when the context member is initially lightweight and may be populated with data later
         * so having it initially present is not very expensive. It is useful in situations when
         * there are different pipeline configurations that may have conflicting requirements
         * about which stage should be the initial provider of the member. For example, in the
         * LIVE Topology pipeline the PolicyStage is the first to require the GroupResolver member
         * and might normally provide it to the pipeline, but in the PLAN TopologyPipeline, the
         * ScopeResolutionStage requires it before the PolicyStage runs, so it makes more sense
         * to just have an empty GroupResolver initially in the context.
         * <p/>
         * Any context members initially provided and not required by any stages are dropped and
         * never executed when the pipeline is run.
         *
         * @param memberDefinition The definition of the context member to add.
         * @param supplier A supplier for the member associated with the definition. When the pipeline
         *                 is run all initial context members will be added to the context prior to the
         *                 execution of any stage.
         * @param <M> The data type of the member data.
         * @return The {@link PipelineDefinitionBuilder}, for method chaining.
         */
        @Nonnull
        public <M> PipelineDefinitionBuilder<I, O, N, C> initialContextMember(
            @Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
            @Nonnull final Supplier<M> supplier) {
            initialContextMemberSuppliers.add(new SupplyToContext<>(memberDefinition, supplier));
            return this;
        }

        /**
         * Add the final stage to the pipeline. The output of the final stage should be the same
         * type as the output of the pipeline.
         *
         * @param stage The stage.
         * @return The full {@link PipelineDefinition}.
         * @throws PipelineContextMemberException if the pipeline stage dependencies have been misconfigured.
         */
        public PipelineDefinition<I, O, C> finalStage(@Nonnull final Stage<N, O, C> stage)
            throws PipelineContextMemberException {
            addStage(stage);
            final List<SupplyToContext<?>> contextMemberSuppliers = configureStageContextMembers();
            return new PipelineDefinition<>(stages, context, contextMemberSuppliers);
        }

        /**
         * Configures the ContextMember drop behavior for the pipeline's stages.
         * While doing this, we ensure:
         * <p/>
         * 1. Every ContextMember has a unique provider.
         * 2. Every ContextMember requirement is provided by an earlier stage or has a default.
         * <p/>
         * Note that it's fine to provide an unused ContextMember. The pipeline will drop any
         * provided ContextMembers as soon as there are no longer needed by any downstream stages that
         * require them, so when a stage provides a ContextMember that no one requires, it will
         * be immediately dropped on completion of that stage.
         *
         * @return A list of the context member suppliers to run before executing any stages.
         * @throws PipelineContextMemberException If the pipeline violates one or both of the above invariants.
         */
        @Nonnull
        private List<SupplyToContext<?>> configureStageContextMembers()
            throws PipelineContextMemberException {
            final Map<PipelineContextMemberDefinition<?>, String> providedDependencies = new HashMap<>();
            final Map<PipelineContextMemberDefinition<?>, Stage<?, ?, ?>> requirements = new HashMap<>();
            initialContextMemberSuppliers.forEach(def ->
                providedDependencies.put(def.getMemberDefinition(), INITIAL_STAGE_NAME));

            for (final Stage<?, ?, ?> stage : stages) {
                // Ensure requirements are provided by an earlier stage
                if (!providedDependencies.keySet().containsAll(stage.getContextMemberRequirements())) {
                    final List<PipelineContextMemberDefinition<?>> missingMembers =
                        Sets.difference(new HashSet<>(stage.getContextMemberRequirements()),
                            providedDependencies.keySet()).stream()
                        .filter(memberDef -> !memberDef.suppliesDefault())
                        .collect(Collectors.toList());
                    if (!missingMembers.isEmpty()) {
                        throw new PipelineContextMemberException(String.format(
                            "No earlier stage provides required pipeline dependencies (%s) without default for stage %s.",
                            missingMembers.stream()
                                .map(PipelineContextMemberDefinition::toString)
                                .collect(Collectors.joining(", ")),
                            stage.getName()));
                    }
                }
                stage.getContextMemberRequirements().forEach(requirement ->
                    requirements.put(requirement, stage));

                // Ensure unique provider for every dependency
                for (final PipelineContextMemberDefinition<?> dependency : stage.getProvidedContextMembers()) {
                    final String firstProviderName = providedDependencies.get(dependency);
                    if (firstProviderName == null) {
                        providedDependencies.put(dependency, stage.getName());
                    } else {
                        throw new PipelineContextMemberException(String.format(
                            "Pipeline ContextMember of type %s provided by stage %s is already "
                                + "provided by earlier stage %s. Only one stage in the pipeline "
                                + "may provide a particular ContextMember to the pipeline context.", dependency,
                            stage.getName(), firstProviderName));
                    }
                    // If no one requires, we drop right after the initial dependency.
                    requirements.put(dependency, stage);
                }
            }

            // The requirements map should have the last stage in the pipeline that requires a
            // particular dependency.
            requirements.entrySet().stream()
                .collect(Collectors.groupingBy(entry -> entry.getValue()))
                .forEach((stage, entries) -> stage.setDependenciesToDrop(entries.stream()
                    .map(Entry::getKey)
                    .collect(Collectors.toList())));

            // Now find the initial context members to provide. Log as a warning any context
            // members that are initially provided but never required. These suppliers will be
            // dropped here.
            final Map<Boolean, List<SupplyToContext<?>>> suppliersByRequirement = initialContextMemberSuppliers.stream()
                .collect(Collectors.groupingBy(member -> requirements.containsKey(member.getMemberDefinition())));
            final List<SupplyToContext<?>> unnecessary =
                suppliersByRequirement.getOrDefault(false, Collections.emptyList());
            if (unnecessary.size() > 0) {
                logger.warn("Dropping unnecessary initial context members: " + unnecessary);
            }

            return suppliersByRequirement.getOrDefault(true, Collections.emptyList());
        }
    }
}
