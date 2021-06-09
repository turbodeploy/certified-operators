package com.vmturbo.components.common.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;

/**
 * A pipeline stage takes an input and produces an output that gets passed along to the
 * next stage.
 *
 * @param <I2> The type of the input.
 * @param <O2> The type of the output.
 * @param <C2> The {@link PipelineContext} of the pipeline to which this stage belongs.
 */
public abstract class Stage<I2, O2, C2 extends PipelineContext> {

    private C2 context;
    private List<SupplyToContext<?>> providesToPipelineContext = new ArrayList<>();
    private List<SupplyFromContext<?>> pipelineContextRequirements = new ArrayList<>();

    // Context members we can drop from the pipeline context after the stage completes.
    // Populated by the pipeline builder when the stages are all assembled.
    private List<PipelineContextMemberDefinition<?>> membersToDrop = Collections.emptyList();

    /**
     * Execute the stage.
     *
     * @param input The input.
     * @return The output of the stage.
     * @throws PipelineStageException If there is an error executing this stage.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    public StageResult<O2> execute(@Nonnull I2 input)
        throws PipelineStageException, InterruptedException {
        // Execute context member consumers prior to executing the stage.
        pipelineContextRequirements.forEach(consumer -> consumer.runConsumer(context, getName()));

        // Perform the actual stage execution.
        final StageResult<O2> result = executeStage(input);

        // Provide all context members that the stage declared.
        providesToPipelineContext.forEach(provider -> provider.runSupplier(context));

        return result;
    }

    /**
     * Execute the stage.
     *
     * @param input The input.
     * @return The output of the stage.
     * @throws PipelineStageException If there is an error executing this stage.
     * @throws InterruptedException If the stage is interrupted.
     */
    @Nonnull
    protected abstract StageResult<O2> executeStage(@Nonnull I2 input)
        throws PipelineStageException, InterruptedException;

    /**
     * Set the context for use when the stage is executed.
     *
     * @param context the context for use when the stage is executed.
     */
    @VisibleForTesting
    public void setContext(@Nonnull final C2 context) {
        this.context = Objects.requireNonNull(context);
    }

    /**
     * Get the context used with this stage. Note that the stage's context is not set until it is added
     * to the pipeline (or if the stage is added to a segment, the context is not added until the segment
     * is added to the main pipeline).
     *
     * @return the context used with this stage.
     */
    @Nonnull
    public C2 getContext() {
        return context;
    }

    /**
     * Get the name of the stage. This name is logged when printing performance metrics, populating
     * prometheus metrics, or populating tracing information.
     *
     * @return The name of the stage.
     */
    @Nonnull
    public String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Get the name of the stage in snake case.
     *
     * @return the name of the stage in snake case.
     */
    @Nonnull
    public String getSnakeCaseName() {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, getName());
    }

    /**
     * Declare a PipelineContextMemberDefinition that the stage requires. The
     * member value on the returned {@link FromContext} will be set to the
     * value of the associated member immediately prior to when the stage's
     * {@link #executeStage(Object)} method is called.
     * <p/>
     * This method cannot be overriden by subclasses.
     *
     * @param memberDefinition The definition of the member.
     *
     * @param <M> The data type of the member.
     * @return A reference to the stage to support method chaining.
     */
    protected final <M> FromContext<M>
    requiresFromContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition) {
        final FromContext<M> fromContext = new FromContext<>(memberDefinition,
            getClass().getSimpleName());
        pipelineContextRequirements.add(new SupplyFromContext<>(memberDefinition,
            member -> fromContext.member = member));
        return fromContext;
    }

    /**
     * Declare a PipelineContextMemberDefinition that the stage provides. The
     * provided member will be set on the context immediately after the stage's
     * {@link #executeStage(Object)} method is called.
     * <p/>
     * This method cannot be overriden by subclasses.
     *
     * @param memberDefinition The definition of the member.
     * @param member The ContextMember associated with the definition to be set on the context
     *               immediately after the stage completes its {@link Stage#executeStage(Object)}.
     * @param <M> The data type of the member.
     * @return A reference to the stage to support method chaining.
     */
    protected final <M> M providesToContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                                            @Nonnull final M member) {
        providesToContext(memberDefinition, (Supplier<M>)(() -> member));
        return Objects.requireNonNull(member);
    }

    /**
     * Declare a PipelineContextMemberDefinition that the stage provides. The
     * provided member supplier run and the returned value will be set on the context
     * immediately after the stage's {@link #executeStage(Object)} method is called.
     * <p/>
     * This method cannot be overriden by subclasses.
     *
     * @param memberDefinition The definition of the member.
     * @param supplier A supplier for the ContextMember to set on the context.
     *
     * @param <M> The data type of the member.
     */
    protected final <M> void providesToContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                                               @Nonnull final Supplier<M> supplier) {
        providesToPipelineContext.add(new SupplyToContext<>(memberDefinition, supplier));
    }

    @Nonnull
    protected List<PipelineContextMemberDefinition<?>> getProvidedContextMembers() {
        return providesToPipelineContext.stream()
            .map(supplyToContext -> supplyToContext.memberDefinition)
            .collect(Collectors.toList());
    }

    @Nonnull
    protected List<PipelineContextMemberDefinition<?>> getContextMemberRequirements() {
        return pipelineContextRequirements.stream()
            .map(supplyFromContext -> supplyFromContext.memberDefinition)
            .collect(Collectors.toList());
    }

    @Nonnull
    @VisibleForTesting
    List<PipelineContextMemberDefinition<?>> getContextMembersToDrop() {
        return membersToDrop;
    }

    /**
     * Configures the ContextMembers for this stage in the parent pipelines stage ordering. The
     * stage must ensure that:
     * <p/>
     * 1. Every context member this stage provided already exists in the map of provided members. This
     *    ensures that the members required by this stage can actually be provided by the context
     * 2. None of the members it provides is already provided by an earlier stage. This ensures
     *    a unique provider for each context member so we don't have to worry about stages accidentally
     *    overwriting the context members provided by earlier stages.
     * <p/>
     * Note that when the stage is executed, the execution proceeds according to the following flow:
     * <ol>
     *     <li>Fetch required dependencies from the context</li>
     *     <li>Execute the stage</li>
     *     <li>Populate provides dependencies onto the context</li>
     * </ol>
     * and this method is called at build time to ensure that all stages have will have their required
     * context members provided when the pipeline is actually run.
     * <p/>
     * The stage should add each of its requirements into the required map.
     * The stage should add each of its provided members into both the provided and required maps.
     *
     * @param provided A map of the context members that will be provided to this stage by earlier stages.
     * @param required A map of the context members that are required by earlier stage. The value for the context
     *                 member in the map will be the LAST stage configured that required the stage.
     * @throws PipelineContextMemberException If the above invariants cannot be confirmed.
     */
    void configureContextMembers(@Nonnull final Map<PipelineContextMemberDefinition<?>, String> provided,
                                 @Nonnull final Map<PipelineContextMemberDefinition<?>, Stage<?, ?, ?>> required)
        throws PipelineContextMemberException {
        configureContextMemberRequires(provided, required);
        configureContextMemberProvides(provided, required);
    }

    /**
     * Configure the context members required by this stage. If no earlier stage already provides
     * a context member required by this stage, this method should throw a
     * {@link PipelineContextMemberException}. Note it is fine for multiple stages to require the
     * same context member.
     *
     * @param provided The context members provided by previous stages. The value is the name of the
     *                 unique stage providing the context member.
     * @param required The context members required by previous stages. The value is the LAST stage
     *                 requiring the context member before this one. Used to compute when in the pipeline
     *                 execution it is safe to drop a context member so it can be garbage collected to
     *                 free up memory.
     * @throws PipelineContextMemberException if no prior stage provides a required context member.
     */
    protected void configureContextMemberRequires(@Nonnull Map<PipelineContextMemberDefinition<?>, String> provided,
                                                  @Nonnull Map<PipelineContextMemberDefinition<?>, Stage<?, ?, ?>> required)
        throws PipelineContextMemberException {
        // Ensure requirements are provided by an earlier stage
        if (!provided.keySet().containsAll(getContextMemberRequirements())) {
            final List<PipelineContextMemberDefinition<?>> missingMembers =
                Sets.difference(new HashSet<>(getContextMemberRequirements()),
                    provided.keySet()).stream()
                    .filter(memberDef -> !memberDef.suppliesDefault())
                    .collect(Collectors.toList());
            if (!missingMembers.isEmpty()) {
                throw new PipelineContextMemberException(String.format(
                    "No earlier stage provides required pipeline context "
                        + (missingMembers.size() > 1 ? "members" : "member")
                        + " (%s) without default for stage %s.", missingMembers.stream()
                        .map(PipelineContextMemberDefinition::toString)
                        .collect(Collectors.joining(", ")),
                    getName()));
            }
        }
        getContextMemberRequirements().forEach(requirement -> required.put(requirement, this));
    }

    /**
     * Configure the context members provided by this stage. If another stage already provides a
     * context member that this stage expects to provide, this method should throw a
     * {@link PipelineContextMemberException}. All context members should be uniquely provided
     * to ensure no stage accidentally overwrites a context member.
     *
     * @param provided The context members provided by previous stages. The value is the name of the
     *                 unique stage providing the context member.
     * @param required The context members required by previous stages. The value is the LAST stage
     *                 requiring the context member before this one. Used to compute when in the pipeline
     *                 execution it is safe to drop a context member so it can be garbage collected to
     *                 free up memory.
     * @throws PipelineContextMemberException if some prior stage already provides a provided context member.
     */
    protected void configureContextMemberProvides(@Nonnull Map<PipelineContextMemberDefinition<?>, String> provided,
                                                  @Nonnull Map<PipelineContextMemberDefinition<?>, Stage<?, ?, ?>> required)
        throws PipelineContextMemberException {
        // Ensure unique provider for every context member
        for (final PipelineContextMemberDefinition<?> providedMember : getProvidedContextMembers()) {
            final String firstProviderName = provided.get(providedMember);
            if (firstProviderName == null) {
                provided.put(providedMember, getName());
            } else {
                throw new PipelineContextMemberException(String.format(
                    "Pipeline ContextMember of type %s provided by stage %s is already "
                        + "provided by earlier stage %s. Only one stage in the pipeline "
                        + "may provide a particular ContextMember to the pipeline context.", providedMember,
                    getName(), firstProviderName));
            }
            // If no one requires, we drop right after the member is initially provided.
            required.put(providedMember, this);
        }
    }

    void setMembersToDrop(@Nonnull final List<PipelineContextMemberDefinition<?>> membersToDrop) {
        this.membersToDrop = Objects.requireNonNull(membersToDrop);
    }

    /**
     * Drop any context members no longer needed further down the pipeline.
     * Return a summary of context member activity performed by this stage.
     *
     * @param context The pipeline context.
     * @return The list of context members that the stage declared it would provide but failed to do so.
     *         This list may be logged in the pipeline summary.
     * @throws PipelineContextMemberException when attempting to access a ContextMember not actually on
     *                                     the pipeline context.
     */
    protected ContextMemberSummary contextMemberCleanup(@Nonnull final PipelineContext context)
        throws PipelineContextMemberException {
        // Be sure to generate the summary before dropping members from the context.
        final ContextMemberSummary summary = context.includeContextMemberSummary()
            ? new ContextMemberSummary(this, context)
            : new BlankContextMemberSummary();

        // Drop any context members no longer needed.
        membersToDrop.forEach(context::dropMember);

        return summary;
    }

    /**
     * Wraps a reference to a ContextMember that will be set to a value provided by the PipelineContext.
     *
     * @param <M> The type of the ContextMember.
     */
    public static class FromContext<M> {
        private final PipelineContextMemberDefinition<M> memberDefinition;
        private final String stageClassName;
        private M member;

        private FromContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                            @Nonnull final String stageClassName) {
            this.memberDefinition = Objects.requireNonNull(memberDefinition);
            this.stageClassName = Objects.requireNonNull(stageClassName);
            member = null;
        }

        /**
         * Get the member required from the context. This will only be set just prior to the
         * stage's {@link Stage#executeStage(Object)} method being called.
         *
         * @return the member required from the context.
         */
        public M get() {
            return Objects.requireNonNull(member, () -> "ContextMember \"" + memberDefinition.getName()
                + "\" (FromContext<" + memberDefinition.getMemberClass().getSimpleName() + ">)is not "
                + "set because the PipelineContext has not yet provided its value to this stage of class "
                + stageClassName + ". The value will be populated by the Pipeline just before your stage's "
                + "#executeStage method is called when the pipeline is run. Do not call FromContext#get() "
                + "before then or it will fail (for example, do not call the FromContext#get() method in "
                + "your stage's constructor or any member variable initializers).");
        }

        /**
         * Get the {@link PipelineContextMemberDefinition} associated with the ContextMember
         * to be provided from the context.
         *
         * @return the {@link PipelineContextMemberDefinition} associated with the ContextMember
         *         to be provided from the context.
         */
        public PipelineContextMemberDefinition<M> getDefinition() {
            return memberDefinition;
        }

        /**
         * Initially, the value in a {@link FromContext} is not available on creation. It will be
         * populated automatically by the pipeline just prior to calling a stage's
         * {@link Stage#executeStage(Object)} method. Although it is usually not necessary to
         * check if the value is available (it should always be available in the executeStage
         * method), this can be used to check before calling {@link #get()}.
         *
         * @return Check whether the value is available via the {@link #get()} method yet.
         */
        public boolean isAvailable() {
            return member != null;
        }
    }

    /**
     * A small helper class bundling together a ContextMemberDefinition and a consumer for that
     * ContextMember. The consumer is run prior to the stage that declared a requirement for the member
     * is executed. The contained consumers are used to fill in the values in
     * {@link Stage.FromContext} instances.
     *
     * @param <M> The Java Class of the ContextMember.
     */
    static class SupplyFromContext<M> {
        private final PipelineContextMemberDefinition<M> memberDefinition;
        private final Consumer<M> memberConsumer;

        SupplyFromContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                          @Nonnull final Consumer<M> memberConsumer) {
            this.memberDefinition = Objects.requireNonNull(memberDefinition);
            this.memberConsumer = Objects.requireNonNull(memberConsumer);
        }

        void runConsumer(@Nonnull final PipelineContext pipelineContext,
                         @Nonnull final String stageName) {
            // If no one has provided a member, but a default is available, set the default to the context.
            boolean hasMember = pipelineContext.hasMember(memberDefinition);
            if (!hasMember) {
                if (memberDefinition.suppliesDefault()) {
                    pipelineContext.addMember(memberDefinition, memberDefinition.getDefaultInstance());
                } else {
                    throw new PipelineContextMemberException(getMissingMemberExceptionMessage(stageName));
                }
            }

            final M member = pipelineContext.getMember(memberDefinition);
            Objects.requireNonNull(member, () -> getMissingMemberExceptionMessage(stageName));
            memberConsumer.accept(member);
        }

        private String getMissingMemberExceptionMessage(@Nonnull final String stageName) {
            return "Missing pipeline ContextMember " + memberDefinition + " required by stage " + stageName
                + ". If this is production code (these errors should not happen in production), it indicates "
                + "a failure to properly declare stage ContextMember provides/requires. To avoid these errors"
                + "in production by catching them first in unit tests, please ensure the factory method that "
                + "generated this particular configuration of the pipeline is exercised in tests. If this is a "
                + "unit test, it can indicate either a failure to properly declare stage ContextMember "
                + "provides/requires or forgetting to correctly initialize the pipeline context members. See "
                + "the StagesTest#createStageContext helper functions for examples of how to initialize "
                + "pipeline ContextMembers for stage unit tests.";
        }
    }

    /**
     * A small helper class bundling together a ContextMemberDefinition and a supplier for that
     * ContextMember. The supplier is run just after execution of the stage that declared it
     * provides that member to the pipeline or sometimes during pipeline initialization.
     *
     * @param <M> The Java Class of the ContextMember.
     */
    static class SupplyToContext<M> {
        private final PipelineContextMemberDefinition<M> memberDefinition;
        private final Supplier<M> memberSupplier;

        SupplyToContext(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                        @Nonnull final Supplier<M> memberSupplier) {
            this.memberDefinition = Objects.requireNonNull(memberDefinition);
            this.memberSupplier = Objects.requireNonNull(memberSupplier);
        }

        void runSupplier(@Nonnull final PipelineContext pipelineContext) {
            pipelineContext.addMember(memberDefinition, memberSupplier.get());
        }

        /**
         * Get the {@link PipelineContextMemberDefinition}.
         *
         * @return the {@link PipelineContextMemberDefinition}.
         */
        PipelineContextMemberDefinition<M> getMemberDefinition() {
            return memberDefinition;
        }
    }

    /**
     * A simple summary of the provided, required, and dropped ContextMembers of a stage.
     * This summary will be used to log similar information about the context members
     * accessed by the stage when the pipeline summary is logged.
     */
    public static class ContextMemberSummary {
        private final String providesSummary;
        private final String requiresSummary;
        private final String dropsSummary;

        /**
         * Create a new {@link ContextMemberSummary}.
         * Note that the summary should be generated for the stage before dropping the stage's
         * ContextMembers to drop to ensure they can still be accessed for summarization purposes.
         *
         * @param stage The stage whose context member access should be summarized.
         * @param context The context whose members are being summarized.
         * @throws PipelineContextMemberException if the context does not contain the context members declard
         *                                     on the stage.
         */
        public ContextMemberSummary(@Nonnull final Stage<?, ?, ?> stage,
                                    @Nonnull final PipelineContext context)
            throws PipelineContextMemberException {
            providesSummary = summary(stage.getProvidedContextMembers(), "PROVIDED",
                def -> sizeSummary(def, context));
            requiresSummary = summary(stage.getContextMemberRequirements(), "REQUIRED",
                def -> sizeSummary(def, context));
            dropsSummary = summary(stage.getContextMembersToDrop(), "DROPPED", def -> "");
        }

        /**
         * Construct a {@link ContextMemberSummary} with empty contents.
         */
        protected ContextMemberSummary() {
            this.providesSummary = "";
            this.requiresSummary = "";
            this.dropsSummary = "";
        }

        private String summary(@Nonnull final List<PipelineContextMemberDefinition<?>> memberDefinitions,
                               @Nonnull final String prefix,
                               @Nonnull final Function<PipelineContextMemberDefinition<?>, String> sizeFunction) {
            if (memberDefinitions.isEmpty()) {
                return "";
            }

            return memberDefinitions.stream()
                .map(def -> def.getName() + sizeFunction.apply(def))
                .collect(Collectors.joining(", ", prefix + "=(", ")"));
        }

        private <M> String sizeSummary(@Nonnull final PipelineContextMemberDefinition<M> memberDefinition,
                                       @Nonnull final PipelineContext context) {
            final M member = context.getMember(memberDefinition);
            if (member != null) {
                final String sizeDescription = memberDefinition.sizeDescription(member);
                return Strings.isNullOrEmpty(sizeDescription) ? "" : "[" + sizeDescription + "]";
            } else {
                return "";
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(128);
            boolean hasProvides = !Strings.isNullOrEmpty(providesSummary);
            boolean hasRequires = !Strings.isNullOrEmpty(requiresSummary);
            boolean hasDrops = !Strings.isNullOrEmpty(dropsSummary);

            if (hasProvides || hasRequires || hasDrops) {
                sb.append("PIPELINE_CONTEXT_MEMBERS:\n");
            }
            if (hasProvides) {
                sb.append(PipelineSummary.NESTING_SPACING).append(providesSummary).append("\n");
            }
            if (hasRequires) {
                sb.append(PipelineSummary.NESTING_SPACING).append(requiresSummary).append("\n");
            }
            if (hasDrops) {
                sb.append(PipelineSummary.NESTING_SPACING).append(dropsSummary).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Blank summary for context members. Used when we want to omit the context member summary
     * from the overall pipeline summary.
     */
    public static class BlankContextMemberSummary extends ContextMemberSummary {
        @Override
        public String toString() {
            return "";
        }
    }
}
