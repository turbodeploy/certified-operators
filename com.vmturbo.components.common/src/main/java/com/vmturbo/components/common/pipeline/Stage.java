package com.vmturbo.components.common.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

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

    // Dependencies we can drop from the pipeline context after the stage completes.
    // Populated by the pipeline builder when the stages are all assembled.
    private List<PipelineContextMemberDefinition<?>> dependenciesToDrop = Collections.emptyList();

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

    @VisibleForTesting
    public void setContext(@Nonnull final C2 context) {
        this.context = Objects.requireNonNull(context);
    }

    @Nonnull
    public C2 getContext() {
        return context;
    }

    @Nonnull
    public String getName() {
        return getClass().getSimpleName();
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
    public List<PipelineContextMemberDefinition<?>> getProvidedContextMembers() {
        return providesToPipelineContext.stream()
            .map(supplyToContext -> supplyToContext.memberDefinition)
            .collect(Collectors.toList());
    }

    @Nonnull
    public List<PipelineContextMemberDefinition<?>> getContextMemberRequirements() {
        return pipelineContextRequirements.stream()
            .map(supplyFromContext -> supplyFromContext.memberDefinition)
            .collect(Collectors.toList());
    }

    @Nonnull
    @VisibleForTesting
    List<PipelineContextMemberDefinition<?>> getContextMembersToDrop() {
        return dependenciesToDrop;
    }

    void setDependenciesToDrop(@Nonnull final List<PipelineContextMemberDefinition<?>> dependenciesToDrop) {
        this.dependenciesToDrop = Objects.requireNonNull(dependenciesToDrop);
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
        final ContextMemberSummary summary = new ContextMemberSummary(this, context);

        // Drop any context members no longer needed.
        dependenciesToDrop.forEach(context::dropMember);

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
                sb.append("\t").append(providesSummary).append("\n");
            }
            if (hasRequires) {
                sb.append("\t").append(requiresSummary).append("\n");
            }
            if (hasDrops) {
                sb.append("\t").append(dropsSummary).append("\n");
            }
            return sb.toString();
        }
    }
}
