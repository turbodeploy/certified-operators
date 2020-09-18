package com.vmturbo.components.common.pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;

/**
 * The {@link PipelineContext} is information that's shared by all stages in a pipeline.
 * Data is passed through stages in a pipeline in two ways:
 * 1. "Functional" data sharing - the output of one stage is provided as the input to the next stage.
 * This is the preferred approach to data sharing and should be used whenever possible. However, if
 * two stages cannot run next to each other in the pipeline but still needed to pass data from the
 * earlier stage to the later stage, data can be put in the PipelineContext.
 * 2. "Context" data sharing - When it is not possible to pass data via the input-output functional
 * model, data can be placed on the context in two ways:
 * 2a. As a variable directly on the context - This approach should be used when a piece of data
 * is useful to ALL or nearly all stages in the pipeline. Examples include ie. the TopologyInfo
 * in the TopologyPipeline (so all stages can know the topologyId and contextId). There are usually
 * not many data that fall into this category. Use sparingly, because these data will reside on
 * the context for the ENTIRE PIPELINE and cannot be garbage collected until the pipeline completes.
 * If the data is not needed for the entire pipeline, it increases peak pipeline memory usage and
 * hurts performance. Furthermore, if the data sits in an empty variable on the pipeline context
 * until some downstream stage eventually populates it, it will be unclear to other stages when
 * the data is actually available and when it's just sitting there empty. For these forms of data
 * that are not needed by every or nearly every stage, prefer the ContextMember form of data sharing.
 * 3a. As an explicit ContextMember - this approach requires a bit more setup, but solves the
 * above problems of ensuring the data can be cleaned up as soon as it is no longer needed and providing
 * clarity around when the data is present and when it is not. There are two steps to use context members:
 *    A: Declare a ContextMember by inheriting from {@link PipelineContextMemberDefinition}.
 *       {@see TopologyPipelineContextMembers} for some examples.
 *    B: Have your stage declare the ContextMembers it provides/requires via
 *       {@link Stage#providesToContext(PipelineContextMemberDefinition, Supplier)} and
 *       {@link Stage#requiresFromContext(PipelineContextMemberDefinition)} methods.
 * <p/>
 * If there are multiple pipeline configurations requiring variance in whether a stage should
 * provide or require a context member, use
 * {@link Pipeline.PipelineDefinitionBuilder#initialContextMember(PipelineContextMemberDefinition, Supplier)}
 * to initialize the member to the context and then all downstream stages can use it and it will still be
 * dropped after the last of these stages runs.
 */
@NotThreadSafe
public abstract class PipelineContext {

    /**
     * Context members.
     */
    private final Map<PipelineContextMemberDefinition<?>, Object> members = new HashMap<>();

    /**
     * Get the name of the pipeline instance. Used mainly for logging/debugging.
     *
     * @return The pipeline name.
     */
    @Nonnull
    public abstract String getPipelineName();

    /**
     * Add a new ContextMember to the {@link PipelineContext}. Adding a new member with the
     * previous for a definition that was already added will overwrite the previous one.
     *
     * @param definition The unique definition of the member.
     * @param contextMember The ContextMember associated with the definition. The member can then be looked up
     *                      through the definition. It is illegal to
     * @param <M> The data type of the member.
     * @throws PipelineContextMemberException If the contextMember is null.
     */
    public <M> void addMember(@Nonnull final PipelineContextMemberDefinition<M> definition,
                              @Nonnull final M contextMember) throws PipelineContextMemberException {
        if (contextMember == null) {
            throw new PipelineContextMemberException("Attempt to add a null ContextMember for definition "
                + definition.toString());
        }
        members.put(definition, contextMember);
    }

    /**
     * Check whether the context contains a particular ContextMember associated with the
     * input {@link PipelineContextMemberDefinition}.
     *
     * @param definition The unique definition describing a context member.
     * @param <M> The data type of the member.
     * @return Whether or not the context currently contains a ContextMember associated with the definition.
     */
    public <M> boolean hasMember(@Nonnull final PipelineContextMemberDefinition<M> definition) {
        return members.containsKey(definition);
    }

    /**
     * Drop a ContextMember (remove it from the context).
     *
     * @param definition The definition of the ContextMember to drop.
     * @param <M> The data type of the context member to drop.
     * @throws PipelineContextMemberException If the context does not have a ContextMember to drop for
     *                                     the definition.
     */
    <M> void dropMember(@Nonnull final PipelineContextMemberDefinition<M> definition)
        throws PipelineContextMemberException {
        if (members.remove(definition) == null) {
            throw new PipelineContextMemberException(String.format(
                "Attempt to drop unavailable PipelineContext member %s. "
                    + "This usually indicates a pipeline misconfiguration in the form of a "
                    + "failure to declare and provide a required dependency",
                definition.toString()));
        }
    }

    /**
     * Get a member from the {@link PipelineContext}. Stages should avoid calling this directly, and
     * instead should declare their dependencies by calling their requires/provides methods which
     * allow them to provide a consumer method for the member. The consumer will run prior to the
     * stage being executed. This consumer can do things like save the member onto the stage for use
     * during execution.
     * <p/>
     * Members can also be initialized on the context by calling the
     * {@link com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder
     * #initialContextMember(PipelineContextMemberDefinition, Object)} method.
     * <p/>
     * This method should generally not be accessed directly. Rather, access members via the
     * provides/requires framework on stages.
     *
     * @param definition The member that should be retrieved from the context.
     * @param <M> The data type of the member.
     * @return The member.
     * @throws PipelineContextMemberException on an attempt to retrieve a member that has not been provided
     *                                     to the context.
     */
    @Nonnull
    <M> M getMember(@Nonnull final PipelineContextMemberDefinition<M> definition) {
        final Class<M> klass = definition.getMemberClass();
        final M member = klass.cast(members.get(definition));
        if (member == null) {
            throw new PipelineContextMemberException("Illegal attempt to acquire member " + definition.toString()
                + " when no prior stage has provided it.");
        }
        return member;
    }

    /**
     * A {@link PipelineContextMemberDefinition} represents a piece of data generated and consumed
     * by pipeline stages. When a pipeline stage generates a piece of data that is directly consumed
     * by the succeeding stage or series of stages, this is best represented as an Output/Input
     * of the stage. However, when one stage generates data that is consumed by some stage
     * much further down the pipeline and is not needed by intervening stages, this is best
     * represented as a context member. The small runtime that manages the pipeline will check
     * that the stage that is supposed to generate the context member does so in order that the
     * downstream stage can consume it. If a declared dependency is not provided, a runtime
     * exception will be thrown potentially halting the pipeline but the API for context members
     * is constructed in a way that makes it difficult to forget to provide a declared member.
     * Furthermore, when the last downstream stage that requires the data finishes, the runtime
     * will automatically drop it from the context so that it can be garbage collected.
     * <p/>
     * Context members are uniquely identified by their name (a String). Only one stage may
     * provide a context member (another stage attempting to overwrite the member will
     * result in an error, although it is fine to mutate a context member added by a
     * previous stage).
     *
     * @param <M> The class of the member.
     */
    public abstract static class PipelineContextMemberDefinition<M> {
        private final Class<M> klass;

        /**
         * Create a new {@link PipelineContextMemberDefinition}.
         *
         * @param klass The Java class of the data to be stored within the context member.
         */
        public PipelineContextMemberDefinition(@Nonnull final Class<M> klass) {
            this.klass = Objects.requireNonNull(klass);
        }

        /**
         * Get the name of the context member. Only a single context member with a given
         * name can be attached to the context at a time.
         *
         * @return The name of the context member.
         */
        @Nonnull
        public abstract String getName();

        /**
         * Quickly generate a brief description summarizing the size of a member.
         * It is important that this size description be generated quickly, so do not, for example,
         * use an expensive utility to compute the size in bytes, but if the size in bytes is easily
         * available without doing anything expensive (ie as with protobufs), it is fine to return
         * a description summarizing the size in bytes. More typical examples might be returning
         * the number of entries in a hash-map, etc.
         * <p/>
         * If there is no easy way to summarize the size of a member, it is fine to return an empty
         * string.
         * <p/>
         * Size descriptions will be logged during pipeline execution to help quantify the impact
         * of the stage on the context members that it provided/required.
         *
         * @param member The ContextMember whose size should be described.
         * @return A very brief summary describing the size of the ContextMember. ie "size=125"
         *         or "321 MB" or "22 entities", etc. Null or empty strings are fine when there
         *         is no meaningful way to summarize the size or it is not helpful to know (ie the size
         *         of a single integer or a lone object) and these will be ignored and not logged
         */
        @Nullable
        public abstract String sizeDescription(@Nonnull M member);

        /**
         * Check whether the context member definition supplies a default instance.
         *
         * @return whether the context member definition supplies a default instance.
         */
        public abstract boolean suppliesDefault();

        /**
         * Get the Java class of the context member.
         *
         * @return the Java class of the context member.
         */
        @Nonnull
        public Class<M> getMemberClass() {
            return klass;
        }

        /**
         * Get the default instance for the definition. Will throw a {@link PipelineContextMemberException}
         * if the definition does not supply a default. See {@link #suppliesDefault()}.
         *
         * @return the default instance for the definition.
         * @throws PipelineContextMemberException If the definition cannot supply a default instance.
         */
        @Nonnull
        public M getDefaultInstance() throws PipelineContextMemberException {
            throw new PipelineContextMemberException(
                String.format("No appropriate default instance for PipelineContextMember %s", toString()));
        }

        @Override
        public String toString() {
            return getName() + "[" + getMemberClass().getSimpleName() + "]";
        }

        /**
         * Create a {@link PipelineContextMemberDefinition} for a new ContextMember.
         * Only one ContextMember can be added to the pipeline for a particular {@link PipelineContextMemberDefinition}.
         * Consider creating static final constants for your {@link PipelineContextMemberDefinition}s.
         * <p/>
         * The created definition will NOT have a default member so either:
         * 1. A stage will have to provide the member or
         * 2. The pipeline will have to be initialized with the member
         * (see {@link Pipeline.PipelineDefinitionBuilder#initialContextMember(PipelineContextMemberDefinition, Supplier)}).
         *
         * @param klass The Java class of the ContextMember.
         * @param nameSupplier A function that supplies the name of the {@link PipelineContextMemberDefinition}.
         *                     This name is used in logging.
         * @param sizeDescriber A function that can be used to describe the size of the member. This function
         *                      should be quick to compute. If there is no appropriate way to compute this or
         *                      knowing the size is not helpful (ie the size of the member never changes), the
         *                      sizeDescriber can return a null or empty string.
         * @param <M> The data type of the ContextMember.
         * @return a {@link PipelineContextMemberDefinition} for a new ContextMember.
         */
        public static <M> PipelineContextMemberDefinition<M> member(@Nonnull final Class<M> klass,
                                                                    @Nonnull final Supplier<String> nameSupplier,
                                                                    @Nonnull final Function<M, String> sizeDescriber) {
            return new PipelineContextMemberDefinition<M>(klass) {
                @Nonnull
                @Override
                public String getName() {
                    return nameSupplier.get();
                }

                @Override
                public boolean suppliesDefault() {
                    return false;
                }

                @Override
                public String sizeDescription(@Nonnull final M member) {
                    return sizeDescriber.apply(member);
                }
            };
        }

        /**
         * Create a {@link PipelineContextMemberDefinition} for a new ContextMember.
         * Only one ContextMember can be added to the pipeline for a particular {@link PipelineContextMemberDefinition}.
         * Consider creating static final constants for your {@link PipelineContextMemberDefinition}s.s
         * <p/>
         * The created definition will NOT have a default member so either:
         * 1. A stage will have to provide the member or
         * 2. The pipeline will have to be initialized with the member
         * (see {@link Pipeline.PipelineDefinitionBuilder#initialContextMember(PipelineContextMemberDefinition, Supplier)}).
         *
         * @param klass The Java class of the ContextMember.
         * @param name The name of the {@link PipelineContextMemberDefinition}.
         *             This name is used in logging.
         * @param sizeDescriber A function that can be used to describe the size of the member. This function
         *                      should be quick to compute. If there is no appropriate way to compute this or
         *                      knowing the size is not helpful (ie the size of the member never changes), the
         *                      sizeDescriber can return a null or empty string.
         * @param <M> The data type of the ContextMember.
         * @return a {@link PipelineContextMemberDefinition} for a new ContextMember.
         */
        public static <M> PipelineContextMemberDefinition<M> member(@Nonnull final Class<M> klass,
                                                                    @Nonnull final String name,
                                                                    @Nonnull final Function<M, String> sizeDescriber) {
            return new PipelineContextMemberDefinition<M>(klass) {
                @Nonnull
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public boolean suppliesDefault() {
                    return false;
                }

                @Override
                public String sizeDescription(@Nonnull final M member) {
                    return sizeDescriber.apply(member);
                }
            };
        }

        /**
         * Create a {@link PipelineContextMemberDefinition} for a new ContextMember.
         * Only one ContextMember can be added to the pipeline for a particular {@link PipelineContextMemberDefinition}.
         * Consider creating static final constants for your {@link PipelineContextMemberDefinition}s.s
         * <p/>
         * The created definition will have a default member so if no earlier stage provides it and the pipeline
         * does not provide it, the default will be added as a member to the pipeline immediately prior to the
         * execution of the first stage that requires it.
         *
         * @param klass The Java class of the ContextMember.
         * @param nameSupplier A function that supplies the name of the {@link PipelineContextMemberDefinition}.
         *                     This name is used in logging.
         * @param defaultSupplier A supplier function for a default value of the member. The supplier function
         *                        must not generate a null member.
         * @param sizeDescriber A function that can be used to describe the size of the member. This function
         *                      should be quick to compute. If there is no appropriate way to compute this or
         *                      knowing the size is not helpful (ie the size of the member never changes), the
         *                      sizeDescriber can return a null or empty string.
         * @param <M> The data type of the ContextMember.
         * @return a {@link PipelineContextMemberDefinition} for a new ContextMember.
         */
        public static <M> PipelineContextMemberDefinition<M> memberWithDefault(
            @Nonnull final Class<M> klass,
            @Nonnull final Supplier<String> nameSupplier,
            @Nonnull final Supplier<M> defaultSupplier,
            @Nonnull final Function<M, String> sizeDescriber) {
            return new PipelineContextMemberDefinition<M>(klass) {
                @Nonnull
                @Override
                public String getName() {
                    return nameSupplier.get();
                }

                @Nonnull
                @Override
                public M getDefaultInstance() {
                    return defaultSupplier.get();
                }

                @Override
                public boolean suppliesDefault() {
                    return true;
                }

                @Override
                public String sizeDescription(@Nonnull final M member) {
                    return sizeDescriber.apply(member);
                }
            };
        }
    }
}
