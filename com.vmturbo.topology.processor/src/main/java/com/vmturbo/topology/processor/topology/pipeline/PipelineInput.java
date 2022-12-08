package com.vmturbo.topology.processor.topology.pipeline;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * The input object to the Pipeline.
 *
 * <p>If Feature Flag "USE_EXTENDABLE_PIPELINE_INPUT" is disabled (default), the {@link PipelineInput}
 * object will contain the {@link EntityStore}. If the Feature Flag is enabled, the
 * {@link PipelineInput} will contain the {@link StitchingContext}, which will be constructed when
 * the {@code .build()} method is called.</p>
 */
public class PipelineInput {
    private StitchingContext stitchingContext;

    private EntityStore entityStore;

    /**
     * The constructor that is called in the {@code .build()} method. If the Feature Flag
     * "USE_EXTENDABLE_PIPELINE_INPUT" is disabled (default), we initialize the {@link EntityStore},
     * which will be used as input to the Pipeline. If the Feature Flag is disabled, we initialize
     * the {@link StitchingContext} to use this as input to the Pipeline.
     *
     * @param builder the builder object.
     */
    private PipelineInput(Builder builder) {
        if (FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT.isEnabled()) {
            this.stitchingContext = builder.stitchingContext;
        } else {
            this.entityStore = builder.entityStore;
        }
    }

    /**
     * Method to get the {@link StitchingContext}.
     *
     * @return the {@link StitchingContext} object.
     */
    public StitchingContext getStitchingContext() {
        return stitchingContext;
    }

    /**
     * Method to get the {@link EntityStore}.
     *
     * @return the {@link EntityStore} object.
     */
    public EntityStore getEntityStore() {
        return entityStore;
    }

    /**
     * Constructs and returns a new {@link PipelineInput.Builder} instance.
     * @return The newly constructed {@link PipelineInput.Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link PipelineInput} instances.
     */
    public static class Builder {
        private EntityStore entityStore;

        private StalenessInformationProvider stalenessProvider;

        private StitchingContext stitchingContext;

        /**
         * A metric that tracks duration of preparation for stitching.
         */
        private static final DataMetricSummary STITCHING_PREPARATION_DURATION_SUMMARY =
                DataMetricSummary.builder()
                                 .withName("tp_stitching_preparation_duration_seconds")
                                 .withHelp("Duration of construction for data structures in preparation for stitching.")
                                 .build()
                                 .register();

        private Builder() {}

        /**
         * Adds the {@link EntityStore} to the Pipeline Input.
         *
         * @return This builder instance for method chaining.
         */
        public Builder setEntityStore(EntityStore entityStore) {
            this.entityStore = entityStore;
            return this;
        }

        /**
         * Adds a {@link EntityStore} to the Pipeline Input.
         *
         * @return This builder instance for method chaining.
         */
        public Builder setStalenessProvider(StalenessInformationProvider stalenessProvider) {
            this.stalenessProvider = stalenessProvider;
            return this;
        }

        /**
         * Method to build the PipelineInput object.
         *
         * <p>If Feature Flag "USE_EXTENDABLE_PIPELINE_INPUT" is enabled, then the input to the
         * pipeline is the {@link StitchingContext}, which we need to construct. Otherwise
         * (default), the input to the pipeline is the {@link EntityStore}.</p>
         *
         * @return a new PipelineInput instance.
         * @throws IllegalStateException if there is no Entity Store provided.
         */
        public PipelineInput build() throws IllegalStateException {
            if (entityStore == null) {
                throw new IllegalStateException(
                        "PipelineInput must have not-null EntityStore provided.");
            }
            if (FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT.isEnabled()) {
                stitchingContext = constructStitchingContext();
            }
            return new PipelineInput(this);
        }

        /**
         * Method to construct the Stitching Context from the Entity Store that is provided to the
         * builder.
         *
         * @return the Stitching Context.
         */
        private StitchingContext constructStitchingContext() {
            final StitchingContext stitchingContext;
            // TODO: Check if this is needed (copied from Stitching Stage).
            try (
                    DataMetricTimer preparationTimer = STITCHING_PREPARATION_DURATION_SUMMARY.startTimer();
                    TracingScope scope = Tracing.trace("constructStitchingContext")
            ) {
                stitchingContext = entityStore.constructStitchingContext(stalenessProvider);
            }
            //TODO: [OM-93423] Check if this is the right place to call this. It was at the end of
            // StitchingStage#executeStage, so maybe it needs the Stitching to happen first.
            entityStore.sendMetricsEntityAndTargetData();
            return stitchingContext;
        }
    }
}