package com.vmturbo.mediation.azure.pricing.pipeline;

import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Definitions for {@link PipelineContextMemberDefinition}s for the {@link PricingPipelineContext}.
 */
public class PricingPipelineContextMembers {

    /**
     * Hide constructor for utility class.
     */
    private PricingPipelineContextMembers() {
    }

    /**
     * The probe properties.
     */
    public static final PipelineContextMemberDefinition<IPropertyProvider> PROPERTY_PROVIDER =
        PipelineContextMemberDefinition.member(IPropertyProvider.class, "Property Provider",
            propertyProvider -> null);
}
