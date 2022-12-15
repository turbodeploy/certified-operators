package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.Map;
import java.util.zip.ZipFile;

import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.mediation.azure.pricing.util.PriceConverter;
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

    /**
     * The key for the specific pricing being discovered.
     */
    public static final PipelineContextMemberDefinition<PricingKey> PRICING_KEY =
        PipelineContextMemberDefinition.member(PricingKey.class, "Pricing Key",
            key -> null);

    /**
     * The opened zip file.
     */
    public static final PipelineContextMemberDefinition<ZipFile> ZIP_FILE =
        PipelineContextMemberDefinition.member(ZipFile.class, "Opened Zip File",
            zipFile -> null);

    /**
     * The pricing converter.
     */
    public static final PipelineContextMemberDefinition<PriceConverter> PRICE_CONVERTER =
        PipelineContextMemberDefinition.member(PriceConverter.class, "Price Converter",
            converter -> null);

    /**
     * DBStorage price map.
     */
    public static final PipelineContextMemberDefinition<Map> DB_STORAGE_PRICE_MAP =
            PipelineContextMemberDefinition.member(Map.class, "DBStorage Price Map",
                    converter -> null);
}
