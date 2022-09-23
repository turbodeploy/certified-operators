package com.vmturbo.mediation.azure.pricing.stages;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingKey;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * A pipeline stage that fetches pricing data from the provider, eg via downloading a file.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class AssignPricingIdentifiersStage<E extends ProbeStageEnum>
        extends Stage<PricingWorkspace, DiscoveredPricing, E> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Gson GSON = new Gson();
    private static final String READING_PLAN_MAP_ERROR =
            "There was an error reading the Azure plan ID map from properties. "
                + "The default map will be used";

    private final IProbePropertySpec<Map<String, String>> planIdMapProperty;

    private FromContext<IPropertyProvider> propertyProvider
            = requiresFromContext(PricingPipelineContextMembers.PROPERTY_PROVIDER);
    private FromContext<PricingKey> requestedKey
            = requiresFromContext(PricingPipelineContextMembers.PRICING_KEY);

    /**
     * Construct a pricing data fetcher stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status
     * @param planIdMapProperty the property to use for getting the plan ID mapping to use
     */
    public AssignPricingIdentifiersStage(@Nonnull E probeStage,
            @Nonnull IProbePropertySpec<Map<String, String>> planIdMapProperty) {
        super(probeStage);
        this.planIdMapProperty = planIdMapProperty;
    }

    @NotNull
    @Override
    protected StageResult<DiscoveredPricing> executeStage(@NotNull PricingWorkspace workspace) {
        final Map<String, String> planIdMap = propertyProvider.get().getProperty(planIdMapProperty);

        final DiscoveredPricing result = new DiscoveredPricing();
        List<String> assignments = new ArrayList<>();

        for (Entry<String, PriceTable.Builder> planPricing : workspace.getBuilders().entrySet()) {
            String turboPlanId = planIdMap.get(planPricing.getKey());
            if (turboPlanId != null) {
                PricingKey pricingKey = requestedKey.get().withPlanId(turboPlanId);
                PriceTable.Builder priceTableBuilder = planPricing.getValue();

                priceTableBuilder.addAllPriceTableKeys(pricingKey.getPricingIdentifiers());

                result.put(pricingKey, priceTableBuilder);
                assignments.add(String.format("%s -> %s", planPricing.getKey(), pricingKey));
            } else {
                LOGGER.warn("Unknown plan identifier for plan with name {}, skipped", planPricing.getKey());
            }
        }

        final String status = String.format("Pricing for %d plans discovered", result.size());
        assignments.sort(String::compareTo);
        getStageInfo().ok(status).longExplanation("Assignments:\n\n"
            + String.join("\n", assignments));

        return StageResult.withResult(result).andStatus(Status.success(status));
    }

    /**
     * Utility function for parsing a string into a plan map, for use in property
     * definitions.
     *
     * @param rawMap the raw string to parse
     * @param defaultValue the default value to use if the parse fails
     * @return the resulting plan name to ID map
     */
    public static Map<String, String> parsePlanIdMap(
            @Nullable final String rawMap,
            @Nonnull final Map<String, String> defaultValue) {
        if (StringUtils.isBlank(rawMap)) {
            return defaultValue;
        }

        final Type itemsMapType = new TypeToken<Map<String, String>>() {}.getType();
        try {
            return new CaseInsensitiveMap(GSON.fromJson(rawMap, itemsMapType));
        } catch (JsonParseException e) {
            LOGGER.error(READING_PLAN_MAP_ERROR, e);
            return defaultValue;
        }
    }
}
