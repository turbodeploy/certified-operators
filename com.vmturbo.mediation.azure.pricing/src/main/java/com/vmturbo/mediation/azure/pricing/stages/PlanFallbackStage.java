package com.vmturbo.mediation.azure.pricing.stages;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * A pipeline stage that applies fallbacks from one plan's pricing to another, where the
 * first plan doesn't provide a specific kind of pricing.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class PlanFallbackStage<E extends ProbeStageEnum>
        extends Stage<Collection<ResolvedMeter>, Collection<ResolvedMeter>, E> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Gson GSON = new Gson();
    private static final String READING_PLAN_MAP_ERROR =
            "There was an error reading the Azure plan fallback map from properties. "
                + "The default map will be used";

    private final IProbePropertySpec<Map<String, String>> planFallbackMapProperty;

    private FromContext<IPropertyProvider> propertyProvider
            = requiresFromContext(PricingPipelineContextMembers.PROPERTY_PROVIDER);

    /**
     * Construct a pricing data fetcher stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status
     * @param planFallbackMapProperty the property to use for getting the plan fallback mapping to use
     */
    public PlanFallbackStage(@Nonnull E probeStage,
            @Nonnull IProbePropertySpec<Map<String, String>> planFallbackMapProperty) {
        super(probeStage);
        this.planFallbackMapProperty = planFallbackMapProperty;
    }

    @NotNull
    @Override
    protected StageResult<Collection<ResolvedMeter>> executeStage(
            @NotNull Collection<ResolvedMeter> resolvedMeters) {
        final Map<String, String> planFallbackMap = propertyProvider.get()
            .getProperty(planFallbackMapProperty);

        Map<MeterType, Integer> copiedByType = new HashMap<>();
        int copied = 0;

        for (Entry<String, String> mapping : planFallbackMap.entrySet()) {
            final String fromPlan = mapping.getKey();
            final String toPlan = mapping.getValue();

            // Only copy pricing to the plan if the plan has some pricing.
            // For example, if a billing profile has no DevTest pricing,
            // we don't copy the entire regular pricing over. Only if there
            // is already at least some pricing do we fill in gaps
            // from the other plan.

            if (resolvedMeters.stream().map(ResolvedMeter::getPricing)
                .anyMatch(p -> p.containsKey(fromPlan))) {
                for (ResolvedMeter resolvedMeter : resolvedMeters) {
                    Map<String, TreeMap<Double, AzureMeter>> pricing = resolvedMeter.getPricing();

                    if (!pricing.containsKey(fromPlan)) {
                        TreeMap<Double, AzureMeter> pricingToCopy = pricing.get(toPlan);
                        if (pricingToCopy != null) {
                            pricing.put(fromPlan, pricingToCopy);

                            // keep a histogram by meter type
                            copiedByType.compute(resolvedMeter.getDescriptor().getType(),
                                (k, v) -> (v == null) ? 1 : v + 1);
                            copied++;
                        }
                    }
                }
            }
        }

        final String status = String.format("Copied %d (%.2f%%) resolved meters from fallback plan",
            copied, 100.0D * copied / resolvedMeters.size());

        final String longExplanation = "Copied pricing by type:\n\n"
            + copiedByType.entrySet().stream()
                .map(e -> String.format("%d\t%s", e.getValue(), e.getKey().toString()))
                .collect(Collectors.joining("\n"));

        getStageInfo().ok(status).longExplanation(longExplanation);

        return StageResult.withResult(resolvedMeters).andStatus(Status.success(status));
    }

    /**
     * Utility function for parsing a string into a plan map, for use in property
     * definitions.
     *
     * @param rawMap the raw string to parse
     * @param defaultValue the default value to use if the parse fails
     * @return the resulting plan name to ID map
     */
    public static Map<String, String> parsePlanFallbackMap(
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
