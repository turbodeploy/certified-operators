package com.vmturbo.mediation.azure.pricing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.map.CaseInsensitiveMap;

import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;

/**
 * Contains PriceTableBuilders and the pre-processed ResolvedMeter data from which to populate them.
 * Individual stages will populate each particular kind of data.
 */
public class PricingWorkspace {
    private static final String REGION_FORMAT = "azure::%s::DC::%s";
    private Map<MeterType, List<ResolvedMeter>> resolvedMeterByMeterType;
    private Map<String, PriceTable.Builder> priceTableBuilderByPlanId;
    private Map<String, Map<String, OnDemandPriceTableByRegionEntry.Builder>>
        onDemandPriceTableBuilder = new HashMap<>();
    private Map<String, LicenseOverrides> onDemandLicenseOverrides;
    private final Map<String, Set<LicensePriceEntry>> onDemandLicensePriceByPlanId = new CaseInsensitiveMap();
    private boolean built = false;

    /**
     * The ID for Azure itself, as a service provider.
     */
    public static final String AZURE_SERVICE_PROVIDER_ID = "SERVICE_PROVIDER::Azure";

    /**
     * Constructor.
     *
     * @param resolvedMeterByMeterType map of {@link MeterType} to Collection of {@link ResolvedMeter}
     */
    public PricingWorkspace(@Nonnull Map<MeterType, List<ResolvedMeter>> resolvedMeterByMeterType) {
        this.resolvedMeterByMeterType = resolvedMeterByMeterType;
        this.priceTableBuilderByPlanId = new CaseInsensitiveMap();
    }

    /**
     * Get the resolved meters by MeterType.
     *
     * @return the map of meter types to all the resolved meters of that type.
     */
    @Nonnull
    public Map<MeterType, List<ResolvedMeter>> getResolvedMeterByMeterType() {
        return resolvedMeterByMeterType;
    }

    /**
     * Get the resolved meters by MeterType, and remove the requested meterType from the map.
     *
     * @param meterType requested {@link MeterType}
     * @return list of all the resolved meters of the requested meter type.
     */
    @Nullable
    public List<ResolvedMeter> getAndRemoveResolvedMeterByMeterType(@Nonnull MeterType meterType) {
        return resolvedMeterByMeterType.remove(meterType);
    }

    /**
     * Get the map of price table builders by plan. Call one time only, when all processors are done.
     *
     * @return the map of builders by plan
     */
    @Nonnull
    public Map<String, PriceTable.Builder> getBuilders() {
        if (built) {
            throw new IllegalStateException("Cannot create builders more than once");
        }

        built = true;

        Map<String, PriceTable.Builder> result = new HashMap<>();

        // Add the regional on demand price tables

        onDemandPriceTableBuilder.forEach((planId, regionMap) -> {
            PriceTable.Builder priceTableBuilder = getPriceTableBuilderForPlan(planId);

            regionMap.forEach((regionId, onDemandBuilder) -> {
                priceTableBuilder.addOnDemandPriceTable(onDemandBuilder);
            });
            if (onDemandLicenseOverrides != null) {
                priceTableBuilder.putAllOnDemandLicenseOverrides(onDemandLicenseOverrides);
            }
            if (onDemandLicensePriceByPlanId.get(planId) != null) {
                priceTableBuilder.addAllOnDemandLicensePriceTable(
                        onDemandLicensePriceByPlanId.get(planId));
            }
        });

        return priceTableBuilderByPlanId;
    }

    /**
     * Get the price table builder for the given plan. If one hasn't been accessed yet,
     * it will be created. Do not modify the OnDemandPriceTable in this builder, use
     * getOnDemandBuilder instead.
     *
     * @param planId the plan for which to get the PriceTable builder
     * @return the PriceTable builder
     */
    @Nonnull
    public PriceTable.Builder getPriceTableBuilderForPlan(@Nonnull String planId) {
        return priceTableBuilderByPlanId.computeIfAbsent(planId,
            k -> PriceTable.newBuilder().setServiceProviderId(AZURE_SERVICE_PROVIDER_ID));
    }

    /**
     * Get an OnDemandPriceTableByRegionEntry builder for the given plan and region.
     * If the builder doesn't exist yet, one will be created, its related region field will
     * be populated, and it will be added to the PriceTable builder for the plan automatically.
     *
     * @param planId the plan ID for which to get the on demand pricing builder
     * @param region the region for which to get the on demand pricing builder
     * @return an OnDemandPriceTableByRegionEntry builder for the region and plan
     */
    @Nonnull
    public OnDemandPriceTableByRegionEntry.Builder getOnDemandBuilder(
            @Nonnull final String planId, @Nonnull final String region) {
        return onDemandPriceTableBuilder
            .computeIfAbsent(planId, plan -> new HashMap<>())
            .computeIfAbsent(region, regionName -> {
                OnDemandPriceTableByRegionEntry.Builder onDemand = OnDemandPriceTableByRegionEntry.newBuilder();
                onDemand.setRelatedRegion(regionEntity(regionName));

                return onDemand;
            });
    }

    @Nonnull
    private EntityDTO regionEntity(@Nonnull String regionName) {
        return EntityDTO.newBuilder()
            .setEntityType(EntityType.REGION)
            .setId(String.format(REGION_FORMAT, regionName, regionName))
            .build();
    }

    /**
     * Used to set the onDemandLicenseOverrides Map.
     *
     * @param onDemandLicenseOverrides Map of onDemandLicenseOverrides.
     */
    public void setOnDemandLicenseOverrides(Map<String, LicenseOverrides> onDemandLicenseOverrides) {
        this.onDemandLicenseOverrides = onDemandLicenseOverrides;
    }

    /**
     * Get a set of LicensePriceEntry builder for the given plan.
     *
     * @param planId the plan ID for which to get the LicencePriceEntry list.
     * @return set of LicencePriceEntry.
     */
    @Nonnull
    public Set<LicensePriceEntry> getLicensePriceEntryList(@Nonnull final String planId) {
        return onDemandLicensePriceByPlanId.computeIfAbsent(planId, plan -> new HashSet<>());
    }
}