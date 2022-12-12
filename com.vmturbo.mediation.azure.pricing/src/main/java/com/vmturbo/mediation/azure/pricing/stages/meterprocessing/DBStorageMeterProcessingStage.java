package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static com.vmturbo.platform.sdk.common.util.Units.GBYTE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.cost.AzureCostProbe;
import com.vmturbo.mediation.azure.cost.db.AzureDBTierStorageInfo;
import com.vmturbo.mediation.azure.cost.db.AzureDBTierStorageInfo.AzureDBTierSupportedStorage;
import com.vmturbo.mediation.azure.cost.parser.VCoreMeterProcessor.AzureVCoreDBTierKeyValidator;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.enums.DeploymentType;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.azure.pricing.util.UnitConverter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.hybrid.cloud.common.azure.DbUtils.ServiceTier;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * Stage for creating a price map for DBStorage meter type.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *         of discovery.
 */
public class DBStorageMeterProcessingStage<E extends ProbeStageEnum>
        extends AbstractMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String AZURE_DC_NAME_FORMAT = "azure::%s::DC::%s";

    private final HashMap<String, HashMap<String, Set<ResolvedMeter>>>
            azureMeterByPlanIdAndRegionId = new HashMap<>();
    private final Map<String, Map<String, Map<String, Map<DeploymentType, List<Price>>>>>
            dBStoragePriceMap = new CaseInsensitiveMap();

    private final Map<String, AzureDBTierStorageInfo> azureDBTierSupportedStorageMapByRegion =
            new AzureCostProbe().getAllDBTierStorageInfo();

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     */
    public DBStorageMeterProcessingStage(@NotNull E probeStage) {
        super(probeStage, MeterType.DBStorage, LOGGER);
        providesToContext(PricingPipelineContextMembers.DB_STORAGE_PRICE_MAP,
                (Supplier<Map>)this::getDBStoragePriceMap);
    }

    @NotNull
    @Override
    String addPricingForResolvedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @NotNull Collection<ResolvedMeter> resolvedMeters) {
        Map<String, Integer> statuses = new HashMap<>();
        resolvedMeters.forEach(resolvedMeter -> {
            final AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            final Map<String, TreeMap<Double, AzureMeter>> pricing = resolvedMeter.getPricing();
            for (String planId : pricing.keySet()) {
                for (String region : descriptor.getRegions()) {
                    azureMeterByPlanIdAndRegionId.computeIfAbsent(planId, k -> new HashMap<>())
                            .computeIfAbsent(region, k -> new HashSet<>())
                            .add(resolvedMeter);
                }
            }
        });
        azureMeterByPlanIdAndRegionId.forEach((planId, value) -> {
            final AtomicInteger addedCount = new AtomicInteger(0);
            value.forEach((region, value1) -> {
                final Map<ServiceTier, List<ResolvedMeter>> resolvedMeterByServiceTier =
                        value1.stream().collect(Collectors.groupingBy(
                                resolvedMeter -> ServiceTier.getType(
                                        resolvedMeter.getDescriptor().getSubType())));
                AzureDBTierStorageInfo dbTierSizes = azureDBTierSupportedStorageMapByRegion.get(
                        String.format(AZURE_DC_NAME_FORMAT, region, region));
                if (dbTierSizes == null) {
                    LOGGER.warn("No DBTiers found in region : {}.", region);
                    return;
                }
                dbTierSizes.getAllDbTier().forEach(dbTier -> {
                    final AzureVCoreDBTierKeyValidator azureVCoreDBTierKeyValidator =
                            new AzureVCoreDBTierKeyValidator(dbTier);
                    final Map<DeploymentType, Collection<Price>> dependentPricesByDeployment =
                            createDBStoragePrices(dbTierSizes.getDbTierByName(dbTier),
                                    resolvedMeterByServiceTier, dbTier,
                                    azureVCoreDBTierKeyValidator, planId);
                    dependentPricesByDeployment.forEach(
                            (key, v) -> dBStoragePriceMap.computeIfAbsent(planId,
                                            k -> new HashMap<>())
                                    .computeIfAbsent(region, k -> new HashMap<>())
                                    .computeIfAbsent(dbTier, k -> new HashMap<>())
                                    .computeIfAbsent(key, k -> new ArrayList<>())
                                    .addAll(v));
                });
                addedCount.incrementAndGet();
            });
            statuses.put(planId, addedCount.get());
        });
        return String.format("DB Storage prices added for [PlanId, Region]: %s", statuses);
    }

    private Map<DeploymentType, Collection<Price>> createDBStoragePrices(
            @Nullable final AzureDBTierSupportedStorage supportedSizes,
            @Nonnull final Map<ServiceTier, List<ResolvedMeter>> resolvedMeterByServiceTier,
            @Nonnull final String tierName,
            final AzureVCoreDBTierKeyValidator azureVCoreDBTierKeyValidator, String planId) {
        ServiceTier serviceTier = ServiceTier.getType(tierName);
        List<ResolvedMeter> resolvedMeters = resolvedMeterByServiceTier.get(serviceTier);
        if (supportedSizes != null && resolvedMeters != null) {
            return resolvedMeters.stream().collect(Collectors.toMap(v -> DeploymentType.from(
                            v.getDescriptor().getRedundancy()).isPresent() ? DeploymentType.from(
                            v.getDescriptor().getRedundancy()).get() : DeploymentType.SINGLE_AZ,
                    v -> generateDBPrices(supportedSizes, v, azureVCoreDBTierKeyValidator,
                            planId)));
        } else {
            return Collections.emptyMap();
        }
    }

    private Collection<Price> generateDBPrices(
            @Nonnull final AzureDBTierSupportedStorage azureDBTierSupportedStorage,
            @Nonnull final ResolvedMeter meter,
            final AzureVCoreDBTierKeyValidator azureVCoreDBTierKeyValidator, String planId) {
        final Collection<Price> prices = new ArrayList<>();
        final List<Long> storageSizes = new LinkedList<>(
                azureDBTierSupportedStorage.getSupportedSizes());
        long incrementInterval = 0L;
        long endRange = 0L;
        Collections.sort(storageSizes);
        if (azureVCoreDBTierKeyValidator.isValid() && !storageSizes.isEmpty()) {
            int maxStorageIndex = Math.max(storageSizes.size() - 1, 0);
            endRange = storageSizes.get(maxStorageIndex);
            incrementInterval = GBYTE;
        } else {
            int index = 0;
            if (azureDBTierSupportedStorage.getIncludedStorage() != null
                    && azureDBTierSupportedStorage.getIncludedStorage() > 0L) {
                int startingSizeIndex = storageSizes.indexOf(
                        azureDBTierSupportedStorage.getIncludedStorage());
                endRange = storageSizes.get(startingSizeIndex);
                // create included or free storage.
                Price freePrice = createPrice(null,
                        Optional.of(storageSizes.get(startingSizeIndex) / GBYTE),
                        Optional.of(endRange / GBYTE), planId);
                prices.add(freePrice);
                index = startingSizeIndex + 1;
            }

            // This loop takes a sorted list and
            // captures the last item with the same interval.
            // eg: 100, 200, 300, 400, 600 will return [{interval: 100, endUnit: 400}, { interval: 200, endUnit : 600}.
            for (; index < storageSizes.size(); index++) {
                long currIncrement = storageSizes.get(index) - endRange;
                if (incrementInterval != 0L && incrementInterval != currIncrement) {
                    //create new price.
                    Price price = createPrice(meter, Optional.of(endRange / GBYTE),
                            Optional.of(incrementInterval / GBYTE), planId);
                    if (price != null) {
                        prices.add(price);
                    }
                }
                incrementInterval = currIncrement;
                endRange = storageSizes.get(index);
            }
        }
        // handling the last case which will not be captured in the loop above.
        if (incrementInterval != 0) {
            Price price = createPrice(meter, Optional.of(endRange / GBYTE),
                    Optional.of(incrementInterval / GBYTE), planId);
            if (price != null) {
                prices.add(price);
            }
        }
        return prices;
    }

    /**
     * Creates a Price {@link Price} for the meter.
     *
     * @param meter ResolvedMeter meter.
     * @param endRangeOpt EndRange value that will be set to all the prices (check
     *         PricingDTO.proto for details)
     * @param incrementInterval incrementInterval dictates the increments allowed to reach
     *         endRangeOpt.
     * @param planId the planId for the current plan.
     * @return Price {@link Price}
     */
    private static Price createPrice(final ResolvedMeter meter, final Optional<Long> endRangeOpt,
            @Nonnull final Optional<Long> incrementInterval, String planId) {
        double amount = 0.0;
        final Unit unit;
        if (meter != null) {
            final AzureMeterDescriptor meterDescriptor = meter.getDescriptor();
            final Map<String, TreeMap<Double, AzureMeter>> pricing = meter.getPricing();
            final TreeMap<Double, AzureMeter> pricingForPlan = pricing.get(planId);
            List<AzureMeter> storageMeters = new ArrayList<>(pricingForPlan.values());
            if (storageMeters.isEmpty()) {
                LOGGER.warn("No DBStorage pricing found for planId {}, SKU={}", planId,
                        meterDescriptor.getSkus());
                return null;
            } else {
                AzureMeter storageMeter = storageMeters.get(0);
                if (UnitConverter.priceUnitMap.get(storageMeter.getUnitOfMeasure()) != null) {
                    unit = UnitConverter.priceUnitMap.get(storageMeter.getUnitOfMeasure());
                } else {
                    LOGGER.warn("Unknown unit: " + storageMeter.getUnitOfMeasure());
                    unit = Unit.GB_MONTH;
                }
                amount = storageMeter.getUnitPrice();
            }
        } else {
            unit = Unit.GB_MONTH;
        }
        Price.Builder priceBuilder = Price.newBuilder();
        endRangeOpt.ifPresent(priceBuilder::setEndRangeInUnits);
        incrementInterval.ifPresent(priceBuilder::setIncrementInterval);
        return priceBuilder.setPriceAmount(CurrencyAmount.newBuilder().setAmount(amount).build())
                .setUnit(unit)
                .build();
    }

    /**
     * Get the DBStorage price map populated by this stage.
     *
     * @return HashMap.
     */
    private Map getDBStoragePriceMap() {
        return dBStoragePriceMap;
    }
}
