package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.common.CostUtils;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * Stage to process MeterType OS and populate on demand license prices.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of discovery.
 */
public class LicensePriceProcessingStage<E extends ProbeStageEnum> extends AbstractMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<String, Map<String, LicensePriceEntry.Builder>> onDemandLicensePriceMap = new HashMap<>();

    static final String WINDOWS_SERVER = "WINDOWS_SERVER";
    @VisibleForTesting
    static final String WINDOWS_SERVER_BURST = "WINDOWS_SERVER_BURST";

    static final String HOUR = "1 Hour";

    /**
     * Create a License Price Processor Stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public LicensePriceProcessingStage(@Nonnull E probeStage) {
        super(probeStage, MeterType.OS, LOGGER);
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
                LOGGER.debug("Processing License Pricing For Plan {}", planId);

                final TreeMap<Double, AzureMeter> pricingForPlan = pricing.get(planId);
                List<AzureMeter> licenseAzureMeters = new ArrayList<>(pricingForPlan.values());
                if (licenseAzureMeters.isEmpty()) {
                    statuses.put(planId, 0);
                    LOGGER.warn("No License pricing found for planId={}, SKU={}", planId,
                            descriptor.getSkus());
                } else {
                    AzureMeter licenseAzureMeter = licenseAzureMeters.get(0);
                    // unit of measure should be "1 Hour".  If not, log the message.
                    final String unitOfMeasure = licenseAzureMeter.getUnitOfMeasure();
                    if (!HOUR.equalsIgnoreCase(unitOfMeasure)) {
                        LOGGER.warn(
                                "Unit of Measure in OS meter from plan {} is {}.  Expected to be \"1 Hour\". ",
                                planId, unitOfMeasure);
                    }
                    // TODO move to UnitConverter.
                    final Price.Builder price = Price.newBuilder().setPriceAmount(
                            CurrencyAmount.newBuilder()
                                    .setAmount(licenseAzureMeter.getUnitPrice())
                                    .setCurrency(Currency.getInstance(CostUtils.COST_CURRENCY_USD)
                                            .getNumericCode())).setUnit(Unit.HOURS);
                    String sku = descriptor.getSkus().get(0);
                    Pair<OSType, Boolean> osPair = getCommonLicenseInfo(sku);
                    onDemandLicensePriceMap.computeIfAbsent(planId, plan -> new HashMap<>())
                            .computeIfAbsent(sku, skuName -> LicensePriceEntry.newBuilder()
                                    .setOsType(osPair.first)
                                    .setBurstableCPU(osPair.second))
                            .addLicensePrices((LicensePrice.newBuilder()
                                    .setNumberOfCores(Integer.parseInt(
                                            Objects.requireNonNull(descriptor.getSubType())))
                                    .setPrice(price)
                                    .build()));
                }
            }
        });
        onDemandLicensePriceMap.keySet().forEach(plan -> {
            statuses.put(plan, onDemandLicensePriceMap.get(plan).size());
            onDemandLicensePriceMap.get(plan).values().forEach(
                    entry -> pricingWorkspace.getLicensePriceEntryList(plan).add(entry.build()));
        });

        return String.format("License prices added for [PlanId, Number of OS Types]: %s", statuses);
    }

    private Pair<OSType, Boolean> getCommonLicenseInfo(String sku) {

        if (WINDOWS_SERVER.equals(sku)) {
            return new Pair<>(OSType.WINDOWS, false);
        } else if (WINDOWS_SERVER_BURST.equals(sku)) {
            // The WINDOWS_SERVER_BURST is used to capture costs for Azure B-Series burstable
            // templates.
            return new Pair<>(OSType.WINDOWS, true);
        } else {
            try {
                return new Pair<>(OSType.valueOf(sku), false);
            } catch (IllegalArgumentException exception) {
                LOGGER.warn("Unknown OSType encountered in LicensePriceProcessing Stage for SKU {}",
                        sku);
                return new Pair<>(OSType.UNKNOWN_OS, false);
            }
        }
    }
}
