package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.common.CostContributersUtils;
import com.vmturbo.mediation.cost.common.CostUtils;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;

/**
 * Stage to process MeterType IP.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of discovery.
 */
public class IPMeterProcessingStage<E extends ProbeStageEnum> extends AbstractMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Create an IP Meter Processing stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public IPMeterProcessingStage(@Nonnull E probeStage) {
        super(probeStage, MeterType.IP, LOGGER);
    }

    @Override
    @Nonnull
    protected String addPricingForResolvedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters) {
        Map<String, Integer> statuses = new HashMap<>();
        resolvedMeters.stream().forEach(resolvedMeter -> {
           final AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
           final Map<String, TreeMap<Double, AzureMeter>> pricing = resolvedMeter.getPricing();

           for (String planId : pricing.keySet()) {
               LOGGER.debug("Processing IP Meters for Plan {}", planId);

               final TreeMap<Double, AzureMeter> pricingForPlan = pricing.get(planId);
               // There should only be one AzureMeter for each IP.
               List<AzureMeter> ipAzureMeters = pricingForPlan.values().stream().collect(Collectors.toList());
               if (ipAzureMeters.isEmpty()) {
                   statuses.put(planId, 0);
                   LOGGER.warn("No IP price found for planId={}", planId);
               } else {
                   // There is only one IP cost per plan in MCA
                   AzureMeter ipAzureMeter = ipAzureMeters.get(0);

                   // unit of measure should be "1 Hour".  If not, log the message.
                   final String unitOfMeasure = ipAzureMeter.getUnitOfMeasure();
                   if (!"1 Hour".equalsIgnoreCase(unitOfMeasure)) {
                       LOGGER.warn("Unit of Measure in ip meter from plan {} is {}.  Expected to be \"1 Hour\". ",
                               planId, unitOfMeasure);
                   }
                   // TODO move to UnitConverter.
                   final Price.Builder price = Price.newBuilder()
                           .setPriceAmount(CurrencyAmount.newBuilder()
                                   .setAmount(ipAzureMeter.getUnitPrice())
                                   // Note: MCA Pricesheet includes a currency column.
                                   // We may want to use that to set the currency in `PriceAmount`
                                   .setCurrency(Currency.getInstance(CostUtils.COST_CURRENCY_USD).getNumericCode()))
                           .setUnit(Unit.HOURS);

                   final IpConfigPrice.Builder ipConfigPrice = IpConfigPrice.newBuilder()
                           .setType(CostContributersUtils.TYPE_PUBLIC_IPADRESS)
                           .setFreeIpCount(0)
                           .addPrices(price);

                   int regionAddedIpPrice = 0;
                   for (String region : descriptor.getRegions()) {
                       OnDemandPriceTableByRegionEntry.Builder onDemand = pricingWorkspace.getOnDemandBuilder(planId, region);
                       onDemand.setIpPrices(IpPriceList.newBuilder().addIpPrice(ipConfigPrice));

                       regionAddedIpPrice++;
                   }
                   statuses.put(planId, regionAddedIpPrice);
               }
           }
        });

        final String status = String.format("IP prices added for [PlanId, Number of Regions]: %s", statuses);
        return status;
    }
}
