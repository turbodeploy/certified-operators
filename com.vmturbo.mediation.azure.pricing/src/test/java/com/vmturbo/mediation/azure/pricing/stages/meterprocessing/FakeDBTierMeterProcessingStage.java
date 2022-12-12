package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.enums.DeploymentType;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * A fake DBTierMeterProcessingStage.
 *
 * @param <E> ProbeStageEnum.
 */
public class FakeDBTierMeterProcessingStage<E extends ProbeStageEnum>
        extends AbstractDbTierMeterProcessingStage<E> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     */
    public FakeDBTierMeterProcessingStage(@NotNull E probeStage) {
        super(probeStage, MeterType.DTU, logger);
    }

    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @NotNull Collection<ResolvedMeter> resolvedMeters) {
        assertEquals(1, resolvedMeters.size());
        List<Price> priceList = getDbStoragePriceList("test", "test", "test", DeploymentType.NONE);
        assertEquals(0, priceList.size());
        priceList = getDbStoragePriceList("azure plan for devtest", "francecentral",
                "generalpurpose_gp_fsv2_16", DeploymentType.MULTI_AZ);
        assertEquals(1, priceList.size());
        Price price = priceList.get(0);
        assertEquals(0.288, price.getPriceAmount().getAmount(), 0);
        assertEquals(Unit.GB_MONTH, price.getUnit());
        priceList = getDbStoragePriceList("azure plan for devtest", "northeurope",
                "generalpurpose_gp_fsv2_16", DeploymentType.MULTI_AZ);
        assertEquals(0, priceList.size());
        priceList = getDbStoragePriceList("azure plan for devtest", "northeurope",
                "generalpurpose_gp_fsv2_16", DeploymentType.NONE);
        assertEquals(1, priceList.size());
        price = priceList.get(0);
        assertEquals(0.1265, price.getPriceAmount().getAmount(), 0);
        assertEquals(Unit.GB_MONTH, price.getUnit());
        priceList = getDbStoragePriceList("azure plan For devtest", "australiasoutheast",
                "premium_p1", DeploymentType.NONE);
        assertEquals(3, priceList.size());
        price = priceList.get(0);
        assertEquals(0, price.getPriceAmount().getAmount(), 0);
        assertEquals(500, price.getEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, price.getUnit());
        priceList = getDbStoragePriceList("Azure plan", "westus", "standard_s12",
                DeploymentType.NONE);
        assertEquals(5, priceList.size());
        price = priceList.get(0);
        assertEquals(0, price.getPriceAmount().getAmount(), 0);
        assertEquals(250, price.getEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, price.getUnit());
        price = priceList.get(1);
        assertEquals(0.221, price.getPriceAmount().getAmount(), 0);
        assertEquals(300, price.getEndRangeInUnits());
        assertEquals(50, price.getIncrementInterval());
        assertEquals(Unit.GB_MONTH, price.getUnit());
        return "";
    }
}
