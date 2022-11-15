package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.common.storage.StorageTier;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * A fake StorageTierMeterProcessingStage.
 *
 * @param <E> ProbeStageEnum.
 */
public class FakeStorageTierMeterProcessingStage<E extends ProbeStageEnum> extends AbstractStorageTierMeterProcessingStage<E> {

    private static final Set<String> SKU_IDS = Sets.newHashSet(StorageTier.MANAGED_STANDARD.toString());
    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     */
    public FakeStorageTierMeterProcessingStage(@NotNull E probeStage) {
        super(probeStage, SKU_IDS, logger);
    }

    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace, @NotNull List<ResolvedMeter> resolvedMeters) {
        return "";
    }
}
