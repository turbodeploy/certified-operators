package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.common.storage.StorageTier;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Abstract StorageTierMeterProcessing.
 *
 * @param <E> ProbeStageEnum.
 */
public abstract class AbstractStorageTierMeterProcessingStage<E extends ProbeStageEnum> extends AbstractMeterProcessingStage<E> {

    private static final String AZURE_STORAGE_PREFIX = "azure::ST::";

    /**
     * Set of {@link StorageTier} the StorageTierMeter will handle.
     */
    private Set<StorageTier> storageTierSkuIds;

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     * @param storageTierSkuIds list of {@link StorageTier} which the stage will process.
     * @param logger Logger
     */
    public AbstractStorageTierMeterProcessingStage(@NotNull E probeStage,
            @NotNull Set<StorageTier> storageTierSkuIds,
            @Nonnull Logger logger) {
        super(probeStage, MeterType.Storage, logger);
        this.storageTierSkuIds = storageTierSkuIds;
    }

    @Nonnull
    String formatStorageTierId(@Nonnull final String localStorageTierName) {
        return AZURE_STORAGE_PREFIX + localStorageTierName.toUpperCase();
    }

    @NotNull
    @Override
    String addPricingForResolvedMeters(
            @NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters) {

        final Predicate<ResolvedMeter> selectResolvedMeterPredicate = resolvedMeter -> storageTierSkuIds.stream()
                .map(st -> st.toString())
                .collect(Collectors.toList())
                .contains(resolvedMeter.getDescriptor().getSkus().get(0));

        final Map<Boolean, List<ResolvedMeter>> separatedResolvedMeters =
                resolvedMeters.stream().collect(Collectors.partitioningBy(selectResolvedMeterPredicate));

        pricingWorkspace.addResolvedMeterByMeterType(MeterType.Storage, separatedResolvedMeters.get(Boolean.FALSE));

        return this.processSelectedMeters(pricingWorkspace, separatedResolvedMeters.get(Boolean.TRUE));
    }

    abstract String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace, @NotNull List<ResolvedMeter> resolvedMeters);
}
