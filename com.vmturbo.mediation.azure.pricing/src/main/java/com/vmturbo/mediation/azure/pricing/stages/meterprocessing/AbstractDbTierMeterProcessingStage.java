package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.enums.DeploymentType;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * Abstract DbTierMeterProcessing.
 *
 * @param <E> ProbeStageEnum.
 */
public abstract class AbstractDbTierMeterProcessingStage<E extends ProbeStageEnum>
        extends AbstractMeterProcessingStage<E> {
    /**
     * Prefix used for entity IDs for DB profiles.
     */
    public static final String AZURE_DBPROFILE_ID_PREFIX = "azure::DBPROFILE::";

    private final Logger logger;

    private final FromContext<Map> dbStoragePriceFromContext = requiresFromContext(
            PricingPipelineContextMembers.DB_STORAGE_PRICE_MAP);

    private Map<String, Map<String, Map<String, Map<DeploymentType, List<Price>>>>>
            dBStoragePriceMap = null;

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     * @param meterType {@link MeterType} which the class is processing. Could be
     *         MeterType.DB or MeterType.DTU.
     * @param logger {@link Logger}.
     */
    public AbstractDbTierMeterProcessingStage(@NotNull E probeStage, @NotNull MeterType meterType,
            @NotNull Logger logger) {
        super(probeStage, meterType, logger);
        this.logger = logger;
    }

    @NotNull
    @Override
    String addPricingForResolvedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters) {
        dBStoragePriceMap = dbStoragePriceFromContext.get();
        return this.processSelectedMeters(pricingWorkspace, resolvedMeters);
    }

    abstract String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @NotNull Collection<ResolvedMeter> resolvedMeters);

    /**
     * returns a list of Price {@link Price} for for the DBStorage based on the input parameters.
     *
     * @param planId the planID.
     * @param region the region.
     * @param tierName the tier name.
     * @param deployment DeploymentType {@link  DeploymentType}
     * @return a list of Price.
     */
    List<Price> getDbStoragePriceList(@Nonnull String planId, @Nonnull String region,
        @Nonnull String tierName, @Nonnull DeploymentType deployment) {
        tierName = tierName.toLowerCase();

        if (dBStoragePriceMap == null || dBStoragePriceMap.isEmpty()) {
            logger.warn("DB Storage price map is empty");
            return Collections.emptyList();
        } else {
            if (dBStoragePriceMap.get(planId) != null && dBStoragePriceMap.get(planId).get(
                    region) != null && dBStoragePriceMap.get(planId).get(region).get(tierName) != null
                    && dBStoragePriceMap.get(planId).get(region).get(tierName).get(deployment)
                    != null) {
                return dBStoragePriceMap.get(planId).get(region).get(tierName).get(deployment);
            } else {
                logger.info(
                        "DBStorage price list not available for planID ={}, region = {}, tierName = {}, deployment = {}",
                        planId, region, tierName, deployment);
                return Collections.emptyList();
            }
        }
    }

    /**
     * Create a DatabaseTierConfigPrice for the base price for a plain SQL Server database,
     * as used in DTU databases.
     *
     * @param price the base price
     * @return a DatabaseTierConfigPrice representing that price
     */
    public DatabaseTierConfigPrice.Builder createBasePrice(@Nonnull Price price) {
        DatabaseTierConfigPrice.Builder builder = DatabaseTierConfigPrice.newBuilder()
        .setDbEngine(DatabaseEngine.SQLSERVER)
        .setDbEdition(DatabaseEdition.NONE)
        .addPrices(price);

        return builder;
    }
}