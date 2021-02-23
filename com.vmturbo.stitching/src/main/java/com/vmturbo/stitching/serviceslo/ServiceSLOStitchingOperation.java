package com.vmturbo.stitching.serviceslo;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy;

/**
 * A stitching operation that stitches Service entities from APM or Custom probes with Cloud Native probe.
 */
public class ServiceSLOStitchingOperation extends StringsToStringsDataDrivenStitchingOperation {
    /**
     * Create an instance of {@link ServiceSLOStitchingOperation} class.
     */
    public ServiceSLOStitchingOperation() {
        super(new StringsToStringsStitchingMatchingMetaData(EntityType.SERVICE,
                    new MergedEntityMetadataBuilder()
                            .internalMatchingProperty("IP", ",")
                            .externalMatchingProperty("IP", ",")
                            .mergedSoldCommodity(CommodityType.RESPONSE_TIME)
                            .mergedSoldCommodity(CommodityType.TRANSACTION)
                            .mergedSoldCommodity(CommodityType.APPLICATION, true)
                            .mergedBoughtCommodity(EntityType.APPLICATION_COMPONENT,
                                    ImmutableList.of(CommodityType.TRANSACTION,
                                            CommodityType.RESPONSE_TIME))
                            .build()),
                ImmutableSet.of(ProbeCategory.CLOUD_NATIVE)
        );
    }

    @Override
    @Nonnull
    public MergeCommoditySoldStrategy getMergeCommoditySoldStrategy() {
        return new ServiceSLOMergeCommoditySoldStrategy(
                getCommoditiesSoldToPatch());
    }

    /**
     * A merging strategy for the SLO commodities sold by Service entities.
     */
    public static class ServiceSLOMergeCommoditySoldStrategy extends MetaDataAwareMergeCommoditySoldStrategy {
        /**
         * Create an instance of {@link ServiceSLOMergeCommoditySoldStrategy} class.
         * @param soldMetaData a collection of {@link CommoditySoldMetadata}
         */
        public ServiceSLOMergeCommoditySoldStrategy(@Nonnull final Collection<CommoditySoldMetadata> soldMetaData) {
            super(soldMetaData);
        }

        @Nonnull
        @Override
        public Optional<Builder> onOverlappingCommodity(@Nonnull final Builder fromCommodity,
                                                        @Nonnull final Builder ontoCommodity) {
            return getCommoditySoldMergeSpec(fromCommodity.getCommodityType())
                    .map(soldSpec -> Optional.of(ontoCommodity.setUsed(
                            ontoCommodity.getUsed() + fromCommodity.getUsed())))
                    .orElse(Optional.of(ontoCommodity));
        }
    }
}
