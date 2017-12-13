package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy.Origin;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class CommoditySoldMerger {
    private final MergeCommoditySoldStrategy mergeCommoditySoldStrategy;

    public CommoditySoldMerger(@Nonnull final MergeCommoditySoldStrategy mergeCommoditySoldStrategy) {
        this.mergeCommoditySoldStrategy = Objects.requireNonNull(mergeCommoditySoldStrategy);
    }

    public MergeCommoditySoldStrategy getMergeCommoditySoldStrategy() {
        return mergeCommoditySoldStrategy;
    }

    public List<CommoditySold> mergeCommoditiesSold(@Nonnull final List<CommoditySold> mergeFromCommodities,
                                                    @Nonnull final List<CommoditySold> mergeOntoCommodities) {
        // Collect the mergeFromCommodities into a map where they can be looked up by
        // {@link CommodityBuilderIdentifier}.
        final Map<CommodityBuilderIdentifier, CommoditySold> mergeFromCommoditiesMap =
            mergeFromCommodities.stream().collect(Collectors.toMap(
                commodity -> new CommodityBuilderIdentifier(commodity.sold.getCommodityType(), commodity.sold.getKey()),
                Function.identity()));

        final List<CommoditySold> mergedCommodities =
            new ArrayList<>(Math.max(mergeFromCommodities.size(), mergeOntoCommodities.size()));

        for (CommoditySold ontoCommodity : mergeOntoCommodities) {
            final CommodityBuilderIdentifier id = new CommodityBuilderIdentifier(
                ontoCommodity.sold.getCommodityType(), ontoCommodity.sold.getKey());

            @Nullable final CommoditySold fromCommodity = mergeFromCommoditiesMap.remove(id);
            final Optional<CommodityDTO.Builder> merged = fromCommodity == null
                    ? mergeCommoditySoldStrategy.onDistinctCommodity(ontoCommodity.sold, Origin.ONTO_ENTITY)
                    : mergeCommoditySoldStrategy.onOverlappingCommodity(fromCommodity.sold, ontoCommodity.sold);
            merged.ifPresent(commoditySoldBuilder ->
                mergedCommodities.add(new CommoditySold(commoditySoldBuilder, ontoCommodity.accesses)));
        }

        mergeFromCommoditiesMap.values().forEach(fromCommodity ->
            mergeCommoditySoldStrategy.onDistinctCommodity(fromCommodity.sold, Origin.FROM_ENTITY)
                .ifPresent(fromBuilder -> mergedCommodities.add(
                    new CommoditySold(fromBuilder, fromCommodity.accesses))));

        return mergedCommodities;
    }
}
