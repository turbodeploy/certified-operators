package com.vmturbo.extractor.export;

import java.util.Optional;

import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Factory for creating different extractors used in data extraction.
 */
public class DataExtractionFactory {

    private final DataProvider dataProvider;
    private final ThinTargetCache targetCache;

    /**
     * Constructor for {@link DataExtractionFactory}.
     *
     * @param dataProvider a provider of topology-wide data
     * @param targetCache cache for all targets in the system
     */
    public DataExtractionFactory(DataProvider dataProvider, ThinTargetCache targetCache) {
        this.dataProvider = dataProvider;
        this.targetCache = targetCache;
    }

    /**
     * Create an instance of {@link PrimitiveFieldsOnTEDPatcher} which is capable of extracting
     * attrs from an entity.
     *
     * @return an instance of {@link PrimitiveFieldsOnTEDPatcher}
     */
    public PrimitiveFieldsOnTEDPatcher newAttrsExtractor() {
        return new PrimitiveFieldsOnTEDPatcher(PatchCase.EXPORTER, newTargetsExtractor());
    }

    /**
     * Create an instance of {@link MetricsExtractor}.
     * @return an instance of {@link MetricsExtractor}
     */
    public MetricsExtractor newMetricsExtractor() {
        return new MetricsExtractor();
    }

    /**
     * Create a new instance of {@link RelatedEntitiesExtractor}.
     *
     * @return optional of {@link RelatedEntitiesExtractor}
     */
    public Optional<RelatedEntitiesExtractor> newRelatedEntitiesExtractor() {
        TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        SupplyChain supplyChain = dataProvider.getSupplyChain();
        if (topologyGraph == null || supplyChain == null) {
            return Optional.empty();
        }
        return Optional.of(new RelatedEntitiesExtractor(topologyGraph, supplyChain,
                dataProvider.getGroupData()));
    }

    /**
     * Create a new instance of {@link TopDownCostExtractor}.
     *
     * @return The {@link TopDownCostExtractor} if the data provider contains sufficient data
     *         to extract top-down costs, or an empty optional.
     */
    public Optional<TopDownCostExtractor> newTopDownCostExtractor() {
        TopDownCostData topDownCostData = dataProvider.getTopDownCostData();
        TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topDownCostData != null && topologyGraph != null) {
            return Optional.of(new TopDownCostExtractor(topDownCostData, topologyGraph));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Create a new instance of {@link BottomUpCostExtractor}.
     *
     * @return The {@link BottomUpCostExtractor} if the data provider contains sufficient data
     *         to extract bottom up costs, or an empty optional.
     */
    public Optional<BottomUpCostExtractor> newBottomUpCostExtractor() {
        BottomUpCostData bottomUpCostData = dataProvider.getBottomUpCostData();
        if (bottomUpCostData != null) {
            return Optional.of(new BottomUpCostExtractor(bottomUpCostData));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Create an instance of {@link GroupPrimitiveFieldsOnGroupingPatcher} which is capable of extracting
     * attrs from a group.
     *
     * @return an instance of {@link GroupPrimitiveFieldsOnGroupingPatcher}
     */
    public GroupPrimitiveFieldsOnGroupingPatcher newGroupAttrsExtractor() {
        return new GroupPrimitiveFieldsOnGroupingPatcher(PatchCase.EXPORTER, newTargetsExtractor());
    }

    /**
     * Create an instance of {@link TargetsExtractor} which is capable of extracting targets info
     * for an entity.
     *
     * @return an instance of {@link TargetsExtractor}
     */
    public TargetsExtractor newTargetsExtractor() {
        return new TargetsExtractor(targetCache, dataProvider.getTopologyGraph());
    }
}
