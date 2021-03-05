package com.vmturbo.extractor.export;

import java.util.Optional;

import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Factory for creating different extractors used in data extraction.
 */
public class DataExtractionFactory {

    /**
     * Create an instance of {@link PrimitiveFieldsOnTEDPatcher} which is capable of extracting
     * attrs from an entity.
     *
     * @return an instance of {@link PrimitiveFieldsOnTEDPatcher}
     */
    public PrimitiveFieldsOnTEDPatcher newAttrsExtractor() {
        return new PrimitiveFieldsOnTEDPatcher(true, true);
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
     * @param dataProvider providing topology, supply chain, group and more
     * @return optional of {@link RelatedEntitiesExtractor}
     */
    public Optional<RelatedEntitiesExtractor> newRelatedEntitiesExtractor(DataProvider dataProvider) {
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
     * @param dataProvider providing topology and top-down cost data.
     * @return The {@link TopDownCostExtractor} if the data provider contains sufficient data
     *         to extract top-down costs, or an empty optional.
     */
    public Optional<TopDownCostExtractor> newTopDownCostExtractor(DataProvider dataProvider) {
        TopDownCostData topDownCostData = dataProvider.getTopDownCostData();
        TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topDownCostData != null && topologyGraph != null) {
            return Optional.of(new TopDownCostExtractor(topDownCostData, topologyGraph));
        } else {
            return Optional.empty();
        }
    }
}
