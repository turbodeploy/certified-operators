package com.vmturbo.extractor.export;

import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Factory for creating different extractors used in data extraction.
 */
public class DataExtractionFactory {

    /**
     * Create an instance of {@link AttrsExtractor}.
     *
     * @return an instance of {@link AttrsExtractor}
     */
    public AttrsExtractor newAttrsExtractor() {
        return new AttrsExtractor();
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
     * @param graph containing whole topology
     * @param supplyChain containing supply chain info for all entities
     * @param groupData containing all groups related data
     * @return {@link RelatedEntitiesExtractor}
     */
    public RelatedEntitiesExtractor newRelatedEntitiesExtractor(
            final TopologyGraph<SupplyChainEntity> graph, final SupplyChain supplyChain,
            final GroupData groupData) {
        return new RelatedEntitiesExtractor(graph, supplyChain, groupData);
    }
}
