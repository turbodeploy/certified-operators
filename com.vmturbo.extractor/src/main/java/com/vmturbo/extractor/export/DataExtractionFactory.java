package com.vmturbo.extractor.export;

import java.util.Optional;

import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
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
}
