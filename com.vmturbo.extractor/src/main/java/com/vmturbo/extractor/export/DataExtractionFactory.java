package com.vmturbo.extractor.export;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private final Logger logger = LogManager.getLogger();

    private final DataProvider dataProvider;
    private final ThinTargetCache targetCache;
    private final ExtractorKafkaSender extractorKafkaSender;

    /**
     * Last time when entities are extracted successfully.
     */
    private final MutableLong lastEntityExtractionTime = new MutableLong(0);
    private final Long entityExtractionIntervalMillis;
    private final Clock clock;

    /**
     * Constructor for {@link DataExtractionFactory}.
     *
     * @param dataProvider a provider of topology-wide data
     * @param targetCache cache for all targets in the system
     * @param extractorKafkaSender for sending entities to kafka
     * @param entityExtractionIntervalMillis the interval for extracting entities, may be null
     * @param clock clock
     */
    public DataExtractionFactory(DataProvider dataProvider, ThinTargetCache targetCache,
            ExtractorKafkaSender extractorKafkaSender, Long entityExtractionIntervalMillis,
            Clock clock) {
        this.dataProvider = dataProvider;
        this.targetCache = targetCache;
        this.extractorKafkaSender = extractorKafkaSender;
        this.entityExtractionIntervalMillis = entityExtractionIntervalMillis;
        this.clock = clock;
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

    /**
     * Create an instance of {@link DataExtractionWriter}. If the extraction interval has not
     * passed when this method is called, it will return null.
     *
     * @return {@link DataExtractionWriter}
     */
    @Nullable
    public DataExtractionWriter newDataExtractionWriter() {
        if (entityExtractionIntervalMillis == null) {
            // no interval defined, default to broadcast schedule, always extract
            return new DataExtractionWriter(extractorKafkaSender, this);
        }
        final long now = clock.millis();
        final long nextExtractionTime = lastEntityExtractionTime.longValue() + entityExtractionIntervalMillis;
        if (nextExtractionTime <= now) {
            return new DataExtractionWriter(extractorKafkaSender, this);
        }
        logger.info("Not extracting entities/groups for another {} minutes.",
                TimeUnit.MILLISECONDS.toMinutes(nextExtractionTime - now));
        return null;
    }

    /**
     * Update the last time when the topology is extracted successfully.
     *
     * @param extractionTime topology extraction time, in millis
     */
    public void updateLastExtractionTime(long extractionTime) {
        lastEntityExtractionTime.setValue(extractionTime);
    }
}
