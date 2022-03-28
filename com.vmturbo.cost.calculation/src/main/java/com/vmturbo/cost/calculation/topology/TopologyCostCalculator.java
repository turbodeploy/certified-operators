package com.vmturbo.cost.calculation.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.CloudCommitmentApplicator;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.SimulatedCloudCostData;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cloud.common.topology.CloudTopology;

/**
 * The {@link TopologyCostCalculator} is responsible for calculating the
 * costs of entities in the realtime topology using the cost calculation library.
 */
public class TopologyCostCalculator {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

    private final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory;

    private final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory;

    private final CloudCommitmentApplicator.CloudCommitmentApplicatorFactory<TopologyEntityDTO> cloudCommitmentApplicatorFactory;

    private final CloudCostData cloudCostData;

    private final CloudTopology<TopologyEntityDTO> cloudTopo;

    private final TopologyInfo topoInfo;

    private TopologyCostCalculator(@Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                  @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
                  @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                  @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                  @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory,
                  @Nonnull final CloudCommitmentApplicator.CloudCommitmentApplicatorFactory<TopologyEntityDTO> cloudCommitmentApplicatorFactory,
                  @Nonnull final TopologyInfo topoInfo, @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor);
        this.cloudCostCalculatorFactory = Objects.requireNonNull(cloudCostCalculatorFactory);
        this.riApplicatorFactory = Objects.requireNonNull(riApplicatorFactory);
        this.cloudCommitmentApplicatorFactory = Objects.requireNonNull(cloudCommitmentApplicatorFactory);
        this.topoInfo = Objects.requireNonNull(topoInfo);
        this.cloudTopo = Objects.requireNonNull(cloudTopology);
        CloudCostData cloudCostData;
        try {
            cloudCostData = cloudCostDataProvider.getCloudCostData(topoInfo, this.cloudTopo, topologyEntityInfoExtractor);
        } catch (CloudCostDataRetrievalException e) {
            logger.error("Failed to fetch cloud cost data. Error: {}.\n Using empty (no costs)",
                    e.getLocalizedMessage());
            cloudCostData = CloudCostData.empty();
        }
        this.cloudCostData = Objects.requireNonNull(cloudCostData);
    }

    /**
     * Calculate the costs of the input {@link TopologyEntityDTO}s.
     *
     * @param cloudTopology The cloud entities in a topology.
     * @return A map of id -> {@link CostJournal} for the entity with that ID.
     */
    @Nonnull
    public Map<Long, CostJournal<TopologyEntityDTO>> calculateCosts(final CloudTopology<TopologyEntityDTO> cloudTopology) {
        logger.info("Starting cost calculation on cloud topology of size: {}", cloudTopology.size());
        return calculateCosts(cloudTopology, cloudCostData.getCurrentRiCoverage());
    }

    /**
     * Calculate the costs of the input {@link TopologyEntityDTO}s.
     *
     * @param cloudTopology The cloud entities in a topology.
     * @return A map of id -> {@link CostJournal} for the entity with that ID.
     */
    @Nonnull
    public Map<Long, CostJournal<TopologyEntityDTO>> calculateCosts(final CloudTopology<TopologyEntityDTO> cloudTopology,
                                                                    final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage) {
        logger.info("Starting cost calculation on cloud topology of size: {}", cloudTopology.size());
        return calculateCostsInTopology(cloudTopology.getEntities().values(), cloudTopology,
                    projectedRiCoverage, Collections.emptyMap());
    }

    /**
     * Calculate the costs of the input {@link TopologyEntityDTO}s.
     *
     * @param cloudTopology The cloud entities in a topology.
     * @param projectedRiCoverage the RI Coverage for the projected topology
     * @param commitmentMappings the commitment mappings that need to be overridden in cloud cost data,
     * @return A map of id -> {@link CostJournal} for the entity with that ID.
     */
    @Nonnull
    public Map<Long, CostJournal<TopologyEntityDTO>> calculateCosts(final CloudTopology<TopologyEntityDTO> cloudTopology,
            final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage,
            Map<Long, Set<CloudCommitmentMapping>> commitmentMappings) {
        logger.info("Starting cost calculation on cloud topology of size: {}", cloudTopology.size());
        return calculateCostsInTopology(cloudTopology.getEntities().values(), cloudTopology,
                projectedRiCoverage, commitmentMappings);
    }

    /**
     * Calculate the costs of the input {@link TopologyEntityDTO}s.
     *
     * @param cloudTopology The cloud entities in a topology.
     * @param cloudEntity The cloud entity for which cost is to be calculated.
     * @return {@link CostJournal} for the cloudEntity.
     */
    @Nonnull
    public Optional<CostJournal<TopologyEntityDTO>> calculateCostForEntity(
            final CloudTopology<TopologyEntityDTO> cloudTopology,
            final TopologyEntityDTO cloudEntity) {
        final List<TopologyEntityDTO> entities = Lists.newArrayList(cloudEntity);
        // If the entity has attached volumes we need to calculate costs for them too, or
        // else we won't be able to get the storage cost for the entity.
        entities.addAll(cloudTopology.getAttachedVolumes(cloudEntity.getOid()));
        final Map<Long, CostJournal<TopologyEntityDTO>> costsForEntities =
                calculateCostsInTopology(entities, cloudTopology,
                        cloudCostData.getCurrentRiCoverage(), Collections.emptyMap());
        return Optional.ofNullable(costsForEntities.get(cloudEntity.getOid()));
    }

    /*
     * Use this method when you need a cost data consistent with the one used by the calculator.
     *
     * @return The {@link CloudCostData} this calculator is using to compute costs.
     */
    @Nonnull
    public CloudCostData getCloudCostData() {
        return cloudCostData;
    }

    @Nonnull
    private Map<Long, CostJournal<TopologyEntityDTO>> calculateCostsInTopology(
            final Collection<TopologyEntityDTO> entities,
            final CloudTopology<TopologyEntityDTO> cloudTopology,
            final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage,
            final Map<Long, Set<CloudCommitmentMapping>> commitmentMappings) {
        final CloudCostCalculator<TopologyEntityDTO> costCalculator;
        final Map<Long, CostJournal<TopologyEntityDTO>> retCosts = new HashMap<>(cloudTopology.size());
        final DependentCostLookup<TopologyEntityDTO> dependentCostLookup = entity -> retCosts.get(entity.getOid());
        CloudCostData costData = cloudCostData;
        if (!commitmentMappings.isEmpty()) {
            costData = new SimulatedCloudCostData(cloudCostData);
            Builder<Object, Object> mappingMultiMapBuilder =
                    ImmutableSetMultimap.builder();
            commitmentMappings.entrySet().stream().forEach(e ->
                e.getValue().stream().forEach(mapping ->
                    mappingMultiMapBuilder.put(e.getKey(), mapping)));
            ((SimulatedCloudCostData)costData).setCloudCommitmentMappingByEntityId(mappingMultiMapBuilder.build());
        }
        costCalculator = cloudCostCalculatorFactory.newCalculator(costData,
                cloudTopology,
                topologyEntityInfoExtractor,
                riApplicatorFactory,
                cloudCommitmentApplicatorFactory,
                dependentCostLookup,
                topologyRICoverage);
        cloudCostData.logMissingAccountPricingData();
        entities.forEach(entity -> {
            retCosts.put(entity.getOid(), costCalculator.calculateCost(entity));
        });
        return retCosts;
    }

    /**
     * Factory for instances of {@link TopologyCostCalculator}.
     */
    public interface TopologyCostCalculatorFactory {

        /**
         * Create a new {@link TopologyCostCalculator} with fresh cost-related data from
         * the {@link CloudCostDataProvider} used by the factory.
         *
         * @param topologyInfo the topology info.
         * @param originalCloudTopology The cloud topology.
         *
         * @return A {@link TopologyCostCalculator}.
         */
        @Nonnull
        TopologyCostCalculator newCalculator(TopologyInfo topologyInfo, CloudTopology<TopologyEntityDTO> originalCloudTopology);

        /**
         * The default implementation of {@link TopologyCostCalculatorFactory}, for use in "real"
         * code.
         */
        class DefaultTopologyCostCalculatorFactory implements TopologyCostCalculatorFactory {

            private final CloudCostDataProvider cloudCostDataProvider;

            private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

            private final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory;

            private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

            private final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory;

            private final CloudCommitmentApplicator.CloudCommitmentApplicatorFactory<TopologyEntityDTO> cloudCommitmentApplicatorFactory;

            public DefaultTopologyCostCalculatorFactory(
                    @Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                    @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
                    @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                    @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                    @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory,
                    @Nonnull final CloudCommitmentApplicator.CloudCommitmentApplicatorFactory<TopologyEntityDTO> cloudCommitmentApplicatorFactory) {
                this.topologyEntityInfoExtractor = topologyEntityInfoExtractor;
                this.cloudCostCalculatorFactory = cloudCostCalculatorFactory;
                this.cloudCostDataProvider = cloudCostDataProvider;
                this.discountApplicatorFactory = discountApplicatorFactory;
                this.riApplicatorFactory = riApplicatorFactory;
                this.cloudCommitmentApplicatorFactory = Objects.requireNonNull(cloudCommitmentApplicatorFactory);
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public TopologyCostCalculator newCalculator(@Nonnull TopologyInfo topoInfo, final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
                return new TopologyCostCalculator(topologyEntityInfoExtractor,
                        cloudCostCalculatorFactory,
                        cloudCostDataProvider,
                        discountApplicatorFactory,
                        riApplicatorFactory,
                        cloudCommitmentApplicatorFactory,
                        topoInfo, originalCloudTopology);
            }

        }
    }

}
