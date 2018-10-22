package com.vmturbo.cost.calculation.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The {@link TopologyCostCalculator} is responsible for calculating the
 * costs of entities in the realtime topology using the cost calculation library.
 */
public class TopologyCostCalculator {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

    private final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory;

    private final CloudCostDataProvider cloudCostDataProvider;

    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

    private final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory;

    public TopologyCostCalculator(@Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                                  @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
                                  @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                                  @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                                  @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory) {
        this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor);
        this.cloudCostCalculatorFactory = Objects.requireNonNull(cloudCostCalculatorFactory);
        this.cloudCostDataProvider = Objects.requireNonNull(cloudCostDataProvider);
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
        this.riApplicatorFactory = Objects.requireNonNull(riApplicatorFactory);
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
        return calculateCostsInTopology(cloudTopology.getEntities().values(), cloudTopology);
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
        // If the entity has connected volumes we need to calculate costs for them too, or
        // else we won't be able to get the storage cost for the entity.
        entities.addAll(cloudTopology.getConnectedVolumes(cloudEntity.getOid()));
        final Map<Long, CostJournal<TopologyEntityDTO>> costsForEntities =
                calculateCostsInTopology(entities, cloudTopology);
        return Optional.ofNullable(costsForEntities.get(cloudEntity.getOid()));
    }

    @Nonnull
    private Map<Long, CostJournal<TopologyEntityDTO>> calculateCostsInTopology(
            final Collection<TopologyEntityDTO> entities,
            final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final CloudCostCalculator<TopologyEntityDTO> costCalculator;
        try {
            final Map<Long, CostJournal<TopologyEntityDTO>> retCosts = new HashMap<>(cloudTopology.size());
            final DependentCostLookup<TopologyEntityDTO> dependentCostLookup = entity -> retCosts.get(entity.getOid());
            costCalculator = cloudCostCalculatorFactory.newCalculator(cloudCostDataProvider,
                    cloudTopology,
                    topologyEntityInfoExtractor,
                    discountApplicatorFactory,
                    riApplicatorFactory,
                    dependentCostLookup);
            entities.forEach(entity -> {
                retCosts.put(entity.getOid(), costCalculator.calculateCost(entity));
            });
            logger.info("Cost calculation completed.");
            return retCosts;
        } catch (CloudCostDataRetrievalException e) {
            logger.error("Failed to retrieve cloud cost data. Not doing any cloud cost calculation.", e);
            return Collections.emptyMap();
        }
    }

}