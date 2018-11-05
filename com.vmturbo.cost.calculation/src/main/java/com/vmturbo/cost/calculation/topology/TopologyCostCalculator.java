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
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
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

    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

    private final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory;

    private final CloudCostData cloudCostData;

    private TopologyCostCalculator(@Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                  @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
                  @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                  @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                  @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory) {
        this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor);
        this.cloudCostCalculatorFactory = Objects.requireNonNull(cloudCostCalculatorFactory);
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
        this.riApplicatorFactory = Objects.requireNonNull(riApplicatorFactory);
        CloudCostData cloudCostData;
        try {
            cloudCostData = cloudCostDataProvider.getCloudCostData();
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
            final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final CloudCostCalculator<TopologyEntityDTO> costCalculator;
        try {
            final Map<Long, CostJournal<TopologyEntityDTO>> retCosts = new HashMap<>(cloudTopology.size());
            final DependentCostLookup<TopologyEntityDTO> dependentCostLookup = entity -> retCosts.get(entity.getOid());
            costCalculator = cloudCostCalculatorFactory.newCalculator(cloudCostData,
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

    /**
     * Factory for instances of {@link TopologyCostCalculator}.
     */
    public interface TopologyCostCalculatorFactory {

        /**
         * Create a new {@link TopologyCostCalculator} with fresh cost-related data from
         * the {@link CloudCostDataProvider} used by the factory.
         *
         * @return A {@link TopologyCostCalculator}.
         */
        @Nonnull
        TopologyCostCalculator newCalculator();

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

            public DefaultTopologyCostCalculatorFactory(
                    @Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                    @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
                    @Nonnull final CloudCostDataProvider cloudCostDataProvider,
                    @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                    @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory) {
                this.topologyEntityInfoExtractor = topologyEntityInfoExtractor;
                this.cloudCostCalculatorFactory = cloudCostCalculatorFactory;
                this.cloudCostDataProvider = cloudCostDataProvider;
                this.discountApplicatorFactory = discountApplicatorFactory;
                this.riApplicatorFactory = riApplicatorFactory;
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public TopologyCostCalculator newCalculator() {
                return new TopologyCostCalculator(topologyEntityInfoExtractor,
                        cloudCostCalculatorFactory,
                        cloudCostDataProvider,
                        discountApplicatorFactory,
                        riApplicatorFactory);
            }
        }
    }

}