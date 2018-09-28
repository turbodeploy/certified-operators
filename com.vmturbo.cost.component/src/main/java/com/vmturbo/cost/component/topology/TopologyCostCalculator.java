package com.vmturbo.cost.component.topology;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;

/**
 * The {@link TopologyCostCalculator} is responsible for calculating the
 * costs of entities in the realtime topology using the cost calculation library.
 */
public class TopologyCostCalculator {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

    private final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory;

    private final LocalCostDataProvider localCostDataProvider;

    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

    private final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory;

    public TopologyCostCalculator(@Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor,
            @Nonnull final CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory,
            @Nonnull final LocalCostDataProvider localCostDataProvider,
            @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
            @Nonnull final ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory) {
        this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor);
        this.cloudCostCalculatorFactory = Objects.requireNonNull(cloudCostCalculatorFactory);
        this.localCostDataProvider = Objects.requireNonNull(localCostDataProvider);
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
        this.riApplicatorFactory = Objects.requireNonNull(riApplicatorFactory);
    }

    /**
     * Calculate the costs of the input {@link TopologyEntityDTO}s.
     *
     * @param cloudTopology The cloud entities in a topology.
     * @return A map of id -> {@link CostJournal} for the entity with that ID.
     */
    public Map<Long, CostJournal<TopologyEntityDTO>> calculateCosts(final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final CloudCostCalculator<TopologyEntityDTO> costCalculator;
        try {
            costCalculator = cloudCostCalculatorFactory.newCalculator(localCostDataProvider,
                    cloudTopology, topologyEntityInfoExtractor,
                    discountApplicatorFactory, riApplicatorFactory);
            return cloudTopology.getEntities().values().stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, costCalculator::calculateCost));
        } catch (CloudCostDataRetrievalException e) {
            logger.error("Failed to retrieve cloud cost data. Not doing any cloud cost calculation.", e);
            return Collections.emptyMap();
        }
    }

}