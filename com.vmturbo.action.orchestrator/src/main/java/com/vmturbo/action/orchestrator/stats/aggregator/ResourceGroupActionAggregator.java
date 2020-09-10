package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;

/**
 * Aggregates action stats for resource group in the cloud.
 */
public class ResourceGroupActionAggregator extends CloudActionAggregator {

    protected ResourceGroupActionAggregator(@Nonnull LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    @Override
    protected Optional<Long> getAggregationEntity(StatsActionViewFactory.StatsActionView action) {
        return action.resourceGroupId();
    }

    @Override
    protected void incrementEntityWithMissedAggregationEntity() {}

    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.RESOURCE_GROUP;
    }

    /**
     * Factory class for {@link ResourceGroupActionAggregator}s.
     */
    public static class ResourceGroupActionAggregatorFactory implements ActionAggregatorFactory<ResourceGroupActionAggregator> {
        /**
         * Constructor for the aggregator factory.
         */
        public ResourceGroupActionAggregatorFactory() {
        }

        @Override
        public ResourceGroupActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new ResourceGroupActionAggregator(snapshotTime);
        }
    }
}
