package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Aggregates action stats for business accounts in the cloud.
 */
public class BusinessAccountActionAggregator extends CloudActionAggregator {

    protected BusinessAccountActionAggregator(@Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    @Override
    public void start() {
    }

    @Override
    protected Optional<Long> getAggregationEntity(StatsActionViewFactory.StatsActionView action) {
        return action.businessAccountId();
    }

    @Override
    protected void incrementEntityWithMissedAggregationEntity() {
        Metrics.MISSING_OWNERS_COUNTER.increment();
    }

    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.BUSINESS_ACCOUNT;
    }

    /**
     * Metrics for {@link BusinessAccountActionAggregator}.
     */
    private static class Metrics {
        private static final DataMetricCounter MISSING_OWNERS_COUNTER = DataMetricCounter.builder()
            .withName("ao_action_ba_agg_missing_owners_count")
            .withHelp("Count of cloud/hybrid entities with missing business account owners.")
            .build()
            .register();
    }

    /**
     * Factory class for {@link BusinessAccountActionAggregator}s.
     */
    public static class BusinessAccountActionAggregatorFactory implements ActionAggregatorFactory<BusinessAccountActionAggregator> {
        /**
         * Constructor for the aggregator factory.
         */
        public BusinessAccountActionAggregatorFactory() {
        }

        @Override
        public BusinessAccountActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new BusinessAccountActionAggregator(snapshotTime);
        }
    }
}
