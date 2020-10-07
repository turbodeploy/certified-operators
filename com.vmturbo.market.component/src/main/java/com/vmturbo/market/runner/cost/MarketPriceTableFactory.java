package com.vmturbo.market.runner.cost;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;

public interface MarketPriceTableFactory {

    @Nonnull
    CloudRateExtractor newPriceTable(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                   @Nonnull CloudCostData cloudCostData);

    class DefaultMarketPriceTableFactory implements MarketPriceTableFactory {

        private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;
        private final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor;

        public DefaultMarketPriceTableFactory(@Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                                              @Nonnull final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor) {
            this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
            this.entityInfoExtractor = Objects.requireNonNull(entityInfoExtractor);
        }

        @Nonnull
        @Override
        public CloudRateExtractor newPriceTable(@Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                                              @Nonnull final CloudCostData cloudCostData) {
            return new CloudRateExtractor(cloudTopology, entityInfoExtractor);
        }
    }
}
