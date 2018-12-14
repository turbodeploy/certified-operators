package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.Sets;

import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig.Subscription;

public class MarketListenerConfig {

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Bean
    public MarketComponent marketComponent() {
        return marketClientConfig.marketComponent(
                Sets.newHashSet(Subscription.ProjectedEntityCosts,
                        Subscription.ProjectedEntityRiCoverage));
    }
}
