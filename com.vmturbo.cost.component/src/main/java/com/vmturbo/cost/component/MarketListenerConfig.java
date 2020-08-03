package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.market.component.api.impl.MarketSubscription.Topic;


public class MarketListenerConfig {

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Bean
    public MarketComponent marketComponent() {
        return marketClientConfig.marketComponent(
            MarketSubscription.forTopic(Topic.ProjectedEntityCosts),
            MarketSubscription.forTopic(Topic.ProjectedEntityRiCoverage),
            MarketSubscription.forTopic(Topic.ProjectedTopologies),
            MarketSubscription.forTopic(Topic.PlanAnalysisTopologies));
    }

}
