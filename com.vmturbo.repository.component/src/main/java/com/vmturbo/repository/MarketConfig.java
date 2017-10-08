package com.vmturbo.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.repository.listener.MarketTopologyListener;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Configuration for integration with the {@link MarketComponentClient}.
 */
@Configuration
@Import({RepositoryApiConfig.class, MarketClientConfig.class})
public class MarketConfig {

    @Autowired
    private RepositoryApiConfig apiConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    /**
     * This field will be set in the constructor with the depedancy injected by Spring.
     */
    TopologyEventHandler topologyEventHandler;

    TopologyProtobufsManager topologyProtobufsManager;

    @Bean
    public MarketTopologyListener MarketTopologyListener(
                final TopologyEventHandler topologyEventHandler,
                final TopologyProtobufsManager topologprotobufsManager) {
        return new MarketTopologyListener(
            topologyEventHandler,
            apiConfig.repositoryNotificationSender(),
            topologprotobufsManager);
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent();
        market.addProjectedTopologyListener(MarketTopologyListener(topologyEventHandler, topologyProtobufsManager));
        return market;
    }
}
