package com.vmturbo.action.orchestrator.market;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

/**
 * Configuration for integration with the {@link MarketComponentClient}.
 */
@Configuration
@Import({
    ActionOrchestratorApiConfig.class,
    ActionStoreConfig.class,
    ActionExecutionConfig.class,
    MarketClientConfig.class
})
public class MarketConfig {


    @Autowired
    private ActionOrchestratorApiConfig apiConfig;

    @Autowired
    private ActionExecutionConfig executionConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Bean
    public MarketActionListener marketActionListener() {
        return new MarketActionListener(apiConfig.actionOrchestratorNotificationSender(),
                actionStoreConfig.actionStorehouse());
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent();
        market.addActionsListener(marketActionListener());
        return market;
    }
}
