package com.vmturbo.plan.orchestrator;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.impl.CostSubscription;
import com.vmturbo.cost.api.impl.CostSubscription.Topic;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;

/**
 * Top-level configuration for common things (e.g. Spring config, Swagger config)
 * that do not belong in any specific package.
 */
@Configuration
@Import({MarketClientConfig.class,
    TopologyProcessorClientConfig.class,
    CostClientConfig.class})
public class GlobalConfig {
    @Value("${identityGeneratorPrefix:3}")
    private long identityGeneratorPrefix;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    /**
     * Initializes the identity generator in the plan orchestrator's Spring context.
     *
     * @return The {@link IdentityInitializer}.
     */
    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    /**
     * GSON HTTP converter configured to support swagger.
     * (see: http://stackoverflow.com/questions/30219946/springfoxswagger2-does-not-work-with-gsonhttpmessageconverterconfig/30220562#30220562)
     *
     * @return The {@link GsonHttpMessageConverter}.
     */
    @Bean
    public GsonHttpMessageConverter gsonHttpMessageConverter() {
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        return msgConverter;
    }

    /**
     * Topology processor notification client.
     *
     * @return The {@link TopologyProcessor}.
     */
    @Bean
    public TopologyProcessor tpNotificationClient() {
        return tpClientConfig.topologyProcessor(TopologyProcessorSubscription.forTopic(
            TopologyProcessorSubscription.Topic.TopologySummaries));
    }

    /**
     * Cost component notification client.
     *
     * @return The {@link CostComponent}.
     */
    @Bean
    public CostComponent costNotificationClient() {
        return costClientConfig.costComponent(
            CostSubscription.forTopic(Topic.COST_STATUS_NOTIFICATION));
    }

    /**
     * Java Bean for Market component.
     *
     * @return The Market component.
     */
    @Bean
    public MarketComponent marketNotificationClient() {
        final MarketComponent market = marketClientConfig.marketComponent(
            MarketSubscription.forTopic(MarketSubscription.Topic.AnalysisStatusNotification),
            // Read the most recent projected topologies instead of reading from the beginning.
            MarketSubscription.forTopic(MarketSubscription.Topic.ProjectedTopologies));
        return market;
    }

    /**
     * The common clock used throughout this component.
     *
     * @return The {@link Clock}.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
