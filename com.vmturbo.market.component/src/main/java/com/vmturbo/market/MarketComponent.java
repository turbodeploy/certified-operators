package com.vmturbo.market;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.topology.PlanOrchestratorConfig;
import com.vmturbo.market.topology.TopologyListenerConfig;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxThrottlingLimit;

/**
 * Component wrapping the Market implementation.
 */
@Configuration("theComponent")
@Import({
    MarketGlobalConfig.class,
    TopologyListenerConfig.class,
    PlanOrchestratorConfig.class,
    MarketApiConfig.class,
    SpringSecurityConfig.class
})
public class MarketComponent extends BaseVmtComponent {

    @Autowired
    private MarketApiConfig marketApiConfig;

    @Autowired
    private MarketGlobalConfig marketGlobalConfig;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Value("${defaultTraxCalculationsTrackedPerDay:360}")
    private int defaultTraxCalculationsTrackedPerDay;
    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(MarketComponent.class);
    }

    @PostConstruct
    private void setup() {
        // add kafka producer health check
        getHealthMonitor().addHealthCheck(marketApiConfig.messageProducerHealthMonitor());

        // Configure Trax to randomly sample a certain number of calculations per day for
        // tracking and debugging purposes.
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .build(),
            new TraxThrottlingLimit(defaultTraxCalculationsTrackedPerDay, Clock.systemUTC(), new Random()),
            Collections.singletonList(TraxConfiguration.DEFAULT_TOPIC_NAME)));
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(marketGlobalConfig.traxConfigurationRpcService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }
}
