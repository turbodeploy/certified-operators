package com.vmturbo.systest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.external.api.TurboApiClient;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Spring Configuration for the System Test suite.
 * Builds upon the com.vmturbo.components.test.utilities classes.
 **/
@Configuration
@Import({TopologyProcessorClientConfig.class})
public class SystemTestConfig {


    // Websocket URL to connect to for notifications, e.g. plan progress updates.
    private static final String WEBSOCKET_URL = "/ws/messages";

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Bean
    public ComponentTestRule componentTestRule() {
        return SystemTestSuite.getComponentTestRule();
    }

    @Bean
    public TurboApiClient externalApiClient() {
        return componentCluster().getExternalApiClient();
    }

    @Bean
    public ComponentCluster componentCluster() {
        return componentTestRule().getCluster();
    }

    @Bean
    URI apiWebsocketUri() {
        ComponentCluster componentCluster = componentCluster();
        try {
            return new URI(String.format("ws://%s:%d%s",
                    componentCluster.getConnectionConfig("api").getHost(),
                    componentCluster.getConnectionConfig("api").getPort(),
                    WEBSOCKET_URL));
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing api websocket URI", e);
        }
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final List<TopologyProcessorSubscription> subscriptionList = Stream.of(Topic.values())
            .map(TopologyProcessorSubscription::forTopic)
            .collect(Collectors.toList());
        return tpClientConfig.topologyProcessor(subscriptionList.toArray(
            new TopologyProcessorSubscription[Topic.values().length]));
    }

    @Bean
    public TopologyServiceGrpc.TopologyServiceBlockingStub topologyService() {
        return TopologyServiceGrpc.newBlockingStub(
                componentCluster().newGrpcChannel("topology-processor"));
    }
}
