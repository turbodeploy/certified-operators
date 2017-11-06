package com.vmturbo.systest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.external.api.TurboApiClient;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Spring Configuration for the System Test suite.
 * Builds upon the com.vmturbo.components.test.utilities classes.
 **/
@Configuration
public class SystemTestConfig {


    // Websocket URL to connect to for notifications, e.g. plan progress updates.
    private static final String WEBSOCKET_URL = "/vmturbo/messages";

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
    public ComponentApiConnectionConfig tpConnectionCofig() {
        return componentCluster().getConnectionConfig("topology-processor");
    }

    @Bean
    public KafkaMessageConsumer kafkaConsumer() {
        return new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                "system-test-1");
    }

    @Bean
    public IMessageReceiver<TopologyProcessorNotification> topologyNotificationReceiver() {
        return kafkaConsumer().messageReceiver(TopologyProcessorClient.NOTIFICATIONS_TOPIC,
                TopologyProcessorNotification::parseFrom);
    }

    @Bean
    public IMessageReceiver<Topology> topologyBroadcastReceiver() {
        return kafkaConsumer().messageReceiver(TopologyProcessorClient.TOPOLOGY_BROADCAST_TOPIC,
                Topology::parseFrom);
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return TopologyProcessorClient.rpcAndNotification(
                componentCluster().getConnectionConfig("topology-processor"),
                Executors.newCachedThreadPool(), topologyNotificationReceiver(),
                topologyBroadcastReceiver());
    }

    @Bean
    public TopologyServiceGrpc.TopologyServiceBlockingStub topologyService() {
        return TopologyServiceGrpc.newBlockingStub(
                componentCluster().newGrpcChannel("topology-processor"));
    }
}
