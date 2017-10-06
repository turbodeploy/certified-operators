package com.vmturbo.action.orchestrator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Global beans for the component that don't belong in any
 * specific package.
 */
@Configuration
public class ActionOrchestratorGlobalConfig {

    @Value("${topologyProcessorHost}")
    private String topologyProcessorHost;

    @Value("${server.port}")
    private int topologyProcessorPort;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${server.grpcPort}")
    private int topologyProcessorRpcPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    /**
     * This bean performs registration of all configured websocket endpoints.
     *
     * @return bean
     */
    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService tpClientExecutorService() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-tp-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean(destroyMethod = "close")
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor =
                TopologyProcessorClient.rpcAndNotification(connectionConfig(), tpClientExecutorService());
        return topologyProcessor;
    }

    /**
     * The gRPC channel to the Topology Processor.
     * This connection can - and should - be shared by all stubs making calls
     * to the Topology Processor's services.
     *
     * @return The gRPC channel.
     */
    @Bean
    public Channel topologyProcessorChannel() {
        return PingingChannelBuilder.forAddress(topologyProcessorHost, topologyProcessorRpcPort)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
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
}
