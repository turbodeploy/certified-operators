package com.vmturbo.platform.analysis.drivers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.WebsocketServerTransport;
import com.vmturbo.communication.WebsocketServerTransportManager;

/**
 * This class represents Spring configuration for analysis server.
 *
 * @author weiduan
 */
public class AnalysisServerConfig implements AutoCloseable {

    /**
     * Path to analysis server websocket endpoint.
     */
    private static final String ANALYSIS_SERVER_PATH = "/server";

    // the thread pool used for analysis server websocket endpoint
    private final ExecutorService analysisServerThreadPool;
    // create a thread pool to process the message received from M2
    private static int NUM_OF_THREAD = 5;
    private final ExecutorService analysisRequestProcessorThreadPool;

    public AnalysisServerConfig(@Nonnull ServerContainer serverContainer)
            throws DeploymentException {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("analysis-server-ws-%d").build();
        analysisServerThreadPool = Executors.newFixedThreadPool(NUM_OF_THREAD, threadFactory);
        analysisRequestProcessorThreadPool = Executors.newFixedThreadPool(NUM_OF_THREAD,
                new ThreadFactoryBuilder().setNameFormat("analysis-server-ws-processor-%d")
                        .build());

        IdentityGenerator.initPrefix(IdentityGenerator.MAXPREFIX);
        final AnalysisServer analysisServer =
                new AnalysisServer(analysisRequestProcessorThreadPool);
        final WebsocketServerTransportManager transportManager =
                new WebsocketServerTransportManager(
                        new WebsocketServerTransportManager.TransportHandler() {
                            @Override
                            public void onNewTransport(WebsocketServerTransport transport) {
                                final AnalysisServerProtobufEndPoint endpoint =
                                        new AnalysisServerProtobufEndPoint(transport);
                                analysisServer.registerEndpoint(endpoint);
                            }
                        }, analysisServerThreadPool, 30L);

        final Configurator configurator = new Configurator() {
            @Override
            public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                @SuppressWarnings("unchecked") final T result = (T)transportManager;
                return result;
            }
        };
        // Add echo endpoint to analysis server container
        final ServerEndpointConfig endpointConfig =
                ServerEndpointConfig.Builder.create(WebsocketServerTransportManager.class,
                        ANALYSIS_SERVER_PATH).configurator(configurator).build();
        serverContainer.addEndpoint(endpointConfig);
    }

    @Override
    public void close() {
        analysisServerThreadPool.shutdownNow();
        analysisRequestProcessorThreadPool.shutdownNow();
    }
}
