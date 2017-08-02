package com.vmturbo.platform.analysis.drivers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.platform.analysis.drivers.AnalysisWebsocketServerTransportHandler;

/**
 * This class represents Spring configuration for analysis server.
 * @author weiduan
 *
 */
public class AnalysisServerConfig implements AutoCloseable {

    /**
     * Path to analysis server websocket endpoint.
     */
    private static final String ANALYSIS_SERVER_PATH = "/server";

    // the thread pool used for analysis server websocket enpoint
    private final ExecutorService analysisServerThreadPool;
    // create a thread pool to process the message received from M2
    private static int NUM_OF_THREAD = 5;

    public AnalysisServerConfig() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("analysis-server-%d").build();
        analysisServerThreadPool = Executors.newFixedThreadPool(NUM_OF_THREAD, threadFactory);
    }

    /**
     * Create the websocket endpoint with configuration.
     *
     * @param serverContainer websocket server container
     * @throws DeploymentException if any error occurred while initializing.
     */
    public void init(ServerContainer serverContainer) throws DeploymentException {

        final AnalysisWebsocketServerTransportHandler transportHandler =
                        new AnalysisWebsocketServerTransportHandler();

        final AnalysisServer analysisServer = new AnalysisServer(transportHandler,
                        analysisServerThreadPool);
        transportHandler.attachEndPoint(analysisServer);

        final Configurator configurator = new Configurator() {
            @Override
            public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                @SuppressWarnings("unchecked")
                final T result = (T)analysisServer;
                return result;
            }
        };
        // Add echo endpoint to analysis server container
        final ServerEndpointConfig endpointConfig =
                        ServerEndpointConfig.Builder
                                        .create(AnalysisServer.class,
                                                        ANALYSIS_SERVER_PATH)
                                        .configurator(configurator).build();
        serverContainer.addEndpoint(endpointConfig);
    }

    @Override
    public void close() {
        analysisServerThreadPool.shutdownNow();
    }

}
