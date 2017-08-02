package com.vmturbo.platform.analysis.drivers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.communication.WebsocketServerTransportManager;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.drivers.AnalysisServer;
import com.vmturbo.platform.analysis.ede.ReplayActions;

import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;

/**
* The WebSocket server endpoint for analysis component. It is the entry point of the application.
*
* <p>
*  Currently it can receive a topology as a sequence of Protobuf messages, run a round of placement,
*  resize, provision and suspension on the resulting economy and send back a list of proposed actions
*  and a sequence of Protobuf messages carrying end state values.
* </p>
*/

public class AnalysisServer extends WebsocketServerTransportManager {

    private static final Logger logger = LogManager.getLogger(AnalysisServer.class);
    // websocket transport handler
    private final AnalysisWebsocketServerTransportHandler analysisTransportHandler;
    // map that associates every topology with a instanceInfo that has the topology and some associated settings
    private Map<Long, AnalysisInstanceInfo> analysisInstanceInfoMap = new ConcurrentHashMap<>();
    // map that associates every market name with actions from last run
    private Map<String, ReplayActions> replayActionsMap = new ConcurrentHashMap<>();
    // a queue to save the AnalysisResult message that was not sent to client side
    private Queue<AnalysisResults> previousFailedMsg = new ConcurrentLinkedQueue<>();

    public AnalysisServer(TransportHandler transportHandler, ExecutorService threadPool) {
        super(transportHandler, threadPool);
        this.analysisTransportHandler = ((AnalysisWebsocketServerTransportHandler)transportHandler);
    }

    /**
     * Logs the initialization connection from client
     * to the server using the url specified in the endpoint.
     *
     * @param session {@link Session} object coming from client
     */
    @Override
    public void onOpen(@NonNull Session session, EndpointConfig endPointConfig) {
        logger.info("Server on open with session id " + session.getId() + "\n"
                        + "Request URI:                     " + session.getRequestURI() + "\n"
                        + "Protocol version:                " + session.getProtocolVersion() + "\n"
                        + "Max session timeout:             " + session.getMaxIdleTimeout() + "ms."
                        + "\n" + "Max binary buffer size:          "
                        + session.getMaxBinaryMessageBufferSize() + "bytes");
        logger.info("Intializing IdentityGenerator");
        IdentityGenerator.initPrefix(IdentityGenerator.MAXPREFIX);
        super.onOpen(session, endPointConfig);
    }

    /**
     * Returns the mapping of market name to replaying actions.
     * @return
     */
    public Map<String, ReplayActions> getReplayActionsMap() {
        return replayActionsMap;
    }

    /**
     * Returns the mapping of topology id to topology.
     * @return
     */
    public Map<Long, AnalysisInstanceInfo> getAnalysisInstanceInfoMap() {
        return analysisInstanceInfoMap;
    }

    /**
     * Returns the transport handler for websocket server.
     * @return
     */
    public AnalysisWebsocketServerTransportHandler getAnalysisTransportHandler() {
        return analysisTransportHandler;
    }

    /**
     * Returns the queue which saves the {@link AnalysisResults} that failed to be sent.
     * @return
     */
    public Queue<AnalysisResults> getPreviousFailedMsg() {
        return previousFailedMsg;
    }
}
