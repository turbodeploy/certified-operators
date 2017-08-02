package com.vmturbo.platform.analysis.drivers;

import javax.websocket.Endpoint;

import static com.google.common.base.Preconditions.checkNotNull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.WebsocketServerTransport;
import com.vmturbo.communication.WebsocketServerTransportManager.TransportHandler;

import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;

/**
 * Analysis websocket server transport handler. It initialize a new protobuf endpoint instance when
 * {@link onNewTransport} is called.
 * @author weiduan
 *
 */
public class AnalysisWebsocketServerTransportHandler
                implements TransportHandler, AnalysisWebsocketServerTransportHandlerWrapper {

    // the protobuf end point that is used to send and receive protobuf message
    private AnalysisServerProtobufEndPoint protobufEndpoint;

    // the websocket server endpoint
    private AnalysisServer analysisServer;

    /**
     * Attach the endpoint to transport.
     */
    @Override
    public synchronized void onNewTransport(WebsocketServerTransport transport) {
        protobufEndpoint = new AnalysisServerProtobufEndPoint(transport);
        // create the handler for the messages
        checkNotNull(analysisServer);
        final ITransport.EventHandler<AnalysisCommand> eventHandler =
                        new AnalysisClientMessageHandler(protobufEndpoint, analysisServer);
        // add the handler to the end point
        protobufEndpoint.addEventHandler(eventHandler);
    }

    /**
     * Returns the endpoint associates with the transport.
     */
    @Override
    public AnalysisServerProtobufEndPoint getRegisteredEndpoint() {
        return protobufEndpoint;
    }

    /**
     * Associate the handler with the websocket endpoint.
     */
    @Override
    public void attachEndPoint(Endpoint endpoint) {
        this.analysisServer = ((AnalysisServer)endpoint);
    }

    /**
     * Returns the websocket endpoint associates with the handler.
     * @return
     */
    public AnalysisServer getAnalysisServer() {
        return analysisServer;
    }

}

