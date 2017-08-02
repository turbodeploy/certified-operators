package com.vmturbo.platform.analysis.drivers;

import javax.websocket.Endpoint;

/**
 * An interface to associate the websocket endpoint with transport handler and to
 * get a {@link AnalysisServerProtobufEndPoint} of the handler.
 * @author weiduan
 *
 */
public interface AnalysisWebsocketServerTransportHandlerWrapper {

    /**
     * Get the protobuf endpoint that associates with the handler.
     * @return
     */
    AnalysisServerProtobufEndPoint getRegisteredEndpoint();

    /**
     * Associates the handler with the websocket endpoint.
     * @param endpoint
     */
    void attachEndPoint(Endpoint endpoint);
}
