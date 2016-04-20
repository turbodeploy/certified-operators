package com.vmturbo.platform.analysis.drivers;


import java.io.InputStream;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This is intended to be accessed via wss://localhost:9400/analysis/server
 */
@ServerEndpoint("/server")
public class AnalysisServer {
    // Fields
    private static final Logger logger = Logger.getLogger(AnalysisServer.class);

    // Constructors

    // Methods
    @OnOpen
    public void logConnectionInitiation(@NonNull Session session) {
        logger.info("New " + (session.isSecure() ? "secure" : "insecure") + " connection with id = \""
                    + session.getId() + "\" established.");
        logger.info("Request URI:                     " + session.getRequestURI());
        logger.info("Protocol version:                " + session.getProtocolVersion());
        logger.info("Negotiated subprotocol:          " + session.getNegotiatedSubprotocol());
        logger.info("Number of negotiated extentions: " + session.getNegotiatedExtensions().size());
        logger.info("Max session timeout:             " + session.getMaxBinaryMessageBufferSize() + "ms.");
        logger.info("Max binary buffer size:          " + session.getMaxIdleTimeout() + "bytes.");

        // The following might not need to be logged each time.
        logger.info("Default max session timeout:     " + session.getContainer().getDefaultMaxSessionIdleTimeout() + "ms.");
        logger.info("Default max binary buffer size:  " + session.getContainer().getDefaultMaxSessionIdleTimeout() + "bytes.");
        logger.info("");;
        // Would be nice to log the remote IP address but I couldn't find a way...
    }

    @OnMessage
    public void handleMessage(@NonNull InputStream input) {
        try {
            // handle messages here
        } catch (Throwable error) {
            logger.error(error);
        }
    }

    @OnClose
    public void logConnectionTermination(@NonNull Session session, @NonNull CloseReason reason) {
        logger.info("Existing " + (session.isSecure() ? "secure" : "insecure") + " connection with id = \""
                    + session.getId() + "\" terminated with reason \"" + reason + "\".");
    }

    @OnError
    public void logError(@NonNull Throwable error) {
        logger.error("Received error: \"" + error + "\" from remote endpoint!");
    }

} // end AnalysisServer class
