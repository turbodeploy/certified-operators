package com.vmturbo.platform.analysis.drivers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.StartDiscoveredTopology;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;

@ClientEndpoint
public class PrimaryClient {
    // Fields
    private static final Logger logger = LogManager.getLogger(PrimaryClient.class);
    private final @NonNull Session browserSession_;
    private final @NonNull LegacyTopology topology_;

    // Constructors
    public PrimaryClient(@NonNull Session browserSession, @NonNull LegacyTopology topology) {
        browserSession_ = browserSession;
        topology_ = topology;
    }

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
        logger.info("");
        // Would be nice to log the remote IP address but I couldn't find a way...

        try {
            logger.info("Start sending messages!");
            try (OutputStream stream = session.getBasicRemote().getSendStream()) {
                AnalysisCommand.newBuilder().setStartDiscoveredTopology(StartDiscoveredTopology
                                            .newBuilder()).build().writeTo(stream);
                logger.info("Sent start discovered topology message!");
            }
            for (@NonNull @ReadOnly Trader trader : topology_.getEconomy().getTraders()) {
                try (OutputStream stream = session.getBasicRemote().getSendStream()) {
                    AnalysisCommand.newBuilder().setDiscoveredTrader(
                         AnalysisToProtobuf.traderTO(topology_.getEconomy(), trader, null, null)).build()
                            .writeTo(stream);
                    logger.info("Sent discovered trader message!");
                }
            }
            try (OutputStream stream = session.getBasicRemote().getSendStream()) {
                AnalysisCommand.newBuilder().setEndDiscoveredTopology(EndDiscoveredTopology
                                            .newBuilder()).build().writeTo(stream);
                logger.info("Sent end discovered topology message!");
            }
        }
        catch (IOException e) {
            logger.error(e);
        };

    }

    @OnMessage
    public void handleMessage(@NonNull InputStream input) {
        try {


            browserSession_.close();
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

} // end PrimaryClient class
