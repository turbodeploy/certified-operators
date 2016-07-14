package com.vmturbo.platform.analysis.drivers;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;

/**
 * The WebSocket server endpoint for the analysis server. It is the entry point of the application.
 *
 * <p>
 *  Currently it can receive a topology as a sequence of Protobuf messages, run a round of placement
 *  on the resulting economy and send back a list of proposed actions.
 * </p>
 *
 * <p>
 *  This is intended to be accessed via wss://localhost:9400/analysis/server
 * </p>
 */
@ServerEndpoint("/server")
public final class AnalysisServer {
    // Fields

    // A logger to be used for all logging by this class.
    private static final Logger logger = Logger.getLogger(AnalysisServer.class);

    // It is possible that some exceptional event, like a connection drop will result in an
    // incomplete topology. e.g. if we receive a START_DISCOVERED_TOPOLOGY, then some
    // DISCOVERED_TRADER messages and then the connection resets and we receive a
    // START_DISCOVERED_TOPOLOGY again. In that context, lastComplete_ is the last complete topology
    // we've received and currentPartial_ is the topology we are currently populating.
    private @NonNull Topology lastComplete_ = new Topology();
    private @NonNull Topology currentPartial_ = new Topology();
    // a flag to decide if move should use shoptpgether algorithm or not
    boolean isShopTogetherEnabled = false;
    // a flag to decide if provision algorithm should run or not
    boolean isProvisionEnabled = true;
    // a flag to decide if suspension algorithm should run or not
    boolean isSuspensionEnabled = true;
    // a flag to decide if resize algorithm should run or not
    boolean isResizeEnabled = true;

    // Constructors

    // Methods

    /**
     * Logs the initialization of new connections. Doesn't change internal state.
     *
     * @param session see {@link OnOpen}
     */
    @OnOpen
    public synchronized void logConnectionInitiation(@ReadOnly AnalysisServer this, @NonNull Session session) {
        logger.info("New " + (session.isSecure() ? "secure" : "insecure") + " connection with id = \""
                    + session.getId() + "\" established.");
        logger.info("Request URI:                     " + session.getRequestURI());
        logger.info("Protocol version:                " + session.getProtocolVersion());
        logger.info("Negotiated subprotocol:          " + session.getNegotiatedSubprotocol());
        logger.info("Number of negotiated extentions: " + session.getNegotiatedExtensions().size());
        logger.info("Max session timeout:             " + session.getMaxBinaryMessageBufferSize() + "ms.");
        logger.info("Max binary buffer size:          " + session.getMaxIdleTimeout() + "bytes.");
        logger.info("");
        // Would be nice to log the remote IP address but I couldn't find a way...
    }

    /**
     * Handles any of the messages {@code this} server is expected to receive.
     *
     * @param session see {@link OnMessage}
     * @param message A single serialized {@link AnalysisCommand} Protobuf message. Also see {@link OnMessage}.
     */
    @OnMessage
    public synchronized void handleMessage(@NonNull Session session, @NonNull InputStream message) {
        try {
            AnalysisCommand command = AnalysisCommand.parseFrom(message);
            switch (command.getCommandTypeCase()) {
                case START_DISCOVERED_TOPOLOGY:
                    isShopTogetherEnabled = command.getStartDiscoveredTopology().getEnableShopTogether();
                    currentPartial_.clear();
                    break;
                case DISCOVERED_TRADER:
                    ProtobufToAnalysis.addTrader(currentPartial_, command.getDiscoveredTrader());
                    break;
                case END_DISCOVERED_TOPOLOGY:
                    // Finish topology
                    ProtobufToAnalysis.populateUpdatingFunctions(command.getEndDiscoveredTopology(),
                                                                         currentPartial_);
                    ProtobufToAnalysis.populateCommodityResizeDependencyMap(command.getEndDiscoveredTopology(),
                                                                            currentPartial_);
                    ProtobufToAnalysis.populateRawCommodityMap(command.getEndDiscoveredTopology(),
                                                               currentPartial_);

                    isProvisionEnabled = command.getEndDiscoveredTopology().getEnableProvision();
                    isSuspensionEnabled =
                                    command.getEndDiscoveredTopology().getEnableSuspension();
                    isResizeEnabled = command.getEndDiscoveredTopology().getEnableResize();

                    // Swap topologies
                    Topology temp = lastComplete_;
                    lastComplete_ = currentPartial_;
                    currentPartial_ = temp;

                    // Run one round of placement measuring time-to-process
                    long start = System.nanoTime();
                    @NonNull List<@NonNull Action> actions = new Ede()
                                    .generateActions((Economy)lastComplete_.getEconomy(),
                                                    isShopTogetherEnabled, isProvisionEnabled,
                                                    isSuspensionEnabled, isResizeEnabled);

                    long stop = System.nanoTime();

                    // Send back the results
                    try (OutputStream stream = session.getBasicRemote().getSendStream()) {
                        AnalysisToProtobuf.analysisResults(actions, lastComplete_.getTraderOids()::get,
                                        lastComplete_.getShoppingListOids()::get, stop - start,
                                        lastComplete_).writeTo(stream);
                    }
                    break;
                case COMMANDTYPE_NOT_SET:
                default:
                    logger.warn("Unknown command received from remote endpoint with case = \""
                                + command.getCommandTypeCase());
            }
        } catch (Throwable error) {
            logger.error("Exception thrown while processing message!",error);
        }
    }

    /**
     * Logs the termination of existing connections. Doesn't change internal state.
     *
     * @param session see {@link OnClose}
     * @param reason see {@link OnClose}
     */
    @OnClose
    public synchronized void logConnectionTermination(@ReadOnly AnalysisServer this,
                                                      @NonNull Session session, @NonNull CloseReason reason) {
        logger.info("Existing " + (session.isSecure() ? "secure" : "insecure") + " connection with id = \""
                    + session.getId() + "\" terminated with reason \"" + reason + "\".");
    }

    /**
     * Logs the receipt of errors from the remote endpoint. Doesn't change internal state.
     *
     * @param error see {@link OnError}
     */
    @OnError
    public synchronized void logError(@ReadOnly AnalysisServer this, @NonNull Throwable error) {
        logger.error("Received an error from remote endpoint!", error);
    }

} // end AnalysisServer class
