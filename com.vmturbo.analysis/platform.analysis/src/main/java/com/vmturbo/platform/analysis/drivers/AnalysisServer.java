package com.vmturbo.platform.analysis.drivers;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.EconomySettingsTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;

/**
 * The WebSocket server endpoint for the analysis server. It is the entry point of the application.
 *
 * <p>
 *  Currently it can receive a topology as a sequence of Protobuf messages, run a round of placement,
 *  resize, provision and suspension on the resulting economy and send back a list of proposed actions
 *  and a sequence of Protobuf messages carrying end state values.
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

    // map that associates every topology with a instanceInfo that has the topology and some associated settings
    private Map<Long, AnalysisInstanceInfo> analysisInstanceInfoMap = new HashMap<>();

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
        logger.info("Intializing IdentityGenerator");
        IdentityGenerator.initPrefix(IdentityGenerator.MAXPREFIX);
        // Would be nice to log the remote IP address but I couldn't find a way...
    }

    /**
     * Handles any of the messages {@code this} server is expected to receive.
     *
     * @param session see {@link OnMessage}
     * @param message A single serialized {@link AnalysisCommand} Protobuf message.
     * Also see {@link OnMessage}.
     */
    @OnMessage
    public synchronized void handleMessage(@NonNull Session session, @NonNull InputStream message) {
        try {
            AnalysisCommand command = AnalysisCommand.parseFrom(message);
            switch (command.getCommandTypeCase()) {
                case START_DISCOVERED_TOPOLOGY:
                    AnalysisInstanceInfo instInfo = new AnalysisInstanceInfo();
                    analysisInstanceInfoMap.put(command.getTopologyId(), instInfo);
                    instInfo.setShopTogetherEnabled(command.getStartDiscoveredTopology()
                                                    .getEnableShopTogether());
                    instInfo.getLastComplete().setTopologyId(command.getTopologyId());
                    Topology currentPartial = instInfo.getCurrentPartial();
                    currentPartial.setTopologyId(command.getTopologyId());
                    EconomySettingsTO settingsTO = command.getStartDiscoveredTopology()
                                    .getEconomySettings();
                    EconomySettings settings = currentPartial.getEconomy().getSettings();
                    settings.setRightSizeLower(settingsTO.getRightsizeLowerWatermark());
                    settings.setRightSizeUpper(settingsTO.getRightsizeUpperWatermark());
                    settings.setUseExpenseMetricForTermination(settingsTO
                            .getUseExpenseMetricForTermination());
                    settings.setExpenseMetricFactor(settingsTO.getExpenseMetricFactor());
                    break;
                case DISCOVERED_TRADER:
                    ProtobufToAnalysis.addTrader(analysisInstanceInfoMap.get(command.getTopologyId())
                                                 .getCurrentPartial(), command.getDiscoveredTrader());
                    break;
                case END_DISCOVERED_TOPOLOGY:
                    // Finish topology
                    EndDiscoveredTopology endDiscMsg = command.getEndDiscoveredTopology();
                    AnalysisInstanceInfo instInfoAfterDisc = analysisInstanceInfoMap.get(command.getTopologyId());
                    Topology currPartial = instInfoAfterDisc.getCurrentPartial();
                    ProtobufToAnalysis.populateUpdatingFunctions(endDiscMsg, currPartial);
                    ProtobufToAnalysis.populateCommodityResizeDependencyMap(endDiscMsg, currPartial);
                    ProtobufToAnalysis.populateRawCommodityMap(endDiscMsg, currPartial);

                    instInfoAfterDisc.setProvisionEnabled(endDiscMsg.getEnableProvision());
                    instInfoAfterDisc.setSuspensionEnabled(endDiscMsg.getEnableSuspension());
                    instInfoAfterDisc.setResizeEnabled(endDiscMsg.getEnableResize());

                    // create a new thread to run the analysis algorithm so that
                    // it does not block the server to receive messages from M1
                    Runnable runAnalysis = new Runnable() {
                        @Override
                        public void run() {
                            runAnalysis(session, command.getTopologyId());
                        }
                    };
                    new Thread(runAnalysis).start();
                    break;
                case FORCE_PLAN_STOP:
                    logger.info("Received a message to stop running analysis from session "
                                    + session.getId());
                    if (analysisInstanceInfoMap != null) {
                        analysisInstanceInfoMap.get(command.getTopologyId()).getCurrentPartial()
                                .getEconomy().setForceStop(true);
                    }
                    break;
                case COMMANDTYPE_NOT_SET:
                default:
                    logger.warn("Unknown command received from remote endpoint with case = \""
                                    + command.getCommandTypeCase() + "\" from session "
                                    + session.getId());
            }
        } catch (Throwable error) {
            logger.error("Exception thrown while processing message from session "
                            + session.getId(), error);
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

    /**
     * Create a new thread to execute the analysis algorithm which
     * generates actions.
     */
    private void runAnalysis(@NonNull Session session, long topologyId) {
        // Swap topologies
        AnalysisInstanceInfo instInfo = analysisInstanceInfoMap.get(topologyId);

        Topology temp = instInfo.getLastComplete();
        Topology lastComplete = instInfo.getCurrentPartial();
        instInfo.setLastComplete(lastComplete);
        instInfo.setCurrentPartial(temp);
        // Run one round of placement measuring time-to-process
        long start = System.nanoTime();
        Economy economy = (Economy)lastComplete.getEconomy();
        PriceStatement startPriceStatement = new PriceStatement().computePriceIndex(economy);
        @NonNull List<@NonNull Action> actions = new Ede().generateActions(
                        economy, instInfo.isShopTogetherEnabled(), instInfo.isProvisionEnabled(),
                        instInfo.isSuspensionEnabled(), instInfo.isResizeEnabled(), true);
        // if the analysis was forced to stop, send a planStopped message back
        // to M1 which can further clear the plan related data
        if (lastComplete.getEconomy().getForceStop()) {
            try (OutputStream stream = session.getBasicRemote().getSendStream()) {
                AnalysisResults.newBuilder().setPlanStopped(true).build().writeTo(stream);
            } catch (Throwable error) {
                logger.error("Exception thrown while sending back stop message from session "
                                + session.getId(), error);
            }
            return;
        }
        long stop = System.nanoTime();

        // Send back the results
        try (OutputStream stream = session.getBasicRemote().getSendStream()) {
            AnalysisToProtobuf.analysisResults(actions, lastComplete.getTraderOids(),
                            lastComplete.getShoppingListOids(), stop - start, lastComplete,
                            startPriceStatement, true).writeTo(stream);
        } catch (Throwable error) {
            logger.error("Exception thrown while sending back actions!", error);
        } finally {
            // remove topologyInfo from the map
            analysisInstanceInfoMap.remove(topologyId);
        }
        return;
    }

    public class AnalysisInstanceInfo {
        // It is possible that some exceptional event like a connection drop will result in an
        // incomplete topology. e.g. if we receive a START_DISCOVERED_TOPOLOGY, then some
        // DISCOVERED_TRADER messages and then the connection resets and we receive a
        // START_DISCOVERED_TOPOLOGY again. In that context, lastComplete_ is the last complete topology
        // we've received and currentPartial_ is the topology we are currently populating.
        private @NonNull Topology lastComplete_ = new Topology();
        private @NonNull Topology currentPartial_ = new Topology();
        // a flag to decide if move should use shop-together algorithm or not
        boolean isShopTogetherEnabled = false;
        // a flag to decide if provision algorithm should run or not
        boolean isProvisionEnabled = true;
        // a flag to decide if suspension algorithm should run or not
        boolean isSuspensionEnabled = true;
        // a flag to decide if resize algorithm should run or not
        boolean isResizeEnabled = true;

        public boolean isShopTogetherEnabled() {
            return isShopTogetherEnabled;
        }
        public void setShopTogetherEnabled(boolean isShopTogetherEnabled) {
            this.isShopTogetherEnabled = isShopTogetherEnabled;
        }
        public boolean isProvisionEnabled() {
            return isProvisionEnabled;
        }
        public void setProvisionEnabled(boolean isProvisionEnabled) {
            this.isProvisionEnabled = isProvisionEnabled;
        }
        public boolean isSuspensionEnabled() {
            return isSuspensionEnabled;
        }
        public void setSuspensionEnabled(boolean isSuspensionEnabled) {
            this.isSuspensionEnabled = isSuspensionEnabled;
        }
        public boolean isResizeEnabled() {
            return isResizeEnabled;
        }
        public void setResizeEnabled(boolean isResizeEnabled) {
            this.isResizeEnabled = isResizeEnabled;
        }
        public Topology getLastComplete() {
            return lastComplete_;
        }
        public void setLastComplete(Topology lastComplete) {
            this.lastComplete_ = lastComplete;
        }
        public Topology getCurrentPartial() {
            return currentPartial_;
        }
        public void setCurrentPartial(Topology currentPartial) {
            this.currentPartial_ = currentPartial;
        }
    } // end AnalysisInstanceInfo class

} // end AnalysisServer class
