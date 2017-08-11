package com.vmturbo.platform.analysis.drivers;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.StatsUtils;

import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.StartDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.EconomySettingsTO;


/**
 * The handler to receive and process the {@link AnalysisCommand} from client
 */
public class AnalysisClientMessageHandler implements ITransport.EventHandler<AnalysisCommand> {

    private static final Logger logger = LogManager.getLogger(AnalysisClientMessageHandler.class);
    // create a thread pool to process the message received from M2
    private static int NUM_OF_THREAD = 5;
    // the maximum number of task queue allowed to process client message
    private static final int MAX_QUEUE_SIZE = 100;
    // the thread pool used for handling message sent from client side
    private ExecutorService executorService =
                    Executors.newFixedThreadPool(NUM_OF_THREAD, new ThreadFactoryBuilder()
                                    .setNameFormat("analysis-client-message-processor-%d")
                                    .build());

    // the protobuf end point that is used to send and receive protobuf message
    private AnalysisServerProtobufEndPoint protobufEndpoint;
    // the analysis server websocket endpoint
    private AnalysisServer analysisServer;

    public AnalysisClientMessageHandler(AnalysisServerProtobufEndPoint protobufEndpoint,
                    AnalysisServer analysisServer) {
        this.protobufEndpoint = protobufEndpoint;
        this.analysisServer = analysisServer;
        // resend the message that was not successfully sent to client side in previous session
        try {
            while(!analysisServer.getPreviousFailedMsg().isEmpty()) {
                AnalysisResults failedMsg = analysisServer.getPreviousFailedMsg().poll();
                sendResult(failedMsg);
            }
        } catch (InterruptedException e) {
            logger.error("Can not send the failed message from previous session", e);
            onClose();
        }

    }

    @Override
    public void onClose() {
        logger.info("Endpiont closed: " + protobufEndpoint);
        executorService.shutdown();
        protobufEndpoint = null;
    }

    @Override
    public void onMessage(AnalysisCommand message) {
        // Create a generic response handler for all messages received from client
        // At this place, all messages are forwarded to remote market actor.
        // Remote Market akka actor can decide from the type of the message what code to invoke
        // in order to process the received messages
        // This actor will return a response with a message that can be sent back to the
        // websocket client who initiated the message communication
        handleAllMessagesSentFromClient(message);

    }

    /**
     * Populates {@link EconomySettings} by parsing {@link StartDiscoveredTopology}
     */
    private AnalysisInstanceInfo parseStartDiscoveredTopology(AnalysisCommand message) {
        StartDiscoveredTopology discovered = message.getStartDiscoveredTopology();
        AnalysisInstanceInfo instInfo = new AnalysisInstanceInfo();
        instInfo.setClassifyActions(discovered.getClassifyActions());
        instInfo.setReplayActions(discovered.getReplayActions());
        instInfo.getLastComplete().setTopologyId(message.getTopologyId());
        instInfo.setMarketName(message.getMarketName());
        instInfo.setRealTime(discovered.getRealTime());
        instInfo.setMarketData(message.getMarketData());
        Topology currentPartial = instInfo.getCurrentPartial();
        currentPartial.setTopologyId(message.getTopologyId());
        EconomySettingsTO settingsTO =
                        message.getStartDiscoveredTopology().getEconomySettings();
        EconomySettings settings = currentPartial.getEconomy().getSettings();
        settings.setRightSizeLower(settingsTO.getRightsizeLowerWatermark());
        settings.setRightSizeUpper(settingsTO.getRightsizeUpperWatermark());
        settings.setUseExpenseMetricForTermination(
                        settingsTO.getUseExpenseMetricForTermination());
        settings.setExpenseMetricFactor(settingsTO.getExpenseMetricFactor());
        settings.setRateOfResize(settingsTO.getRateOfResize());
        settings.setEstimatesEnabled(settingsTO.getEstimates());
        return instInfo;
    }

    /**
     * Populates {@link EconomySettings} by parsing {@link EndDiscoveredTopology}
     */
    private void parseEndDiscoveredTopology(AnalysisCommand message) {
     // Finish topology
        EndDiscoveredTopology endDiscMsg = message.getEndDiscoveredTopology();
        AnalysisInstanceInfo instInfoAfterDisc =
                        analysisServer.getAnalysisInstanceInfoMap().get(message.getTopologyId());
        Topology currPartial = instInfoAfterDisc.getCurrentPartial();
        currPartial.populateMarketsWithSellers();
        ProtobufToAnalysis.populateCommodityResizeDependencyMap(endDiscMsg,
                        currPartial);
        ProtobufToAnalysis.populateRawCommodityMap(endDiscMsg, currPartial);
        ProtobufToAnalysis.commToAdjustOverhead(endDiscMsg, currPartial);

        if (message.getMarketName().equals("Deploy")) {
            instInfoAfterDisc.setProvisionEnabled(false);
            instInfoAfterDisc.setSuspensionEnabled(false);
            instInfoAfterDisc.setResizeEnabled(false);
        } else {
            instInfoAfterDisc.setProvisionEnabled(endDiscMsg.getEnableProvision());
            instInfoAfterDisc.setSuspensionEnabled(endDiscMsg.getEnableSuspension());
            instInfoAfterDisc.setResizeEnabled(endDiscMsg.getEnableResize());
        }
        instInfoAfterDisc.setMarketName(message.getMarketName());
        instInfoAfterDisc.setMarketData(message.getMarketData());
    }

    /**
     * Sets {@link forceStop} true when receiving {@link ForcePlanStop}
     */
    private void forceStopAnalysis(AnalysisCommand message) {
        logger.info("Received a message to stop running analysis");
        AnalysisInstanceInfo analysisInstance =
                        analysisServer.getAnalysisInstanceInfoMap().get(message.getTopologyId());
        if (analysisInstance != null) {
            analysisInstance.getCurrentPartial().getEconomy().setForceStop(true);
            analysisInstance.getLastComplete().getEconomy().setForceStop(true);
        }
    }
    /**
     * Handles all messages coming from client side.
     *
     * @param receivedMsg Message received from the client side
     */
    private void handleAllMessagesSentFromClient(AnalysisCommand message) {
        switch (message.getCommandTypeCase()) {
            case START_DISCOVERED_TOPOLOGY:
                analysisServer.getAnalysisInstanceInfoMap().put(message.getTopologyId(),
                                parseStartDiscoveredTopology(message));
                break;
            case DISCOVERED_TRADER:
                if (message.getDiscoveredTrader().getTemplateForHeadroom()) {
                    analysisServer.getAnalysisInstanceInfoMap().get(message.getTopologyId())
                                    .getCurrentPartial().addTradersForHeadroom(message.getDiscoveredTrader());
                } else {
                    ProtobufToAnalysis.addTrader(
                                    analysisServer.getAnalysisInstanceInfoMap().get(message.getTopologyId())
                                                    .getCurrentPartial(), message.getDiscoveredTrader());
                }
                break;
            case END_DISCOVERED_TOPOLOGY:
                parseEndDiscoveredTopology(message);
                // once finished constructing economy, start analysis in a separate thread to
                // release endpoint
                if (((ThreadPoolExecutor)executorService).getQueue().size() >= MAX_QUEUE_SIZE) {
                    logger.error("Exceeding client message processing queue size " + MAX_QUEUE_SIZE
                                    + " Can not trigger analysis on topology with name "
                                    + message.getMarketName());
                    try {
                        sendResult(AnalysisResults.newBuilder().setTopologyId(message.getTopologyId())
                                        .setAnalysisFailed(true).build());
                    } catch (InterruptedException e) {
                        logger.error("Shutting down thread pool when sending result back to client", e);
                        executorService.shutdown();
                    }
                    return;
                }
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                        long topologyId = message.getTopologyId();
                        // remove topologyInfo from the map if result is sent successfully
                        AnalysisResults results;
                        results = runAnalysis(topologyId);
                        logger.info("Preparing to send back analysis result to client side");
                        if (!sendResult(results)) {
                            analysisServer.getPreviousFailedMsg().add(results);
                        }
                        analysisServer.getAnalysisInstanceInfoMap().remove(topologyId);
                        } catch (InterruptedException e) {
                            logger.error("Shutting down thread pool when sending result back to client", e);
                            executorService.shutdown();
                        }

                    }
                });
                break;
            case FORCE_PLAN_STOP:
                forceStopAnalysis(message);
                break;
            case COMMANDTYPE_NOT_SET:
            default:
                logger.warn("Unknown command received from remote endpoint with case = \""
                                + message.getCommandTypeCase());
        }

    }

    /**
     * Sends the result message to the server side.
     */
    private synchronized boolean sendResult(AnalysisResults result) throws InterruptedException {
        boolean sentSuccessful = false;
        try {
            // get the latest registered protobuf endpoint and send result
            analysisServer.getAnalysisTransportHandler().getRegisteredEndpoint().send(result);
            sentSuccessful = true;
            logger.info("Sending message to analysis client succeed");
        } catch (NullPointerException | IllegalStateException | CommunicationException e) {
            logger.error("Exception caught when sending the message to analysis client", e);
        }
        return sentSuccessful;
    }
    /**
     * Run the analysis algorithm which generates actions.
     *
     * @param topologyId Topology  Id of the topology for which analysis is being run.
     * @return AnalysisResults the object containing result
     */
    private @NonNull AnalysisResults runAnalysis(long topologyId) {
        AnalysisResults results;
        AnalysisInstanceInfo instInfo = analysisServer.getAnalysisInstanceInfoMap().get(topologyId);
        Topology temp = instInfo.getLastComplete();
        Topology lastComplete = instInfo.getCurrentPartial();
        instInfo.setLastComplete(lastComplete);
        instInfo.setCurrentPartial(temp);
        try {// Swap topologies
            results = generateAnalysisResult(instInfo, lastComplete);
            // if the analysis was forced to stop, send a planStopped message back
            // to M1 which can further clear the plan related data
            if (((Economy)lastComplete.getEconomy()).getForceStop()) {
                results = AnalysisResults.newBuilder().setPlanStopped(true)
                                .setTopologyId(topologyId).build();
            }
        } catch (RuntimeException error) {
            // if exceptions occur when running analysis, send back a message
            // with analysis_failed=true
            logger.error("Encounter invalid date during topology analysis", error);
            StatsUtils statsUtils = new StatsUtils("m2stats-" + instInfo.getMarketName(), true);
            statsUtils.write("Encounter invalid date during topology analysis: " + error + "\n",
                            true);
            // since running analysis throws exceptions, we send a failure
            // message with topology id back to notify ops manager
            results = AnalysisResults.newBuilder().setTopologyId(topologyId).setAnalysisFailed(true)
                            .build();
        }
        // Send back the results
        return results;
    }

    /**
     * Run provision, placement, resize and suspension algorithm to produce actions that
     * contributes to the desired state.
     *
     * @param instInfo the given topology wrapper object
     * @param lastComplete the most recent complete topology
     * @return actionDTOs, projected traderDTOs and priceIndex
     */
    private AnalysisResults generateAnalysisResult(AnalysisInstanceInfo instInfo,
                    Topology lastComplete) {
        List<Action> actions;
        AnalysisResults results;
        Economy economy = (Economy)lastComplete.getEconomy();
        String mktName = instInfo.getMarketName();
        String mktData = instInfo.getMarketData();
        long start = System.nanoTime();
        PriceStatement startPriceStatement = new PriceStatement().computePriceIndex(economy);
        if (!economy.getTradersForHeadroom().isEmpty()) {
            actions = new Ede().generateHeadroomActions(economy, false, false, false, true);
            long stop = System.nanoTime();
            results = AnalysisToProtobuf.analysisResults(actions, lastComplete.getTraderOids(),
                            lastComplete.getShoppingListOids(), stop - start, lastComplete,
                            startPriceStatement, true);
        } else {
            // if there are no templates to be added this is not a headroom plan
            ReplayActions lastDecisions = analysisServer.getReplayActionsMap().get(mktName);
            Ede ede = new Ede();
            boolean isReplayOrRealTime = instInfo.isReplayActions() || instInfo.isRealTime();
            if (isReplayOrRealTime) {
                ede.setReplayActions(
                                (lastDecisions != null) ? lastDecisions : new ReplayActions());
            }
            actions = ede.generateActions(economy, instInfo.isClassifyActions(),
                            instInfo.isProvisionEnabled(), instInfo.isSuspensionEnabled(),
                            instInfo.isResizeEnabled(), true, mktData, instInfo.isRealTime());
            long stop = System.nanoTime();
            results = AnalysisToProtobuf.analysisResults(actions, lastComplete.getTraderOids(),
                            lastComplete.getShoppingListOids(), stop - start, lastComplete,
                            startPriceStatement, true);
            if (isReplayOrRealTime) {
                ReplayActions newReplayActions = ede.getReplayActions();
                // the oids have to be updated after analysisResults
                newReplayActions.setTraderOids(lastComplete.getTraderOids());
                if (instInfo.isReplayActions()) {
                    newReplayActions.setActions(actions);
                } else if (instInfo.isRealTime()) {
                    // if replay is disabled, perform selective replay to deactivate entities in RT (OM-19855)
                    newReplayActions.setActions(actions.stream()
                                    .filter(action -> action instanceof Deactivate)
                                    .collect(Collectors.toList()));
                }
                analysisServer.getReplayActionsMap().put(mktName, newReplayActions);
            }

        }
        return results;
    }

}
