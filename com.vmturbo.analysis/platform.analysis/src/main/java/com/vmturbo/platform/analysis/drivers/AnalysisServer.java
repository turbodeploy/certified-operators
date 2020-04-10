package com.vmturbo.platform.analysis.drivers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.ProvisionBase;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.StartDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.EconomySettingsTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.StatsUtils;


/**
 * AnalysisServer is a single instance, that handle incoming requests from client and sends
 * responses back.
 */
public class AnalysisServer implements AutoCloseable {

    private final Logger logger = LogManager.getLogger(AnalysisServer.class);
    // the maximum number of task queue allowed to process client message
    private static final int MAX_QUEUE_SIZE = 100;
    // the thread pool used for handling message sent from client side
    private final ExecutorService executorService;
    // map that associates every topology with a instanceInfo that has the topology and some associated settings
    private final Map<Long, AnalysisInstanceInfo> analysisInstanceInfoMap = new
            ConcurrentHashMap<>();
    // map that associates every market name with actions from last run
    private final Map<String, @NonNull ReplayActions> replayActionsMap = new ConcurrentHashMap<>();
    // a queue to save the AnalysisResult message that was not sent to client side
    private final BlockingQueue<AnalysisResults> resultsToSend = new LinkedBlockingQueue<>();
    // websocket transport handler
    private final Set<ITransport<AnalysisResults, AnalysisCommand>> endpoints =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Object newEndpointNotifier = new Object();
    /**
     * Task to poll outgoing message, when available to send using transports.
     */
    private final Future<?> pollOutgoingTask;

    public AnalysisServer(@Nonnull ExecutorService threadPool) {
        executorService = Objects.requireNonNull(threadPool);
        pollOutgoingTask = threadPool.submit(this::pollOutgoingMessages);
    }

    /**
     * Performs polling for new analysis results, ready for send. As soon as they arrive, this
     * method will try to send them to analysis client.
     */
    private void pollOutgoingMessages() {
        try {
            for (;;) {
                final AnalysisResults msg = resultsToSend.take();
                sendSingleMessage(msg);
            }
        } catch (InterruptedException e) {
            logger.info("Thread interrupted, Can not send the failed message from previous " +
                    "session", e);
        }
    }

    /**
     * Sends single analysis result using on of the transports available. If no transport is
     * available, it will hang to await new transport to appear.
     *
     * @param result analysis result to send
     * @throws InterruptedException in thread has been interrupted while sending or waiting
     */
    private void sendSingleMessage(@Nonnull AnalysisResults result) throws InterruptedException {
        for (;;) {
            for (ITransport<AnalysisResults, AnalysisCommand> transport : endpoints) {
                try {
                    transport.send(result);
                    logger.info("Successfully send market results " + result.getTopologyId());
                    return;
                } catch (CommunicationException e) {
                    logger.error("Communication error occurred, while sending market results " +
                            result.getTopologyId(), e);
                    transport.close();
                }
            }
            synchronized (newEndpointNotifier) {
                newEndpointNotifier.wait();
            }
        }
    }

    /**
     * Registers new endpoint, able to request market analysis, so AnalysisServer will receive
     * requests from this endpoint and send responses to it.
     *
     * @param endpoint new endpoint appeared
     */
    public void registerEndpoint(@Nonnull ITransport<AnalysisResults, AnalysisCommand> endpoint) {
        endpoints.add(Objects.requireNonNull(endpoint));
        endpoint.addEventHandler(new EventHandler<AnalysisCommand>() {
            @Override
            public void onMessage(AnalysisCommand message) {
                handleAllMessagesSentFromClient(message);
            }

            @Override
            public void onClose() {
                unregisterEndpoint(endpoint);
            }
        });
        synchronized (newEndpointNotifier) {
            newEndpointNotifier.notify();
        }
    }

    /**
     * Unregisters endpoint, when it is no longer needed (closed). This endpoint will no longer
     * be used for sending responses.
     *
     * @param endpoint endpoint to unregister.
     */
    private void unregisterEndpoint(
            @Nonnull ITransport<AnalysisResults, AnalysisCommand> endpoint) {
        logger.info("Endpoint closed: " + endpoint);
        endpoints.remove(Objects.requireNonNull(endpoint));
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
        instInfo.setBalanceDeploy(discovered.getBalanceDeploy());
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
        settings.setQuoteFactor(settingsTO.getQuoteFactor());
        settings.setMaxPlacementIterations(settingsTO.getMaxPlacementIterations());
        settings.setSortShoppingLists(settingsTO.getSortShoppingLists());
        if (settingsTO.hasDiscountedComputeCostFactor()) {
            settings.setDiscountedComputeCostFactor(settingsTO.getDiscountedComputeCostFactor());
        }
        return instInfo;
    }

    /**
     * Populates {@link EconomySettings} by parsing {@link EndDiscoveredTopology}
     */
    private void parseEndDiscoveredTopology(AnalysisCommand message) {
        // Finish topology
        EndDiscoveredTopology endDiscMsg = message.getEndDiscoveredTopology();
        AnalysisInstanceInfo instInfoAfterDisc =
                        analysisInstanceInfoMap.get(message.getTopologyId());
        Topology currPartial = instInfoAfterDisc.getCurrentPartial();
        currPartial.populateMarketsWithSellersAndMergeConsumerCoverage();
        ProtobufToAnalysis.populateCommodityResizeDependencyMap(endDiscMsg,
                        currPartial);
        ProtobufToAnalysis.populateRawCommodityMap(endDiscMsg, currPartial);
        ProtobufToAnalysis.populateCommodityProducesDependancyMap(endDiscMsg, currPartial);
        ProtobufToAnalysis.populateHistoryBasedResizeDependencyMap(endDiscMsg, currPartial);
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
        instInfoAfterDisc.setMovesThrottling(endDiscMsg.getMovesThrottling());
        instInfoAfterDisc.setSuspensionsThrottlingConfig(endDiscMsg.getSuspensionsThrottlingConfig());
    }

    /**
     * Calls {@link Economy#setForceStop} with true when receiving {@link com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.ForcePlanStop}
     */
    private void forceStopAnalysis(AnalysisCommand message) {
        logger.info("Received a message to stop running analysis");
        AnalysisInstanceInfo analysisInstance =
                        analysisInstanceInfoMap.get(message.getTopologyId());
        if (analysisInstance != null) {
            analysisInstance.getCurrentPartial().getEconomy().setForceStop(true);
            analysisInstance.getLastComplete().getEconomy().setForceStop(true);
        }
    }
    /**
     * Handles all messages coming from client side.
     *
     * @param message Message received from the client side
     */
    private void handleAllMessagesSentFromClient(AnalysisCommand message) {
        switch (message.getCommandTypeCase()) {
            case START_DISCOVERED_TOPOLOGY:
                analysisInstanceInfoMap.put(message.getTopologyId(),
                                parseStartDiscoveredTopology(message));
                break;
            case DISCOVERED_TRADER:
                if (message.getDiscoveredTrader().getTemplateForHeadroom()) {
                    analysisInstanceInfoMap.get(message.getTopologyId())
                                    .getCurrentPartial().addTradersForHeadroom(message.getDiscoveredTrader());
                } else {
                    try {
                        ProtobufToAnalysis.addTrader(
                            analysisInstanceInfoMap.get(message.getTopologyId())
                                .getCurrentPartial(), message.getDiscoveredTrader());
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Error when adding trader %s",
                            message.getDiscoveredTrader().getDebugInfoNeverUseInCode()), e);
                    }
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
                            sendResult(results);
                            analysisInstanceInfoMap.remove(topologyId);
                        } catch (InterruptedException e) {
                            logger.error("Shutting down thread pool when sending result back to client", e);
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
    private void sendResult(@Nonnull AnalysisResults result) throws InterruptedException {
        resultsToSend.add(Objects.requireNonNull(result));
    }

    /**
     * Run the analysis algorithm which generates actions.
     *
     * @param topologyId Topology  Id of the topology for which analysis is being run.
     * @return AnalysisResults the object containing result
     */
    private @NonNull AnalysisResults runAnalysis(long topologyId) {
        AnalysisResults results;
        AnalysisInstanceInfo instInfo = analysisInstanceInfoMap.get(topologyId);
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
            logger.error("Encounter invalid data during topology analysis", error);
            StatsUtils statsUtils = new StatsUtils("m2stats-" + instInfo.getMarketName(), false);
            statsUtils.write("Encounter invalid data during topology analysis: " + error + "\n",
                            true);
            // since running analysis throws exceptions, we send a failure
            // message with topology id back to notify ops manager
            results = AnalysisResults.newBuilder().setTopologyId(topologyId).setAnalysisFailed(true)
                            .build();
            return results;
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
    @VisibleForTesting
    AnalysisResults generateAnalysisResult(AnalysisInstanceInfo instInfo,
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
            Ede ede = new Ede();
            boolean isReplayOrRealTime = instInfo.isReplayActions() || instInfo.isRealTime();
            final @NonNull ReplayActions seedActions = isReplayOrRealTime
                ? replayActionsMap.getOrDefault(mktName, new ReplayActions())
                : new ReplayActions();
            actions = ede.generateActions(economy, instInfo.isClassifyActions(),
                instInfo.isProvisionEnabled(), instInfo.isSuspensionEnabled(),
                instInfo.isResizeEnabled(), true, seedActions, mktData, instInfo.isRealTime(),
                instInfo.getSuspensionsThrottlingConfig());
            if (instInfo.isBalanceDeploy()) {
                Set<Trader> placedVMSet = economy.getPlacementEntities().stream()
                    .filter(vm -> economy.getMarketsAsBuyer(vm).keySet().stream()
                        .allMatch(sl -> sl.getSupplier() != null)).collect(Collectors.toSet());
                if (!placedVMSet.isEmpty()) {
                    Collections.reverse(actions);
                    actions.stream().filter(action -> ((ActionImpl)action).isActionTaken())
                        .forEach(action -> action.rollback());
                    economy.getTraders().stream()
                        .filter(trader -> !placedVMSet.contains(trader))
                            .forEach(trader -> {
                                economy.getMarketsAsBuyer(trader).keySet()
                                                .forEach(sl -> sl.setMovable(false));
                            });
                    actions = ede.generateActions(economy, instInfo.isClassifyActions(),
                        instInfo.isProvisionEnabled(), instInfo.isSuspensionEnabled(),
                        instInfo.isResizeEnabled(), true, seedActions, mktData,
                        instInfo.isRealTime(), instInfo.getSuspensionsThrottlingConfig());
                }
            }
            long stop = System.nanoTime();
            results = AnalysisToProtobuf.analysisResults(actions, lastComplete.getTraderOids(),
                            lastComplete.getShoppingListOids(), stop - start, lastComplete,
                            startPriceStatement, true);
            if (instInfo.isRealTime() && instInfo.isProvisionEnabled()) {
                // Clear Unquoted Commodities list for Provision round in order to Provision
                // enough supply as expected.
                economy.getMarkets().forEach(m -> m.getBuyers().stream().forEach(sl -> {
                        sl.getModifiableUnquotedCommoditiesBaseTypeList().clear();
                        sl.getUnquotedCommoditiesBaseTypeList().clear();
                    })
                );
                // run another round of analysis on the new state of the economy with provisions enabled
                // and resize disabled. We add only the provision recommendations to the list of actions generated.
                // We neglect suspensions since there might be associated moves that we don't want to include
                @NonNull List<Action> secondRoundActions = new ArrayList<>();
                AnalysisResults.Builder builder = results.toBuilder();
                economy.getSettings().setResizeDependentCommodities(false);
                secondRoundActions.addAll(ede
                                .generateActions(economy, instInfo.isClassifyActions(), true, true,
                                                false, true, false, new ReplayActions(), mktData)
                                .stream().filter(action -> action instanceof ProvisionBase
                                                || action instanceof Activate
                                                // Extract resizes that explicitly set extractAction to true as part
                                                // of resizeThroughSupplier provision actions.
                                                || (action instanceof Resize
                                                                && action.isExtractAction()))
                                .collect(Collectors.toList()));
                for (Action action : secondRoundActions) {
                    ActionTO actionTO = AnalysisToProtobuf.actionTO(action,
                                    lastComplete.getTraderOids(),
                                    lastComplete.getShoppingListOids(), lastComplete);
                    if (actionTO != null) {
                        builder.addActions(actionTO);
                    }
                }
                builder.addAllNewShoppingListToBuyerEntry(
                                AnalysisToProtobuf.createNewShoppingListToBuyerMap(lastComplete));

                // Recreate TraderTO in order to send back the updated TraderTO after actions from
                // this provision round may have updated the trader.
                Set<Trader> preferentialTraders = economy.getPreferentialShoppingLists().stream()
                                .map(sl -> sl.getBuyer()).collect(Collectors.toSet());

                for (Trader trader : secondRoundActions.stream().map(Action::getActionTarget)
                                .collect(Collectors.toSet())) {
                    if (!trader.isClone()) {
                        builder.setProjectedTopoEntityTO(trader.getEconomyIndex(),
                                        AnalysisToProtobuf.traderTO(economy, trader,
                                                        lastComplete.getTraderOids(),
                                                        lastComplete.getShoppingListOids(),
                                                        preferentialTraders));
                    }
                }
                results = builder.build();
            }
            if (isReplayOrRealTime) {
                ReplayActions newReplayActions = new ReplayActions();
                // the OIDs have to be updated after analysisResults
                if (instInfo.isReplayActions()) {
                    newReplayActions = new ReplayActions(actions, ImmutableList.of(), lastComplete);
                } else if (instInfo.isRealTime()) {
                    // if replay is disabled, perform selective replay to deactivate entities in RT
                    // (OM-19855)
                    newReplayActions = new ReplayActions(ImmutableList.of(),
                                                actions.stream()
                                                    .filter(action -> action instanceof Deactivate)
                                                    .map(action -> (Deactivate)action)
                                                    .collect(Collectors.toList()),
                                                lastComplete);
                }
                replayActionsMap.put(mktName, newReplayActions);
            }

        }
        return results;
    }

    @Override
    public void close() {
        pollOutgoingTask.cancel(true);
    }
}
