package com.vmturbo.market.runner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Analysis execution and properties. This can be for a scoped plan or for a real-time market.
 */
public class Analysis {
    public static LocalDateTime EPOCH = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
    // Analysis started (kept true also when it is completed).
    private AtomicBoolean started = new AtomicBoolean();
    // Analysis completed (successfully or not).
    private boolean completed = false;

    private final boolean includeVDC;
    private LocalDateTime startTime = EPOCH;
    private LocalDateTime completionTime = EPOCH;
    private final Set<TopologyEntityDTO> topologyDTOs;
    private final String logPrefix;

    private Collection<TopologyEntityDTO> projectedEntities = null;
    private final long projectedTopologyId;
    private ActionPlan actionPlan = null;
    private PriceIndexMessage priceIndexMessage = null;

    private String errorMsg;
    private AnalysisState state;

    private final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    public Analysis(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                    final boolean includeVDC) {
        this.topologyInfo = topologyInfo;
        this.topologyDTOs = topologyDTOs;
        this.includeVDC = includeVDC;
        this.state = AnalysisState.INITIAL;
        logPrefix = topologyInfo.getTopologyType() + " Analysis " +
            topologyInfo.getTopologyContextId() + " with topology " +
            topologyInfo.getTopologyId() + " : ";
        this.projectedTopologyId = IdentityGenerator.next();
    }

    private static final DataMetricSummary RESULT_PROCESSING = DataMetricSummary.builder()
            .withName("mkt_process_result_duration_seconds")
            .withHelp("Time to process the analysis results.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 20) // 20 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * Execute the analysis run. Generate the action plan, projected topology and price index message.
     * Only the first invocation of this method will actually run the analysis. Subsequent calls will
     * log an error message and immediately return {@code false}.
     * @return true if this is the first invocation of this method, false otherwise.
     */
    public boolean execute() {
        if (started.getAndSet(true)) {
            logger.error(logPrefix + "Completed or being computed");
            return false;
        }
        state = AnalysisState.IN_PROGRESS;
        startTime = LocalDateTime.now();
        logger.info(logPrefix + "Started");
        try {
            final TopologyConverter converter = new TopologyConverter(includeVDC,
                    topologyInfo.getTopologyType());
            final Set<TraderTO> traderTOs = converter
                            .convertToMarket(Lists.newLinkedList(topologyDTOs));
            if (logger.isDebugEnabled()) {
                logger.debug(traderTOs.size() + " Economy DTOs");
            }
            if (logger.isTraceEnabled()) {
                traderTOs.stream().map(dto -> "Economy DTO: " + dto).forEach(logger::trace);
            }
            final AnalysisResults results =
                    TopologyEntitiesHandler.performAnalysis(traderTOs, topologyInfo);
            final DataMetricTimer processResultTime = RESULT_PROCESSING.startTimer();
            // add shoppinglist from newly provisioned trader to shoppingListOidToInfos
            converter.updateShoppingListMap(results.getNewShoppingListToBuyerEntryList());
            logger.info(logPrefix + "Done performing analysis");
            projectedEntities = converter.convertFromMarket(results.getProjectedTopoEntityTOList(), topologyDTOs);
            // Create the action plan
            logger.info(logPrefix + "Creating action plan");
            final ActionPlan.Builder actionPlanBuilder = ActionPlan.newBuilder()
                    .setId(IdentityGenerator.next())
                    .setTopologyId(topologyInfo.getTopologyId())
                    .setTopologyContextId(topologyInfo.getTopologyContextId());
            results.getActionsList().stream()
                    .map(converter::interpretAction)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(actionPlanBuilder::addAction);
            actionPlan = actionPlanBuilder.build();

            priceIndexMessage = results.getPriceIndexMsg();
            logger.info(logPrefix + "Completed successfully");
            processResultTime.observe();
            state = AnalysisState.SUCCEEDED;
        } catch (InvalidTopologyException | RuntimeException e) {
            logger.error(logPrefix + e + " while running analysis", e);
            state = AnalysisState.FAILED;
            errorMsg = e.toString();
        }
        completionTime = LocalDateTime.now();
        logger.info(logPrefix + "Execution time : "
                + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds");
        completed = true;
        return true;
    }

    /**
     * Get the ID of the projected topology.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     *
     * @return The ID of the projected topology.
     */
    public Optional<Long> getProjectedTopologyId() {
        return completed ? Optional.of(projectedTopologyId) : Optional.empty();
    }

    /**
     * The projected topology resulted from the analysis run.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     * @return the projected topology
     */
    public Optional<Collection<TopologyEntityDTO>> getProjectedTopology() {
        return completed ? Optional.ofNullable(projectedEntities) : Optional.empty();
    }

    /**
     * The action plan resulted from the analysis run.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     * @return the action plan
     */
    public Optional<ActionPlan> getActionPlan() {
        return completed ? Optional.ofNullable(actionPlan) : Optional.empty();
    }

    /**
     * The price index message resulted from the analysis run.
     * The value will be available only when the run is completed successfully, meaning
     * the projected topology, the action plan and the price index message were all computed.
     * @return the price index message
     */
    public Optional<PriceIndexMessage> getPriceIndexMessage() {
        return completed ? Optional.ofNullable(priceIndexMessage) : Optional.empty();
    }

    /**
     * Start time of this analysis run.
     * @return start time of this analysis run
     */
    public LocalDateTime getStartTime() {
        return startTime;
    }

    /**
     * Completion time of this analysis run, or epoch if not yet completed.
     * @return completion time of this analysis run
     */
    public LocalDateTime getCompletionTime() {
        return completionTime;
    }

    /**
     * The topology context id of this analysis run.
     * @return the topology context id of this analysis run
     */
    public long getContextId() {
        return topologyInfo.getTopologyContextId();
    }

    /**
     * The topology id of this analysis run.
     * @return the topology id of this analysis run
     */
    public long getTopologyId() {
        return topologyInfo.getTopologyId();
    }

    /**
     * The topology type: realtime or plan.
     *
     * @return the topology type
     */
    public TopologyType getTopologyType() {
        return topologyInfo.getTopologyType();
    }

    /**
     * An unmodifiable view of the set of topology entity DTOs that this analysis run is executed on.
     * @return an unmodifiable view of the set of topology entity DTOs that this analysis run is executed on
     */
    public Set<TopologyEntityDTO> getTopology() {
        return Collections.unmodifiableSet(topologyDTOs);
    }

    /**
     * Set the {@link #state} to {@link AnalysisState#QUEUED}.
     */
    protected void queued() {
        state = AnalysisState.QUEUED;
    }

    /**
     * The state of this analysis run.
     * @return the state of this analysis run
     */
    public AnalysisState getState() {
        return state;
    }

    /**
     * The error message reported if the state of this analysis run
     * is {@link AnalysisState#FAILED FAILED}.
     * @return the error message reported if the state of this analysis run
     * is {@link AnalysisState#FAILED FAILED}
     */
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Check if the analysis run is done (either successfully or not).
     * @return true if the state is either COMPLETED or FAILED
     */
    public boolean isDone() {
        return completed;
    }

    /**
     * Get the {@link TopologyInfo} of the topology this analysis is running on.
     *
     * @return The {@link TopologyInfo}.
     */
    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return topologyInfo;
    }

    /**
     * The state of an analysis run.
     *
     * <p>An analysis run starts in the {@link INITIAL} state when it is created. If it then gets
     * executed via the {@link MarketRunner} then it transitions to {@link QUEUED} when it is
     * placed in the queue for execution. When the {@link Analysis#execute} method is invoked it
     * goes into {@link IN_PROGRESS}, and when the run completes it goes into {@link SUCCEEDED}
     * if it completed successfully, or to {@link FAILED} if it completed with an exception.
     */
    public enum AnalysisState {
        /**
         * The analysis object was created, but not yet queued or started.
         */
        INITIAL,
        /**
         * The analysis is queued for execution.
         */
        QUEUED,
        /**
         * The analysis was removed from the queue and is currently running.
         */
        IN_PROGRESS,
        /**
         * Analysis completed successfully.
         */
        SUCCEEDED,
        /**
         * Analysis completed with an exception.
         */
        FAILED;
    }
}
