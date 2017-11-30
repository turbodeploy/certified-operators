package com.vmturbo.market.runner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
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

    /**
     * Create and execute a context for a Market Analysis given a topology, an optional 'scope' to
     * apply, and a flag determining whether guaranteed buyers (VDC, VPod, DPod) are included
     * in the market analysis or not.
     *
     * The scoping algorithm is described more completely below - {@link #scopeTopology}.
     *
     * @param topologyInfo descriptive info about the topology - id, type, etc
     * @param topologyDTOs the Set of {@link TopologyEntityDTO}s that make up the topology
     * @param includeVDC specify whether guaranteed buyers (VDC, VPod, DPod) are included in the
     *                     market analysis
     */
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
            final TopologyConverter converter = new TopologyConverter(includeVDC, topologyInfo);

            Set<TraderTO> traderTOs = converter.convertToMarket(
                    Lists.newLinkedList(topologyDTOs));
            // if a scope 'seed' entity OID list is specified, then scope the topology starting with
            // the given 'seed' entities
            if (!topologyInfo.getScopeSeedOidsList().isEmpty()) {
                traderTOs = scopeTopology(traderTOs,
                    ImmutableSet.copyOf(topologyInfo.getScopeSeedOidsList()));
            }
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
     * An unmodifiable view of the set of topology entity DTOs that this analysis run is executed on.
     * @return an unmodifiable view of the set of topology entity DTOs that this analysis run is executed on
     */
    public Set<TopologyEntityDTO> getTopology() {
        return Collections.unmodifiableSet(topologyDTOs);
    }

    /**
     * Return the value of the "includeVDC" flag for this builder.
     *
     * @return the current value of the "includeVDC" flag for this builder
     */
    public boolean getIncludeVDC() {
        return includeVDC;
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
     * <p>An analysis run starts in the {@link #INITIAL} state when it is created. If it then gets
     * executed via the {@link MarketRunner} then it transitions to {@link #QUEUED} when it is
     * placed in the queue for execution. When the {@link Analysis#execute} method is invoked it
     * goes into {@link #IN_PROGRESS}, and when the run completes it goes into {@link #SUCCEEDED}
     * if it completed successfully, or to {@link #FAILED} if it completed with an exception.
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
        FAILED
    }

    /**
     * Create a subset of the original topology representing the "scoped topology" given a topology
     * and a "seed scope" set of SE's.
     * <p>
     * Starting with the "seed", follow the "buys-from" relationships "going up" and elements to
     * the "scoped topology". Along the way construct a set of traders at the "top", i.e. that do
     * not buy from any other trader. Then, from the "top" elements, follow relationships "going down"
     * and add Traders that "may sell" to the given "top" elements based on the commodities each is
     * shopping for. Recursively follow the "may sell" relationships down to the bottom, adding those
     * elements to the "scoped topology" as well.
     * <p>
     * Setting up the initial market, specifically the populateMarketsWthSellers() call on T traders
     * with M markets runs worst case O(M*T) - see the comments on that method for further details.
     * <p>
     * Once the market is set up, lookups for traders and markets are each in constant time,
     * and each trader is examined at most once, adding at worst O(T).
     *
     * @param traderTOs the topology to be scoped
     * @param seedOids the OIDs of the ServiceEntities that constitute the scope 'seed'
     **/
    @VisibleForTesting
    Set<TraderTO> scopeTopology(@Nonnull Set<TraderTO> traderTOs, @Nonnull Set<Long> seedOids) {

        // the resulting scoped topology - contains at least the seed OIDs
        Set<Long> scopedTopologyOIDs = Sets.newHashSet(seedOids);

        // create a Topology and associated Economy representing the source topology
        final Topology topology = new Topology();
        for (final TraderTO traderTO : traderTOs) {
            if (seedOids.contains(traderTO.getOid())) {
                scopedTopologyOIDs.add(traderTO.getOid());
            }
            ProtobufToAnalysis.addTrader(topology, traderTO);
        }
        // this call 'finalizes' the topology, calculating the inverted maps in the 'economy'
        // and makes the following code run more efficiently
        topology.populateMarketsWithSellers();

        // a "work queue" of entities to expand; any given OID is only ever added once -
        // if already in 'scopedTopologyOIDs' it has been considered and won't be re-expanded
        Queue<Long> suppliersToExpand = Lists.newLinkedList(seedOids);
        // the queue of entities to expand "downwards"
        Queue<Long> buyersToSatisfy = Lists.newLinkedList();
        // starting with the seed, expand "up"
        while (suppliersToExpand.size() > 0) {
            long traderOid = suppliersToExpand.remove();
            if (logger.isTraceEnabled()) {
                logger.trace("expand OID {}: {}", traderOid, topology.getTraderOids().inverse()
                        .get(traderOid).getDebugInfoNeverUseInCode());
            }
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);
            // add OIDs from which buy from this entity and which we have not already added
            final List<Long> customerOids = thisTrader.getUniqueCustomers().stream()
                    .map(trader -> topology.getTraderOids().get(trader))
                    .filter(customerOid -> !scopedTopologyOIDs.contains(customerOid) &&
                            !suppliersToExpand.contains(customerOid))
                    .collect(Collectors.toList());
            if (customerOids.size() == 0) {
                // if no customers, then "start downwards" from here
                buyersToSatisfy.add(traderOid);
            } else {
                // otherwise keep expanding upwards
                suppliersToExpand.addAll(customerOids);
                if (logger.isTraceEnabled()) {
                    logger.trace("add supplier oids ");
                    customerOids.forEach(oid -> logger.trace("{}: {}", oid, topology.getTraderOids()
                            .inverse().get(oid).getDebugInfoNeverUseInCode()));
                }
            }
        }
        logger.trace("top OIDs: {}", buyersToSatisfy);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        Queue<Long> providersExpanded = Lists.newLinkedList();
        // starting with buyersToSatisfy, expand "downwards"
        while (buyersToSatisfy.size() > 0) {
            long traderOid = buyersToSatisfy.remove();
            providersExpanded.add(traderOid);
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            final Map<ShoppingList, Market> marketsAsBuyer =
                    topology.getEconomy().getMarketsAsBuyer(thisTrader);
            // build list of sellers of markets this Trader buys from; omit Traders already expanded
            List<Long> buyerOids = marketsAsBuyer.values().stream()
                    .flatMap(market -> market.getActiveSellers().stream())
                    .map(trader -> topology.getTraderOids().get(trader))
                    .filter(buyerOid -> !providersExpanded.contains(buyerOid))
                    .collect(Collectors.toList());
            scopedTopologyOIDs.addAll(buyerOids);
            buyersToSatisfy.addAll(buyerOids);
            if (logger.isTraceEnabled()) {
                if (buyerOids.size() > 0) {
                    logger.trace("add buyer oids: ");
                    buyerOids.forEach(oid -> logger.trace("{}: {}", oid, topology.getTraderOids()
                            .inverse().get(oid).getDebugInfoNeverUseInCode()));
                }
            }
        }
        // return the subset of the original TraderTOs that correspond to the scoped topology
        // TODO: improve the speed of this operation by iterating over the scopedTopologyIds instead
        // of the full topology - OM-27745
        return traderTOs.stream()
                .filter(traderTO -> scopedTopologyOIDs.contains(traderTO.getOid()))
                .collect(Collectors.toSet());
    }

    /**
     * Helper for building an Analysis object. Useful since there are a number of
     * parameters to pass to the market.
     */
    public static class AnalysisBuilder {
        // basic information about this topology, including the IDs, type, timestamp, etc.
        private TopologyInfo topologyInfo = TopologyInfo.getDefaultInstance();
        // the Topology DTOs that make up the topology
        private Set<TopologyEntityDTO> topologyDTOs = Collections.emptySet();
        // specify whether the analysis should include guaranteed buyers in the market analysis
        private boolean includeVDC = false;

        /**
         * Capture the {@link TopologyInfo} describing this Analysis, including the IDs, type,
         * timestamp, etc.
         *
         * @param topologyInfo the {@link TopologyInfo} for this Analysys
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setTopologyInfo(TopologyInfo topologyInfo) {
            this.topologyInfo = topologyInfo;
            return this;
        }

        /**
         * Capture the Set of TopologyEntityDTOs to be analyzed.
         *
         * @param topologyDTOs the Set of TopologyEntityDTOs to be analyzed
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setTopologyDTOs(Set<TopologyEntityDTO> topologyDTOs) {
            this.topologyDTOs = topologyDTOs;
            return this;
        }

        /**
         * Configure whether to include guaranteed buyers (VDC, VPod, DPod) in the analysis.
         *
         * @param includeVDC true if the guaranteed buyers (VDC, VPod, DPod) should be included
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setIncludeVDC(boolean includeVDC) {
            this.includeVDC = includeVDC;
            return this;
        }

        /**
         * Request a new Analysis object be built using the values specified.
         *
         * @return the newly build Analysis object initialized from the Builder fields.
         */
        public Analysis build() {
            return new Analysis(topologyInfo, topologyDTOs, includeVDC);
        }
    }

    /**
     * Define a factory for creating new AnalysisBuilders. This will chiefly be used for testing,
     * allowing a test to override the 'newAnalysisBuilder()' method and return an AnalysisBuilder
     * that will build a Mock Analysis object.
     */
    public static class AnalysisFactory {
        public AnalysisBuilder newAnalysisBuilder() {
            return new AnalysisBuilder();
        }
    }
}
