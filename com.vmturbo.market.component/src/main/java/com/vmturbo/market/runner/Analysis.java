package com.vmturbo.market.runner;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Analysis execution and properties. This can be for a scoped plan or for a real-time market.
 */
public class Analysis {
    // Analysis started (kept true also when it is completed).
    private AtomicBoolean started = new AtomicBoolean();
    // Analysis completed (successfully or not).
    private boolean completed = false;

    private final boolean includeVDC;
    private Instant startTime = Instant.EPOCH;
    private Instant completionTime = Instant.EPOCH;
    private final Map<Long, TopologyEntityDTO> topologyDTOs;
    private final String logPrefix;

    private final TopologyConverter converter;

    private Map<Long, TopologyEntityDTO> scopeEntities = Collections.emptyMap();

    private Collection<ProjectedTopologyEntity> projectedEntities = null;
    private final long projectedTopologyId;
    private ActionPlan actionPlan = null;

    private String errorMsg;
    private AnalysisState state;

    private final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    private static final DataMetricSummary TOPOLOGY_SCOPING_SUMMARY = DataMetricSummary.builder()
        .withName("mkt_economy_scoping_duration_seconds")
        .withHelp("Time to scope the economy for analysis.")
        .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
        .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
        .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
        .withMaxAgeSeconds(60 * 10) // 10 mins.
        .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
        .build()
        .register();

    private static final DataMetricSummary TOPOLOGY_CONVERT_TO_TRADER_SUMMARY = DataMetricSummary.builder()
        .withName("mkt_economy_convert_to_traders_duration_seconds")
        .withHelp("Time to convert from TopologyDTO to TraderTO before sending for analysis.")
        .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
        .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
        .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
        .withMaxAgeSeconds(60 * 10) // 10 mins.
        .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
        .build()
        .register();

    private static final DataMetricSummary TOPOLOGY_CONVERT_FROM_TRADER_SUMMARY = DataMetricSummary.builder()
        .withName("mkt_economy_convert_from_traders_duration_seconds")
        .withHelp("Time to convert from TraderTO back to TopologyDTO for projected topology after analysis.")
        .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
        .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
        .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
        .withMaxAgeSeconds(60 * 10) // 10 mins.
        .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
        .build()
        .register();

    /**
     * The clock used to time market analysis.
     */
    private final Clock clock;

    private final Map<String, Setting> globalSettingsMap;

    private final Optional<Integer> maxPlacementsOverride;

    private final float rightsizeLowerWatermark;

    private final float rightsizeUpperWatermark;

    /**
     * The quote factor controls the aggressiveness with which the market suggests moves.
     *
     * It's a number between 0 and 1, so that Move actions are only generated if
     * best-quote < quote-factor * current-quote. That means that if we only want Moves that
     * result in at least 25% improvement we should use a quote-factor of 0.75.
     */
    private final float quoteFactor;

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
     * @param globalSettingsMap Used to look up settings to control the behavior of market analysis.
     * @param maxPlacementsOverride If present, overrides the default number of placement rounds performed
     *                              by the market during analysis.
     * @param clock The clock used to time market analysis.
     * @param rightsizeLowerWatermark the minimum utilization threshold, if entity utilization is below
     *                                it, Market could generate resize down actions.
     * @param rightsizeUpperWatermark the maximum utilization threshold, if entity utilization is above
     *                                it, Market could generate resize up actions.
     * @param quoteFactor to be used. See {@link Analysis#quoteFactor}.
     */
    public Analysis(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                    final boolean includeVDC,
                    @Nonnull final Map<String, Setting> globalSettingsMap,
                    @Nonnull final Optional<Integer> maxPlacementsOverride,
                    @Nonnull final Clock clock,
                    final float rightsizeLowerWatermark,
                    final float rightsizeUpperWatermark,
                    final float quoteFactor) {
        this.topologyInfo = topologyInfo;
        this.topologyDTOs = topologyDTOs.stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        this.includeVDC = includeVDC;
        this.state = AnalysisState.INITIAL;
        logPrefix = topologyInfo.getTopologyType() + " Analysis " +
            topologyInfo.getTopologyContextId() + " with topology " +
            topologyInfo.getTopologyId() + " : ";
        this.projectedTopologyId = IdentityGenerator.next();
        this.globalSettingsMap = globalSettingsMap;
        this.clock = Objects.requireNonNull(clock);
        this.maxPlacementsOverride = Objects.requireNonNull(maxPlacementsOverride);
        this.rightsizeLowerWatermark = rightsizeLowerWatermark;
        this.rightsizeUpperWatermark = rightsizeUpperWatermark;
        this.converter = new TopologyConverter(topologyInfo, includeVDC, quoteFactor);
        this.quoteFactor = quoteFactor;
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
     *
     * @return true if this is the first invocation of this method, false otherwise.
     */
    public boolean execute() {
        if (started.getAndSet(true)) {
            logger.error(" {} Completed or being computed", logPrefix);
            return false;
        }
        state = AnalysisState.IN_PROGRESS;
        startTime = clock.instant();
        logger.info("{} Started", logPrefix);
        try {

            final DataMetricTimer conversionTimer = TOPOLOGY_CONVERT_TO_TRADER_SUMMARY.startTimer();
            Set<TraderTO> traderTOs = converter.convertToMarket(topologyDTOs);
            conversionTimer.observe();

            // if a scope 'seed' entity OID list is specified, then scope the topology starting with
            // the given 'seed' entities
            if (!topologyInfo.getScopeSeedOidsList().isEmpty()) {
                try (final DataMetricTimer scopingTimer = TOPOLOGY_SCOPING_SUMMARY.startTimer()) {
                    traderTOs = scopeTopology(traderTOs,
                        ImmutableSet.copyOf(topologyInfo.getScopeSeedOidsList()));
                }

                // save the scoped topology for later broadcast
                scopeEntities = traderTOs.stream()
                        .collect(Collectors.toMap(TraderTO::getOid,
                                trader -> topologyDTOs.get(trader.getOid())));
            }

            // remove any (scoped) traders that may have been flagged for removal
            // We are doing this after the convertToMarket() function because we need the original
            // traders available in the "old providers maps" so the biclique calculation can
            // preserve the original topology structure. We may refactor this in a way that lets us
            // remove the old provider map though -- see OM-26631.
            //
            // Note that although we do NOT attempt to unplace buyers of the entities being removed
            // here, the effect on the analysis is exactly equivalent if we had unplaced them
            // (as of 4/6/2016) because attempting to buy from a non-existing trader results in
            // an infinite quote which is exactly the same as not having a provider.
            final Set<Long> oidsToRemove = topologyDTOs.values().stream()
                    .filter(dto -> dto.hasEdit())
                    .filter(dto -> dto.getEdit().hasRemoved()
                                || dto.getEdit().hasReplaced())
                    .map(TopologyEntityDTO::getOid)
                    .collect(Collectors.toSet());
            if (oidsToRemove.size() > 0) {
                logger.debug("Removing {} traders before analysis: ", oidsToRemove.size());
                traderTOs = traderTOs.stream()
                        .filter(traderTO -> !(oidsToRemove.contains(traderTO.getOid())))
                        .collect(Collectors.toSet());
            }

            if (logger.isDebugEnabled()) {
                logger.debug(traderTOs.size() + " Economy DTOs");
            }
            if (logger.isTraceEnabled()) {
                traderTOs.stream().map(dto -> "Economy DTO: " + dto).forEach(logger::trace);
            }

            final AnalysisResults results = TopologyEntitiesHandler.performAnalysis(traderTOs,
                topologyInfo, globalSettingsMap, maxPlacementsOverride, rightsizeLowerWatermark,
                rightsizeUpperWatermark);
            final DataMetricTimer processResultTime = RESULT_PROCESSING.startTimer();
            // add shoppinglist from newly provisioned trader to shoppingListOidToInfos
            converter.updateShoppingListMap(results.getNewShoppingListToBuyerEntryList());
            logger.info(logPrefix + "Done performing analysis");

            try (DataMetricTimer convertFromTimer = TOPOLOGY_CONVERT_FROM_TRADER_SUMMARY.startTimer()) {
                projectedEntities = converter.convertFromMarket(
                    results.getProjectedTopoEntityTOList(),
                    topologyDTOs,
                    results.getPriceIndexMsg());
            }

            // Create the action plan
            logger.info(logPrefix + "Creating action plan");
            final ActionPlan.Builder actionPlanBuilder = ActionPlan.newBuilder()
                    .setId(IdentityGenerator.next())
                    .setTopologyId(topologyInfo.getTopologyId())
                    .setTopologyContextId(topologyInfo.getTopologyContextId())
                    .setAnalysisStartTimestamp(startTime.toEpochMilli());
            // We shouldn't need to put the original topology entities into the map, because
            // the entities the market operates on (i.e. has actions for) should appear in
            // the projected topology.
            //
            // We look through the projected traders directly instead of the projected entities
            // because iterating through the projected entities will actually convert all the
            // traders (it's a lazy-transforming list at the time of this writing - Mar 20 2018).
            final Map<Long, Integer> entityIdToType = results.getProjectedTopoEntityTOList().stream()
                    .collect(Collectors.toMap(TraderTO::getOid, TraderTO::getType));
            results.getActionsList().stream()
                    .map(action -> converter.interpretAction(action, entityIdToType))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(actionPlanBuilder::addAction);
            logger.info(logPrefix + "Completed successfully");
            processResultTime.observe();
            state = AnalysisState.SUCCEEDED;

            completionTime = clock.instant();
            actionPlan = actionPlanBuilder.setAnalysisCompleteTimestamp(completionTime.toEpochMilli())
                .build();
        } catch (RuntimeException e) {
            logger.error(logPrefix + e + " while running analysis", e);
            state = AnalysisState.FAILED;
            completionTime = clock.instant();
            errorMsg = e.toString();
        }

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
    public Optional<Collection<ProjectedTopologyEntity>> getProjectedTopology() {
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
     * Start time of this analysis run.
     * @return start time of this analysis run
     */
    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Completion time of this analysis run, or epoch if not yet completed.
     * @return completion time of this analysis run
     */
    public Instant getCompletionTime() {
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
     *
     * If the analysis was scoped, this will return only the entities in the scope, because those
     * were the entities that the market analysis actually ran on.
     *
     * @return an unmodifiable view of the map of topology entity DTOs that this analysis run is executed on
     */
    public Map<Long, TopologyEntityDTO> getTopology() {
        return isScoped() ? scopeEntities : Collections.unmodifiableMap(topologyDTOs);
    }

    /**
     * An unmodifiable view of the original topology input into the market. Whether or not the
     * analysis was scoped, this will return all the input entities (i.e. the whole topology).
     *
     * @return an unmodifiable view of the map of topology entity DTOs passed to the analysis,
     *         prior to scoping.
     */
    @Nonnull
    public Map<Long, TopologyEntityDTO> getOriginalInputTopology() {
        return Collections.unmodifiableMap(topologyDTOs);
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
     * Get the override for the number of maximum placement iterations.
     *
     * @return the maxPlacementsOverride.
     */
    public Optional<Integer> getMaxPlacementsOverride() {
        return maxPlacementsOverride;
    }

    /**
     * Get the minimum utilization threshold.
     *
     * @return the rightsizeLowerWatermark.
     */
    public float getRightsizeLowerWatermark() {
        return rightsizeLowerWatermark;
    }

    /**
     * Get the maximum utilization threshold.
     *
     * @return the rightsizeUpperWatermark.
     */
    public float getRightsizeUpperWatermark() {
        return rightsizeUpperWatermark;
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
     * Check if the analysis is running on a scoped topology.
     *
     * @return true if the analysis is running on a scoped topology, false otherwise.
     */
    public boolean isScoped() {
        return !topologyInfo.getScopeSeedOidsList().isEmpty();
    }

    /**
     * Get the OIDs of entities skipped during conversion of {@link TopologyEntityDTO}s to
     * {@link TraderTO}s.
     *
     * @return A set of the OIDS of entities skipped during conversion.
     */
    @Nonnull
    public Set<Long> getSkippedEntities() {
        if (!isDone()) {
            throw new IllegalStateException("Attempting to get skipped entities before analysis is done.");
        }
        return converter.getSkippedEntities().stream()
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());
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
     * Get the quote factor used for this analysis.
     *
     * @return The quote factor.
     */
    public float getQuoteFactor() {
        return quoteFactor;
    }

    /**
     * Get the global settings map used for this analysis.
     *
     * @return The map of global setting values arranged by name.
     */
    @Nonnull
    public Map<String, Setting> getGlobalSettingsMap() {
        return Collections.unmodifiableMap(globalSettingsMap);
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
     *
     * <p>Any unplaced service entities are automatically included in the "scoped topology"
     *
     * <p>Starting with the "seed", follow the "buys-from" relationships "going up" and elements to
     * the "scoped topology". Along the way construct a set of traders at the "top", i.e. that do
     * not buy from any other trader. Then, from the "top" elements, follow relationships "going down"
     * and add Traders that "may sell" to the given "top" elements based on the commodities each is
     * shopping for. Recursively follow the "may sell" relationships down to the bottom, adding those
     * elements to the "scoped topology" as well.
     *
     * <p>Setting up the initial market, specifically the populateMarketsWthSellers() call on T traders
     * with M markets runs worst case O(M*T) - see the comments on that method for further details.
     *
     * <p>Once the market is set up, lookups for traders and markets are each in constant time,
     * and each trader is examined at most once, adding at worst O(T).
     *
     * @param traderTOs the topology to be scoped
     * @param seedOids the OIDs of the ServiceEntities that constitute the scope 'seed'
     * @return Set of {@link TraderTO} objects
     **/
    @VisibleForTesting
    Set<TraderTO> scopeTopology(@Nonnull Set<TraderTO> traderTOs, @Nonnull Set<Long> seedOids) {
        // the resulting scoped topology - contains at least the seed OIDs
        final Set<Long> scopedTopologyOIDs = Sets.newHashSet(seedOids);

        // holder for the scoped topology
        final Set<TraderTO> traderTOsInScopedTopology = Sets.newHashSet();

        // create a Topology and associated Economy representing the source topology
        final Topology topology = new Topology();
        for (final TraderTO traderTO : traderTOs) {
            // include all "uplaced" traders in the final scoped topology
            if (traderIsUnplaced(traderTO)) {
                traderTOsInScopedTopology.add(traderTO);
            } else {
                // include all "placed" traders in the market for calculating the scope
                ProtobufToAnalysis.addTrader(topology, traderTO);
            }
        }

        // this call 'finalizes' the topology, calculating the inverted maps in the 'economy'
        // and makes the following code run more efficiently
        topology.populateMarketsWithSellers();

        // a "work queue" of entities to expand; any given OID is only ever added once -
        // if already in 'scopedTopologyOIDs' it has been considered and won't be re-expanded
        Queue<Long> suppliersToExpand = Lists.newLinkedList(seedOids);

        // the queue of entities to expand "downwards"
        Queue<Long> buyersToSatisfy = Lists.newLinkedList();
        Set<Long> visited = new HashSet<>();

        // starting with the seed, expand "up"
        while (!suppliersToExpand.isEmpty()) {
            long traderOid = suppliersToExpand.remove();

            if (!topology.getTraderOids().containsValue(traderOid)) {
                // not all entities are guaranteed to be in the traders set -- the
                // market will exclude entities based on factors such as entitystate, for example.
                // If we encounter an entity that is not in the market, don't expand it any further.
                logger.debug("Skipping OID {}, as it is not in the market.", traderOid);
                continue;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("expand OID {}: {}", traderOid, topology.getTraderOids().inverse()
                        .get(traderOid).getDebugInfoNeverUseInCode());
            }
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);
            // add OIDs of traders THAT buy from this entity which we have not already added
            final List<Long> customerOids = thisTrader.getUniqueCustomers().stream()
                    .map(trader -> topology.getTraderOids().get(trader))
                    .filter(customerOid -> !scopedTopologyOIDs.contains(customerOid) &&
                            !suppliersToExpand.contains(customerOid))
                    .collect(Collectors.toList());
            if (customerOids.size() == 0) {
                // if no customers, then "start downwards" from here
                if (!visited.contains(traderOid)) {
                    buyersToSatisfy.add(traderOid);
                    visited.add(traderOid);
                }
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
        Set<Long> providersExpanded = new HashSet<>();
        // starting with buyersToSatisfy, expand "downwards"
        while (!buyersToSatisfy.isEmpty()) {
            long traderOid = buyersToSatisfy.remove();
            providersExpanded.add(traderOid);
            Trader thisTrader = topology.getTraderOids().inverse().get(traderOid);
            // build list of sellers of markets this Trader buys from; omit Traders already expanded
            Set<Trader> potentialSellers = topology.getEconomy().getPotentialSellers(thisTrader);
            List<Long> sellersOids = potentialSellers.stream()
                            .map(trader -> topology.getTraderOids().get(trader))
                            .filter(buyerOid -> !providersExpanded.contains(buyerOid))
                            .collect(Collectors.toList());
            scopedTopologyOIDs.addAll(sellersOids);
            for (Long buyerOid : sellersOids) {
                if (visited.contains(buyerOid)) {
                    continue;
                }
                visited.add(buyerOid);
                buyersToSatisfy.add(buyerOid);
            }

            if (logger.isTraceEnabled()) {
                if (sellersOids.size() > 0) {
                    logger.trace("add buyer oids: ");
                    sellersOids.forEach(oid -> logger.trace("{}: {}", oid, topology.getTraderOids()
                            .inverse().get(oid).getDebugInfoNeverUseInCode()));
                }
            }
        }
        // return the subset of the original TraderTOs that correspond to the scoped topology
        // TODO: improve the speed of this operation by iterating over the scopedTopologyIds instead
        // of the full topology - OM-27745
        traderTOs.stream()
                .filter(traderTO -> scopedTopologyOIDs.contains(traderTO.getOid()))
                .forEach(traderTOsInScopedTopology::add);
        return traderTOsInScopedTopology;
    }

    /**
     * Determine if a TraderTO is unplaced. It is considered unplaced if any of the commodities
     * bought do not have a supplier.
     *
     * @param traderTO the TraderTO to test for suppliers
     * @return true iff any of the commodities bought have no supplier
     */
    private boolean traderIsUnplaced(@Nonnull final TraderTO traderTO) {
        return traderTO.getShoppingListsList().stream()
                .anyMatch(shoppingListTO -> !shoppingListTO.hasSupplier() ||
                        shoppingListTO.getSupplier() <= 0);
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
        // The clock to use when generating timestamps.
        private Clock clock = Clock.systemUTC();

        private Optional<Integer> maxPlacementsOverride = Optional.empty();
        private Map<String, Setting> settingsMap = Collections.emptyMap();

        // The minimum utilization threshold, if entity's utilization is below threshold,
        // Market could generate resize down action.
        private float rightsizeLowerWatermark;
        // The maximum utilization threshold, if entity's utilization is above threshold,
        // Market could generate resize up action.
        private float rightsizeUpperWatermark;
        // quoteFactor to be used by move recommendations.
        private float quoteFactor = AnalysisUtil.QUOTE_FACTOR;

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
         *
         * @param settingsMap The map of global settings retrieved from the group client.
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setSettingsMap(@Nonnull final Map<String, Setting> settingsMap) {
            this.settingsMap = settingsMap;
            return this;
        }

        /**
         * If present, overrides the default number of placement rounds performed by the market during analysis.
         * If empty, uses the default value from the analysis project.
         *
         * @param maxPlacementsOverride the configuration store.
         * @return this Builder to support flow style.
         */
        public AnalysisBuilder setMaxPlacementsOverride(@Nonnull final Optional<Integer> maxPlacementsOverride) {
            this.maxPlacementsOverride = Objects.requireNonNull(maxPlacementsOverride);
            return this;
        }

        /**
         * Configure the minimum utilization threshold.
         *
         * @param rightsizeLowerWatermark minimum utilization threshold.
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setRightsizeLowerWatermark(@Nonnull final float rightsizeLowerWatermark) {
            this.rightsizeLowerWatermark = rightsizeLowerWatermark;
            return this;
        }

        /**
         * Configure the maximum utilization threshold.
         *
         * @param rightsizeUpperWatermark maximum utilization threshold.
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setRightsizeUpperWatermark(@Nonnull final float rightsizeUpperWatermark) {
            this.rightsizeUpperWatermark = rightsizeUpperWatermark;
            return this;
        }

        /**
         * Set the clock used to time market analysis.
         * If not explicitly set, will use the System UTC clock.
         *
         * @param clock The clock used to time market analysis.
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setClock(@Nonnull final Clock clock) {
            this.clock = Objects.requireNonNull(clock);
            return this;
        }

        /**
         * Set the quote factor for this round of analysis.
         *
         * @param quoteFactor The quote factor to be used in market analysis.
         * @return this Builder to support flow style
         */
        public AnalysisBuilder setQuoteFactor(final float quoteFactor) {
            this.quoteFactor = quoteFactor;
            return this;
        }

        /**
         * Request a new Analysis object be built using the values specified.
         *
         * @return the newly build Analysis object initialized from the Builder fields.
         */
        public Analysis build() {
            return new Analysis(topologyInfo, topologyDTOs, includeVDC, settingsMap,
                maxPlacementsOverride, clock, rightsizeLowerWatermark, rightsizeUpperWatermark, quoteFactor);
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
