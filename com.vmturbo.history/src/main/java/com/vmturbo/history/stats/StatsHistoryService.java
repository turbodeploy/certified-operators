package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.StringConstants.AVG_VALUE;
import static com.vmturbo.reports.db.StringConstants.CAPACITY;
import static com.vmturbo.reports.db.StringConstants.COMMODITY_KEY;
import static com.vmturbo.reports.db.StringConstants.MAX_VALUE;
import static com.vmturbo.reports.db.StringConstants.MIN_VALUE;
import static com.vmturbo.reports.db.StringConstants.PRODUCER_UUID;
import static com.vmturbo.reports.db.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.reports.db.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.reports.db.StringConstants.RELATION;
import static com.vmturbo.reports.db.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.reports.db.StringConstants.UTILIZATION;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Summary;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.reports.db.CommodityTypes;
import com.vmturbo.reports.db.RelationType;
import com.vmturbo.reports.db.StringConstants;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.reports.db.abstraction.tables.records.ScenariosRecord;

/**
 * Handles incoming RPC calls to History Component to return Stats information.
 * Calls a {@link LiveStatsReader} to fetch the desired data, and then converts the resulting
 * DB {@link Record}s into a protobuf response value.
 **/
public class StatsHistoryService extends StatsHistoryServiceGrpc.StatsHistoryServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final LiveStatsReader liveStatsReader;
    private final HistorydbIO historydbIO;
    private final PlanStatsReader planStatsReader;
    private final long realtimeContextId;
    private final ClusterStatsWriter clusterStatsWriter;

    private final ProjectedStatsStore projectedStatsStore;

    private static final Summary GET_STATS_SNAPSHOT_DURATION_SUMMARY = Summary.build()
        .name("history_get_stats_snapshot_duration_seconds")
        .help("Duration in seconds it takes the history component to get a stats snapshot for a group or entity.")
        .labelNames("context_type")
        .register();

    StatsHistoryService(final long realtimeContextId,
                        @Nonnull final LiveStatsReader liveStatsReader,
                        @Nonnull final PlanStatsReader planStatsReader,
                        @Nonnull ClusterStatsWriter clusterStatsWriter,
                        @Nonnull final HistorydbIO historydbIO,
                        @Nonnull final ProjectedStatsStore projectedStatsStore) {
        this.realtimeContextId = realtimeContextId;
        this.liveStatsReader = Objects.requireNonNull(liveStatsReader);
        this.planStatsReader = Objects.requireNonNull(planStatsReader);
        this.clusterStatsWriter = Objects.requireNonNull(clusterStatsWriter);
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
    }

    /**
     * Get stats from the latest projected topology. Will return a {@link ProjectedStatsResponse}
     * with no data if the projected stats are not available.
     *
     * @param request gives the entities and stats to search for
     * @param responseObserver the sync for the result value {@link ProjectedStatsResponse}
     */
    public void getProjectedStats(ProjectedStatsRequest request,
                                  StreamObserver<ProjectedStatsResponse> responseObserver) {
        final ProjectedStatsResponse.Builder builder = ProjectedStatsResponse.newBuilder();
        projectedStatsStore.getStatSnapshot(request).ifPresent(builder::setSnapshot);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * Get stats from the latest projected topology on a per-entity basis. Will return
     * an {@link EntityStats} response with no stats for each entity in the request list
     * if the projected stats are not available.
     *
     * @param request gives entities and stats to search for
     * @param responseObserver the sync for {@link EntityStats}, one for each entity in the request
     */
    public void getProjectedEntityStats(@Nonnull ProjectedStatsRequest request,
                                        @Nonnull StreamObserver<EntityStats> responseObserver) {
        final Set<String> commodityNames = new HashSet<>(request.getCommodityNameList());
        final Set<Long> targetEntities = new HashSet<>(request.getEntitiesList());
        targetEntities.forEach(entityOid -> {
            final EntityStats.Builder entityStatsBuilder = EntityStats.newBuilder()
                    .setOid(entityOid);
            projectedStatsStore.getStatSnapshotForEntities(Collections.singleton(entityOid),
                    commodityNames).ifPresent(entityStatsBuilder::addStatSnapshots);
            responseObserver.onNext(entityStatsBuilder.build());
        });
        responseObserver.onCompleted();
    }

    /**
     * Fetch a sequence of StatSnapshot's based on the time range and filters in the StatsRequest.
     * The stats may be taken from the live topology history or the plan topology history depending
     * on the ID in the request.
     * <p>
     * Unfortunately, we need to determine, by a lookup, which type of entity ID(s) we are
     * to gather stats for. This call has been overloaded (ultimately) by the External REST API,
     * which doesn't distinguish between stats requests for groups vs. individual SEs, live
     * vs. plan topologies, etc.
     * <p>
     * The 'entitiesList' in the {@link EntityStatsRequest} may be:
     * <ul>
     * <li>a single-element list containing the distinguished ID for the Real-Time Topology = "Market"
     * <li>a single-element list containing the OID of a plan topology - a numeric (long)
     * <li>a list of one or more {@link com.vmturbo.api.dto.ServiceEntityApiDTO} OIDs - these are
     * numeric (longs)
     * </ul>
     *
     * @param request a set of parameters describing how to fetch the snapshot and what entities.
     * @param responseObserver the sync for each result value {@link StatSnapshot}
     */
    @Override
    public void getAveragedEntityStats(EntityStatsRequest request,
                               StreamObserver<StatSnapshot> responseObserver) {
        try {
            final List<Long> entitiesList = request.getEntitiesList();
            final StatsFilter filter = request.getFilter();
            // fetch stats for this request
            Long startDate = null;
            if (filter.hasStartDate()) {
                startDate = filter.getStartDate();
            }
            Long endDate = null;
            if (filter.hasEndDate()) {
                endDate = filter.getEndDate();
            }

            final List<String> commodityNames = filter.getCommodityNameList();

            // determine if this request is for stats for a plan topology; for efficiency, check
            // first for the special case ID of the entire live topology, "Market" (i.e. not a plan)
            boolean isPlan = entitiesList.size() == 1
                    && entitiesList.get(0) != realtimeContextId
                    && historydbIO.entityIdIsPlan(entitiesList.get(0));

            final Summary.Timer timer = GET_STATS_SNAPSHOT_DURATION_SUMMARY
                .labels(isPlan ?
                        SharedMetrics.PLAN_CONTEXT_TYPE_LABEL :
                        SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                .startTimer();

            if (isPlan) {
                // read from plan topologies
                returnPlanTopologyStats(responseObserver, entitiesList.get(0),
                        commodityNames);
            } else {
                // read from live topologies
                returnLiveMarketStats(responseObserver, startDate, endDate, entitiesList,
                        commodityNames);
            }

            timer.observeDuration();
        } catch (Exception e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
        }
    }

    @Override
    public void getEntityStats(@Nonnull Stats.EntityStatsRequest request,
                                 @Nonnull StreamObserver<EntityStats> responseObserver) {
        // unfortunately we need to translate the OID Long values into Strings to call HistorydbIO
        List <String> oidStrings = request.getEntitiesList().stream()
                .map(l -> Long.toString (l))
                .collect (Collectors.toList());

        // gather stats and return them for each entity one at a time
        try {
            for (long entityOid : request.getEntitiesList()) {

                final StatsFilter statsFilter = request.getFilter();
                List<Record> statDBRecords = liveStatsReader.getStatsRecords(
                        Collections.singletonList(Long.toString(entityOid)),
                        statsFilter.getStartDate(), statsFilter.getEndDate(),
                        statsFilter.getCommodityNameList());
                EntityStats.Builder statsForEntity = EntityStats.newBuilder()
                        .setOid(entityOid);

                // organize the stats DB records for this entity into StatSnapshots and add to response
                createStatSnapshots(statDBRecords, false, statSnapshotBuilder ->
                        statsForEntity.addStatSnapshots(statSnapshotBuilder.build()));

                // done with this entity
                responseObserver.onNext(statsForEntity.build());

            }
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("DB Error fetching stats for: " + oidStrings)
                    .withCause(e)
                    .asException());
        } catch (Exception e) {
            logger.error("Internal exception fetching stats for: " + oidStrings, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + oidStrings)
                    .withCause(e)
                    .asException());
        }
    }



    @Override
    public void getClusterStats(@Nonnull Stats.ClusterStatsRequest request,
                                @Nonnull StreamObserver<StatSnapshot> responseObserver) {
        responseObserver.onCompleted();
    }

    /**
     * Calculate a Cluster Stats Rollup, if necessary, and persist to the appropriate stats table.
     *
     * The request contains all the clusters to be rolled up. For each cluster in the request
     * we get the ID, and the ID's of the cluster members.
     *
     * @param request a {link @ClusterRollupRequest} containing the cluster DTOs to be persisted.
     * @param responseObserver an indication that the request has completed - there is no data
     *                         returned with this response.
     */
    @Override
    public void computeClusterRollup(Stats.ClusterRollupRequest request,
                                     StreamObserver<Stats.ClusterRollupResponse> responseObserver) {
        try {
            // Filter by type as an extra precaution.
            clusterStatsWriter.rollupClusterStats(request.getClustersToRollupList().stream()
                .filter(group -> group.getType().equals(Type.CLUSTER))
                .collect(Collectors.toMap(Group::getId, Group::getCluster)));
            responseObserver.onNext(Stats.ClusterRollupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            // acatch and handle any internal exception so we return a useful error to caller
            logger.error("Internal exception rolling up clusters, request: " + request, e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error rolling up clusters")
                    .withCause(e)
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveClusterHeadroom(Stats.SaveClusterHeadroomRequest request,
                                    StreamObserver<Stats.SaveClusterHeadroomResponse> responseObserver) {
        logger.info("Got request to save cluster info: {}", request);
        responseObserver.onNext(Stats.SaveClusterHeadroomResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Gather stats from the PLAN topology history for the given topologyContextID. There will be
     * only one.
     * Package the stats up in a StatSnapshot response protobuf and pass it back via the chunking
     * response "onNext()" cal.
     *
     * @param responseObserver the chunking channel on which the response should be returned
     * @param topologyContextId the context id for the plan topology to look up
     * @param commodityNames the names of the commodities to include.
     */
    private void returnPlanTopologyStats(StreamObserver<StatSnapshot> responseObserver,
                                         long topologyContextId,
                                         List<String> commodityNames) {
        StatSnapshot.Builder statSnapshotResponseBuilder = StatSnapshot.newBuilder();
        List<MktSnapshotsStatsRecord> dbSnapshotStatsRecords;
        try {
            dbSnapshotStatsRecords = planStatsReader.getStatsRecords(
                    topologyContextId, commodityNames);
        } catch (VmtDbException | SQLException e) {
            throw new RuntimeException("Error fetching plan market stats for "
                    + topologyContextId, e);
        }
        for (MktSnapshotsStatsRecord statsDBRecord : dbSnapshotStatsRecords) {
            final StatRecord statResponseRecord = buildStatRecord(
                    statsDBRecord.getPropertyType(),
                    statsDBRecord.getPropertySubtype(),
                    statsDBRecord.getCapacity() == null ? null : statsDBRecord.getCapacity()
                            .floatValue(),
                    null /* producerId */,
                    statsDBRecord.getAvgValue().floatValue(),
                    statsDBRecord.getMinValue().floatValue(),
                    statsDBRecord.getMaxValue().floatValue(),
                    null /* commodityKey */,
                    statsDBRecord.getAvgValue().floatValue(), /* Since there is only one value,
                                                             set the total equal to the average.*/
                    // (Feb 3, 2017) Currently, unlike in the real-time market, UI
                    // doesn't use the 'relation' data for the plan results. Here, set the
                    // property as "plan" to indicate this record is for a plan result.
                    "plan" /*relation*/);
            statSnapshotResponseBuilder.addStatRecords(statResponseRecord);
        }
        Optional<ScenariosRecord> planStatsRecord = historydbIO.getScenariosRecord(topologyContextId);
        if (planStatsRecord.isPresent()) {
            final long planTime = planStatsRecord.get().getCreateTime().getTime();
            statSnapshotResponseBuilder.setSnapshotDate(DateTimeUtil.toString(planTime));
            // TODO: the timestamps should be based on listening to the Plan Orchestrator OM-14839
            statSnapshotResponseBuilder.setStartDate(planTime);
            statSnapshotResponseBuilder.setEndDate(planTime);
            responseObserver.onNext(statSnapshotResponseBuilder.build());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    "Unable to find scenarios record for " + topologyContextId).asException());
        }

    }

    /**
     * Read the stats DB for the live market, stored in xxx_stats_yyy table, organized by
     * ServiceEntity type (e.g. pm_stats_yyy and vm_stats_yyy) and by time frame, e.g.
     * xxx_stats_latest, xxx_stats__by_hour, xxx_stats_by_day, xxx_stats_by_month.
     *
     * @param responseObserver the chunking channel on which the response should be returned
     * @param startDate return stats with date equal to or after this date
     * @param endDate return stats with date before this date
     * @param entities A list of service entity OIDs; an empty list implies full market
     * @param commodityNames the names of the commodities to include.
     * @throws VmtDbException if error writing to the db
     */
    private void returnLiveMarketStats(StreamObserver<StatSnapshot> responseObserver,
                                       Long startDate, Long endDate, List<Long> entities,
                                       List<String> commodityNames) throws VmtDbException {

        // get a full list of stats that satisfy this request, depending on the entity request
        final List<Record> statDBRecords;
        final boolean fullMarket = entities.size() == 0;
        if (fullMarket) {
            statDBRecords = liveStatsReader.getFullMarketStatsRecords(startDate, endDate,
                    commodityNames);
        } else {
            // partial request
            statDBRecords = liveStatsReader.getStatsRecords(
                    entities.stream()
                        .map(id -> Long.toString(id))
                        .collect(Collectors.toList()),
                    startDate, endDate, commodityNames);
        }

        // organize the stats DB records into StatSnapshots and return them to the caller
        createStatSnapshots(statDBRecords, fullMarket, statSnapshotBuilder ->
                responseObserver.onNext(statSnapshotBuilder.build()));

        responseObserver.onCompleted();
    }

    /**
     * Process the given DB Stats records, organizing into {@link StatSnapshot} by time and commodity,
     * and then invoking the handler ({@link Consumer}) from the caller on each StatsSnapshot that is built.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @param fullMarket is this a query against the full market table vs. individual SE type
     * @param handleSnapshot a function ({@link Consumer}) to call for each new {@link StatSnapshot}
     */

    private void createStatSnapshots(@Nonnull List<Record> statDBRecords,
                                     boolean fullMarket,
                                     @Nonnull Consumer<StatSnapshot.Builder> handleSnapshot) {
        // Process all the DB records grouped by, and ordered by, snapshot_time
        TreeMap<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity =
                organizeStatsRecordsByTime(statDBRecords);

        // For each snapshot_time, create a {@link StatSnapshot} and handle as it is constructed
        statRecordsByTimeByCommodity.forEach((timestamp, commodityMap) -> {
            StatSnapshot.Builder statSnapshotBuilder = StatSnapshot.newBuilder();
            statSnapshotBuilder.setSnapshotDate(DateTimeUtil.toString(timestamp.getTime()));

            // process all the stats records for a given commodity for the curren snapshot_time
            // - might be 1, many for group, or none if time range didn't overlap recorded stats
            commodityMap.asMap().forEach((commodityName, dbStatRecordList) -> {
                if (!dbStatRecordList.isEmpty()) {
                    // use the first element as the core of the group value
                    Record dbFirstStatRecord = dbStatRecordList.iterator().next();
                    final String propertyType = dbFirstStatRecord.getValue(PROPERTY_TYPE, String.class);
                    String propertySubtype = dbFirstStatRecord.getValue(PROPERTY_SUBTYPE, String.class);
                    Float capacity = dbFirstStatRecord.getValue(CAPACITY, Float.class);

                    // In the full-market requests the "relation" column (in market_stats_* tables)
                    // is an integer that gets mapped to a value.
                    //
                    // TODO (roman, Feb 23, 2017): This is getting out of hand, the
                    // StatsHistoryService shouldn't be handling the various types of database
                    // records, and this is in need of refactoring.
                    String relation;
                    if (fullMarket) {
                        final RelationType relationType =
                                dbFirstStatRecord.getValue(RELATION, RelationType.class);
                        relation = relationType == null ? null : relationType.getLiteral();
                    } else {
                        relation = dbFirstStatRecord.getValue(RELATION, String.class);
                    }

                    // In the full-market request we return aggregate stats with no commodity_key
                    // or producer_uuid.
                    final String commodityKey = fullMarket ? null :
                            dbFirstStatRecord.getValue(COMMODITY_KEY, String.class);
                    final String producerIdString = fullMarket ? null :
                            dbFirstStatRecord.getValue(PRODUCER_UUID, String.class);
                    Long producerId = null;
                    if (StringUtils.isNotEmpty(producerIdString)) {
                        producerId = Long.valueOf(producerIdString);
                    }
                    Float avgValue;
                    Float minValue;
                    Float maxValue;
                    Float totalValue = 0.0f;

                    final int numStatRecords = dbStatRecordList.size();
                    if (numStatRecords > 1) {
                        // need to average the value over the stats records
                        float avgTotal = 0.0f;
                        float minTotal = 0.0f;
                        float maxTotal = 0.0f;
                        // calculate averages
                        for (Record dbStatRecord : dbStatRecordList) {
                            Float oneAvgValue = dbStatRecord.getValue(AVG_VALUE, Float.class);
                            if (oneAvgValue != null) {
                                avgTotal += oneAvgValue;
                            }
                            Float oneMinValue = dbStatRecord.getValue(MIN_VALUE, Float.class);
                            if (oneMinValue != null) {
                                minTotal += oneMinValue;
                            }
                            Float oneMaxValue = dbStatRecord.getValue(MAX_VALUE, Float.class);
                            if (oneMaxValue != null) {
                                maxTotal += oneMaxValue;
                            }
                            if (oneAvgValue != null) {
                                totalValue += oneAvgValue;
                            }
                        }
                        avgValue = avgTotal / numStatRecords;
                        minValue = minTotal / numStatRecords;
                        maxValue = maxTotal / numStatRecords;
                    } else {
                        // single value - no need to average
                        avgValue = dbFirstStatRecord.getValue(AVG_VALUE, Float.class);
                        minValue = dbFirstStatRecord.getValue(MIN_VALUE, Float.class);
                        maxValue = dbFirstStatRecord.getValue(MAX_VALUE, Float.class);
                        if (avgValue != null) {
                            totalValue = maxValue;
                        }
                    }

                    // build the record for this stat (commodity type)
                    final StatRecord statRecord = buildStatRecord(propertyType, propertySubtype, capacity,
                            producerId, avgValue, minValue, maxValue, commodityKey, totalValue, relation);

                    // return add this record to the snapshot for this timestamp
                    statSnapshotBuilder.addStatRecords(statRecord);
                }
            });
            // process the snapshot for this time_stamp
            handleSnapshot.accept(statSnapshotBuilder);
        });
    }

    /**
     * Process a list of DB stats reecords and organize into a {@link TreeMap} ordered by Timestamp
     * and then commodity.
     * <p>
     * Note that properties bought and sold may have the same name, so we need to distinguish
     * those by appending PROPERTY_TYPE with PROPERTY_SUBTYPE for the key for the commodity map.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @return a map from each unique Timestamp to the map of properties to DB stats records for
     * that property and timestamp
     */
    private TreeMap<Timestamp, Multimap<String, Record>> organizeStatsRecordsByTime(List<Record> statDBRecords) {
        // organize the statRecords by SNAPSHOT_TIME and then by PROPERTY_TYPE + PROPERTY_SUBTYPE
        TreeMap<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity = new TreeMap<>();
        for (Record dbStatRecord : statDBRecords) {
            Timestamp snapshotTime = dbStatRecord.getValue(SNAPSHOT_TIME, Timestamp.class);
            Multimap<String, Record> snapshotMap = statRecordsByTimeByCommodity.computeIfAbsent(
                    snapshotTime, k -> HashMultimap.create()
            );
            String commodityName = dbStatRecord.getValue(PROPERTY_TYPE, String.class);
            String propertySubType = dbStatRecord.getValue(PROPERTY_SUBTYPE, String.class);
            // See the enum RelationType in com.vmturbo.reports.db.
            // Commodities, CommoditiesBought, and CommoditiesFromAttributes
            // (e.g., priceIndex, numVCPUs, etc.)
            String relation = dbStatRecord.getValue(RELATION, String.class);

            // Need to separate commodity bought and sold as some commodities are both bought
            // and sold in the same entity, e.g., StorageAccess in Storage entity.
            String recordKey = commodityName + relation;

            // Filter out the utilization as we are interested in the used values
            if (!UTILIZATION.equals(propertySubType)) {
                snapshotMap.put(recordKey, dbStatRecord);
            }
        }
        return statRecordsByTimeByCommodity;
    }


    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs "utilization"
     * @param capacity available amount on the producer
     * @param producerId unique id of the producer for commodity bought
     * @param avgValue average value reported from discovery
     * @param minValue min value reported from discovery
     * @param maxValue max value reported from discovery
     * @param commodityKey unique key to associate commodities between seller and buyer
     * @param totalValue total of value (avgValue) over all elements of a group
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    private StatRecord buildStatRecord(String propertyType,
                                       String propertySubtype,
                                       Float capacity,
                                       Long producerId,
                                       Float avgValue,
                                       Float minValue,
                                       Float maxValue,
                                       String commodityKey,
                                       Float totalValue,
                                       String relation) {

        StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                .setName(propertyType);

        if (capacity != null) {
            statRecordBuilder.setCapacity(capacity);
        }

        if (relation != null) {
            statRecordBuilder.setRelation(relation);
        }

        // reserved ??
        if (commodityKey != null) {
            statRecordBuilder.setStatKey(commodityKey);
        }
        if (producerId != null) {
            // providerUuid
            statRecordBuilder.setProviderUuid(Long.toString(producerId));
            // providerDisplayName
            final String producerDisplayName = liveStatsReader.getEntityDisplayNameForId(producerId);
            if (producerDisplayName != null) {
                statRecordBuilder.setProviderDisplayName(producerDisplayName);
            }
        }

        // units
        CommodityTypes commodityType = CommodityTypes.fromString(propertyType);
        if (commodityType != null) {
            statRecordBuilder.setUnits(commodityType.getUnits());
        }

        // values, used, peak
        StatRecord.StatValue.Builder statValueBuilder = StatRecord.StatValue.newBuilder();
        if (avgValue != null) {
            statValueBuilder.setAvg(avgValue);
        }
        if (minValue != null) {
            statValueBuilder.setMin(minValue);
        }
        if (maxValue != null) {
            statValueBuilder.setMax(maxValue);
        }
        if (totalValue != null) {
            statValueBuilder.setTotal(totalValue);
        }

        // currentValue
        if (avgValue != null && (propertySubtype == null ||
                StringConstants.PROPERTY_SUBTYPE_USED.equals(propertySubtype))) {
            statRecordBuilder.setCurrentValue(avgValue);
        } else {
            if (maxValue != null) {
                statRecordBuilder.setCurrentValue(maxValue);
            }
        }

        StatRecord.StatValue statValue = statValueBuilder.build();

        statRecordBuilder.setValues(statValue);
        statRecordBuilder.setUsed(statValue);
        statRecordBuilder.setPeak(statValue);

        return statRecordBuilder.build();
    }

    /**
     * Delete stats associated with a plan.
     *
     * @param request PlanId/TopologyContextId which indentifies the plan
     * @param responseObserver channel for sending the response {@link DeletePlanStatsResponse}
     *
     */
    @Override
    public void deletePlanStats(DeletePlanStatsRequest request,
                               StreamObserver<DeletePlanStatsResponse> responseObserver) {
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter topologyContextId.")
                .asException());
            return;
        }

        try {
            final DeletePlanStatsResponse.Builder responseBuilder = DeletePlanStatsResponse.newBuilder();
            historydbIO.deletePlanStats(request.getTopologyContextId());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL.withDescription("Error deleting plan stats with id: " + request.getTopologyContextId())
                    .withCause(e)
                    .asException());
        }
    }
}
