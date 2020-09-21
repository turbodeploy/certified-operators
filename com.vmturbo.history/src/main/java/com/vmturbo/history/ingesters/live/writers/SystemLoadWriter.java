package com.vmturbo.history.ingesters.live.writers;

import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import gnu.trove.TCollections;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Update system load data based on the content of a new live topology.
 */
public class SystemLoadWriter extends TopologyWriterBase {
    private static final long TROVE_MISSING_LONG = -1L;
    private static final TLongSet EMPTY_LONG_SET = TCollections.unmodifiableSet(new TLongHashSet());
    private static Logger logger = LogManager.getLogger();

    private static final int SYSTEM_LOAD_COMMODITIES_COUNT = SystemLoadCommodity.values().length;

    /** main loader for the system_load table. */
    private final BulkLoader<SystemLoadRecord> loader;
    /**
     * loader for a transient table patterned after system load, where some records are
     * written before we know we'll be keeping them.
     */
    private final BulkLoader<SystemLoadRecord> transientLoader;
    private final BasedbIO basedbIO;
    private final Timestamp snapshotTime;
    private final Timestamp startOfDay;
    private final Timestamp endOfDay;
    /**
     * The slice that each PM encountered in the topology belongs to, if any.
     *
     * <p>At present every slice is just a compute-cluster, but other types of slice may be added
     * in the future.</p>
     */
    private final TLongLongMap hostToSliceMap;
    /** all the slices. */
    private final TLongSet sliceSet;
    private final SystemLoadReader systemLoadReader;

    /**
     * Capacities for system load commodities, aggregated across all PMs and STORAGES in each slice.
     *
     * <p>The map relates slice id (i.e. compute cluster id, at least for now) to per-commodity-type
     * aggregataed values for that slice, represented as arrays of doubles.. The commodity types
     * of interest are the members of the {@link SystemLoadCommodity} enum, and the ordinals of the
     * enum members are used to index into the arrays.</p>
     */
    private TLongObjectMap<double[]> sliceCapacities = new TLongObjectHashMap<>(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, TROVE_MISSING_LONG);

    /**
     *  Usages for system load commodities, aggregated VMs in each slice.
     *
     *  <p>Form is identical to {@link #sliceCapacities} above.</p>
     */
    private TLongObjectMap<double[]> sliceUsages = new TLongObjectHashMap<>(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, TROVE_MISSING_LONG);

    /**
     * Create a new instance.
     *
     * @param groupService     group service endpoint
     * @param basedbIO         access to DB stuff
     * @param systemLoadReader for reading prior system load values
     * @param loaders          for writing records to tables
     * @param info             info about the topology being processed
     * @throws SQLException           if there's a database exception
     * @throws InstantiationException if we can't create a new transient table
     * @throws VmtDbException         if there's a problem getting a DB connection
     * @throws IllegalAccessException if we can't create a transient table
     */
    SystemLoadWriter(GroupServiceBlockingStub groupService,
            BasedbIO basedbIO,
            SystemLoadReader systemLoadReader,
            SimpleBulkLoaderFactory loaders,
            TopologyInfo info) throws SQLException, InstantiationException, VmtDbException, IllegalAccessException {
        this.hostToSliceMap = loadClusterInfo(groupService);
        this.sliceSet = new TLongHashSet(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, TROVE_MISSING_LONG);
        this.sliceSet.addAll(hostToSliceMap.valueCollection());
        this.basedbIO = basedbIO;
        this.systemLoadReader = systemLoadReader;
        this.loader = loaders.getLoader(SYSTEM_LOAD);
        try {
            this.transientLoader = loaders.getTransientLoader(SYSTEM_LOAD, table -> {
                try (Connection conn = basedbIO.connection()) {
                    basedbIO.using(conn)
                            .createIndex("slice")
                            .on(table, SYSTEM_LOAD.SLICE)
                            .execute();
                }
            });
        } catch (IllegalAccessException | SQLException | InstantiationException | VmtDbException e) {
            logger.error("Failed to instantiate transient table based on {}; " +
                    "cannot produce system load data", SYSTEM_LOAD.getName(), e);
            throw e;
        }
        Instant snapshotTime = Instant.ofEpochMilli(info.getCreationTime());
        this.snapshotTime = Timestamp.from(snapshotTime);
        this.startOfDay = Timestamp.from(snapshotTime.truncatedTo(ChronoUnit.DAYS));
        this.endOfDay = Timestamp.from(snapshotTime.truncatedTo(ChronoUnit.DAYS)
                .plus(1, ChronoUnit.DAYS)
                .minus(1, ChronoUnit.MILLIS));

    }

    /**
     * Load all the cluster membership info from the group service.
     *
     * @param groupService group service endpoint
     * @return map of host OID -> cluster id
     */
    private TLongLongMap loadClusterInfo(GroupServiceBlockingStub groupService) {
        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                        .build())
                .build();
        final Iterator<Grouping> groupIterator = groupService.getGroups(groupsRequest);
        TLongLongMap result = new TLongLongHashMap(
                DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, TROVE_MISSING_LONG, TROVE_MISSING_LONG);
        while (groupIterator.hasNext()) {
            Grouping group = groupIterator.next();
            final long clusterId = group.getId();
            GroupProtoUtil.getAllStaticMembers(group.getDefinition())
                    .forEach(hostId -> result.put(hostId, clusterId));
        }
        return result;
    }

    /**
     * As we process each chunk we write all VM records that may need to be saved into our transient
     * table.
     *
     * <p>We won't know until we finish the topology which slices need to be replaced in the
     * database, so that's done in {@link #finish(int, boolean, String)} method. Writing these
     * to the database means we spread our database IO out rather than doing it all at the end,
     * and it also means we don't need to keep large amounts of state data in memory the whole
     * time.</p>
     *
     * @param chunk       a raw chunk from the topology, which may contain a mixture of extension
     *                    and entity items.
     * @param infoSummary summary of topology
     * @return whether this chunk succeeded, and whether we want more
     * @throws InterruptedException if interrupted
     */
    @Override
    protected ChunkDisposition processEntities(
            @Nonnull final Collection<TopologyEntityDTO> chunk,
            @Nonnull final String infoSummary) throws InterruptedException {
        for (final TopologyEntityDTO entity : chunk) {
            switch (entity.getEntityType()) {
                // PM and STORAGE are handled identically, namely incorporate their sold commodity
                // capacities into per-cluster aggregates
                case EntityType.PHYSICAL_MACHINE_VALUE:
                case EntityType.STORAGE_VALUE:
                    recordSoldCapacityForSlices(entity, getSlicesForEntity(entity));
                    break;
                case EntityType.VIRTUAL_MACHINE_VALUE: {
                    final TLongSet slices = getSlicesForEntity(entity);
                    recordBoughtUsageForSlices(entity, slices);
                    writeCommodities(entity, slices);
                    break;
                }
                default:
                    // we're not interested in any other entities
                    break;
            }
        }
        return ChunkDisposition.SUCCESS;
    }

    /**
     * Here's where we decide which slices need to be updated in the database, and write their
     * data.
     *
     * <p>Since the VM records are already in the transient data, "writing" them really just
     * means performing an INSERT-SELECT to copy them to the main table after deleting the
     * records they are to replace. Besides that, we write the cluster-wide utilization records
     * and a single record per cluster that records the overall system-load value calculated for
     * the slice.</p>
     *
     * @param objectCount number of objects processed
     * @param expedite    true if this thread has already been interrupted,
     *                    and noncritical processing should be avoided
     * @param infoSummary summary of broadcast info, for logging
     * @throws InterruptedException if interrupted
     */
    @Override
    public void finish(final int objectCount, final boolean expedite, final String infoSummary)
            throws InterruptedException {
        // make sure all system-load inserts have completed before we wrap up
        loader.flush(true);
        transientLoader.flush(true);
        final Map<Long, Double> priorSystemLoads = getPriorSystemLoads();
        for (long slice : sliceSet.toArray()) {
            // get all the existing system-load values, if any, for today
            Optional<Double> priorSystemLoad = Optional.ofNullable(priorSystemLoads.get(slice));
            double systemLoad = calculateSystemLoad(slice);
            if (priorSystemLoad.map(prior -> prior < systemLoad).orElse(true)) {
                // newly calculated value is today's new high, so... out with the old,
                // in with the new
                logger.info("Updating system load with new daily high for slice {}", slice);
                writeSystemLoadData(slice, systemLoad);
            }
        }
    }

    /**
     * Here we figure out which slices an entity of interest belongs to.
     *
     * <p>Only storage entities can currently belong to more than one slice.</p>
     *
     * @param entity the entity
     * @return the slices it belongs to
     */
    private TLongSet getSlicesForEntity(TopologyEntityDTO entity) {
        // start by figuring out which host(s) this entity relates to for system-load calculation
        Set<Long> relatedHosts;
        switch (entity.getEntityType()) {
            case EntityType.PHYSICAL_MACHINE_VALUE:
                // a host is related only to itself
                relatedHosts = Collections.singleton(entity.getOid());
                break;
            case EntityType.VIRTUAL_MACHINE_VALUE:
                // a VM is related to every host it buys (anything) from. It'd be a neat trick
                // for a VM to buy from multiple hosts, so this is a singleton
                relatedHosts = entity.getCommoditiesBoughtFromProvidersList().stream()
                        // look for hosts that sell any system-load commodities to this VM
                        .filter(fromProvder ->
                                fromProvder.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                        .filter(fromProvider -> fromProvider.getCommodityBoughtList().stream()
                                // check that at least one commodity from this provider is a
                                // system-load commodity
                                .map(comm -> SystemLoadCommodity.fromSdkCommodityType(
                                        comm.getCommodityType().getType()))
                                .anyMatch(Optional::isPresent))
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .collect(Collectors.toSet());
                break;
            case EntityType.STORAGE_VALUE:
                // a storage is related to every host to which it sells DSPM access - could be
                // multiple
                relatedHosts = entity.getCommoditySoldListList().stream()
                        // find hosts that buy DSPM access form this storage
                        .filter(comm -> comm.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE)
                        .map(CommoditySoldDTO::getAccesses)
                        .collect(Collectors.toSet());
                break;
            default:
                return EMPTY_LONG_SET;
        }
        // now collect all the clusters that contain any of the related hotsts
        return new TLongHashSet(relatedHosts.stream()
                .map(hostToSliceMap::get)
                .filter(slice -> slice != TROVE_MISSING_LONG)
                .collect(Collectors.toSet()));
    }

    /**
     * Accumulate capacities for commodities sold by this entity for its slices.
     *
     * @param entity entity to record
     * @param slices slices to record it for
     */
    private void recordSoldCapacityForSlices(TopologyEntityDTO entity, TLongSet slices) {
        for (final CommoditySoldDTO soldCommodity : entity.getCommoditySoldListList()) {
            SystemLoadCommodity.fromSdkCommodityType(soldCommodity.getCommodityType().getType())
                    .ifPresent(slType -> recordSoldCapacityForSlices(soldCommodity, slType, slices));
        }
    }

    /**
     * Accumulate the capacity of a single sold commodity into the aggregated capacities for the
     * given slices.
     *
     * @param soldCommodity the sold commodity
     * @param slType        the {@link SystemLoadCommodity} type
     * @param slices        the slices to aggregate for
     */
    private void recordSoldCapacityForSlices(
            CommoditySoldDTO soldCommodity, SystemLoadCommodity slType, TLongSet slices) {
        recordValueForSlices(soldCommodity.getCapacity(), slType.ordinal(), sliceCapacities, slices);
    }

    /**
     * Accumulate usage values for commodities bought by this entity for its slices.
     *
     * @param entity the entity to record
     * @param slices the slices to record it for
     */
    private void recordBoughtUsageForSlices(TopologyEntityDTO entity, TLongSet slices) {
        for (final CommoditiesBoughtFromProvider boughtFromProvider
                : entity.getCommoditiesBoughtFromProvidersList()) {
            for (final CommodityBoughtDTO boughtCommodity : boughtFromProvider.getCommodityBoughtList()) {
                SystemLoadCommodity.fromSdkCommodityType(boughtCommodity.getCommodityType().getType())
                        .ifPresent((slType -> recordBoughtUsageForSlices(boughtCommodity, slType, slices)));
            }
        }
    }

    /**
     * Accumulate the capacity of a single sold commodity into the aggregated capacities for the
     * given slices.
     *
     * @param boughtCommodity the commodity to record
     * @param slType          the {@link SystemLoadCommodity} type
     * @param slices          the slices to accumulate for
     */
    private void recordBoughtUsageForSlices(
            CommodityBoughtDTO boughtCommodity, SystemLoadCommodity slType, TLongSet slices) {
        recordValueForSlices(boughtCommodity.getUsed(), slType.ordinal(), sliceUsages, slices);
    }

    /**
     * Write bought and sold commodities for the given (VM) entity to the transient table, for
     * copying into the system load table if the slice's overall system load is a new maximum for
     * the day.
     *
     * @param entity the VM entity
     * @param slices the slices it belongs to
     * @throws InterruptedException if we're interrupted
     */
    private void writeCommodities(TopologyEntityDTO entity, TLongSet slices) throws InterruptedException {
        // write records for all sold system-load commodities
        for (final CommoditySoldDTO soldCommodity : entity.getCommoditySoldListList()) {
            final Optional<SystemLoadCommodity> slType =
                    SystemLoadCommodity.fromSdkCommodityType(soldCommodity.getCommodityType().getType());
            if (slType.isPresent()) {
                writeSoldCommodity(entity, slType.get(), soldCommodity.getCommodityType().getKey(),
                        soldCommodity.getCapacity(), soldCommodity.getUsed(), soldCommodity.getPeak(),
                        slices);
            }
        }
        // record records for all bought system-load commodities from all providerss
        for (final CommoditiesBoughtFromProvider boughtFromProvider :
                entity.getCommoditiesBoughtFromProvidersList()) {
            writeCommodities(entity, boughtFromProvider, slices);

        }
    }

    /**
     * Write commodities bought from a single provider by the given (VM) entity to the transient table,
     * for copying into the system load table if the slice's overall system load is a new maximum for
     * the day.
     *
     * @param entity             the (VM) entity
     * @param boughtFromProvider structure listing all commodities bought from a particular provider
     * @param slices             the slices this VM belongs to
     * @throws InterruptedException if we're interrupted
     */
    private void writeCommodities(TopologyEntityDTO entity,
            CommoditiesBoughtFromProvider boughtFromProvider, TLongSet slices) throws InterruptedException {
        for (final CommodityBoughtDTO boughtCommodity : boughtFromProvider.getCommodityBoughtList()) {
            final Optional<SystemLoadCommodity> slType =
                    SystemLoadCommodity.fromSdkCommodityType(boughtCommodity.getCommodityType().getType());
            if (slType.isPresent()) {
                final Long provider = boughtFromProvider.hasProviderId()
                        ? boughtFromProvider.getProviderId()
                        : null;
                writeBoughtCommodity(entity, slType.get(), boughtCommodity.getCommodityType().getKey(),
                        provider, boughtCommodity.getUsed(), boughtCommodity.getPeak(),
                        slices);
            }
        }
    }

    /**
     * Write the record for a sold (VM) commodity to the transient table.
     *
     * @param entity       the entity
     * @param slType       the {@link SystemLoadCommodity} type
     * @param commodityKey the commodity key
     * @param capacity     the capacity value
     * @param used         the used value
     * @param peak         the peak value
     * @param slices       the slices this entity belongs to
     * @throws InterruptedException if we're interrupted
     */
    private void writeSoldCommodity(final TopologyEntityDTO entity, final SystemLoadCommodity slType,
            final String commodityKey, final double capacity, final double used, final double peak,
            final TLongSet slices) throws InterruptedException {
        for (final long slice : slices.toArray()) {
            transientLoader.insert(createRecord(slice, entity.getOid(), null,
                    slType.name(), StringConstants.USED, commodityKey,
                    capacity, used, peak, RelationType.COMMODITIES));
        }
    }

    /**
     * Write the record for a bought (VM) commodity to the transient table.
     *
     * @param entity       the entity
     * @param slType       the {@link SystemLoadCommodity} type
     * @param commodityKey the commodity key
     * @param producer     the producer id, or null
     * @param used         the used value
     * @param peak         the peak value
     * @param slices       the slices this entity belongs to
     * @throws InterruptedException if we're interrupted
     */
    private void writeBoughtCommodity(final TopologyEntityDTO entity, final SystemLoadCommodity slType,
            final String commodityKey, final Long producer, final double used, final double peak,
            final TLongSet slices) throws InterruptedException {
        for (final long slice : slices.toArray()) {
            transientLoader.insert(createRecord(slice, entity.getOid(), producer,
                    slType.name(), StringConstants.USED, commodityKey,
                    null, used, peak, RelationType.COMMODITIESBOUGHT));
        }
    }

    /**
     * Accumulate the given value into the cluster-wide values for its slices.
     *
     * @param value         the value to be accumulated
     * @param slTypeOrdinal index in value arrays for the {@link SystemLoadCommodity} type
     * @param sliceMap      map of slices to value arrays (capacities or usages)
     * @param slices        slices that should accumulate this value
     */
    private void recordValueForSlices(
            double value, int slTypeOrdinal, TLongObjectMap<double[]> sliceMap, TLongSet slices) {
        for (long slice : slices.toArray()) {
            double[] values = sliceMap.get(slice);
            if (values == null) {
                sliceMap.put(slice, new double[SYSTEM_LOAD_COMMODITIES_COUNT]);
                values = sliceMap.get(slice);
            }
            values[slTypeOrdinal] += value;
        }
    }

    /**
     * Calculate the overall system load for the given slice.
     *
     * <p>This is just the maximum slice-wide utilization among all the system load commodities.</p>
     *
     * @param slice slice to calculate for
     * @return calculated system-load value
     */
    private double calculateSystemLoad(long slice) {
        final double[] usages = sliceUsages.containsKey(slice)
                ? sliceUsages.get(slice) : new double[SYSTEM_LOAD_COMMODITIES_COUNT];
        final double[] capacities = sliceCapacities.containsKey(slice)
                ? sliceCapacities.get(slice) : new double[SYSTEM_LOAD_COMMODITIES_COUNT];
        double maxUtilization = 0.0;
        for (int i = 0; i < SYSTEM_LOAD_COMMODITIES_COUNT; i++) {
            double utilization = capacities[i] == 0.0 ? 0.0 : (usages[i] / capacities[i]);
            maxUtilization = Math.max(utilization, maxUtilization);
        }
        return maxUtilization;
    }

    /**
     * Call out to {@link SystemLoadReader} to retrieve today's highest recorded system load
     * values for all slices.
     *
     * @return map of slice id to today's high value
     */
    private Map<Long, Double> getPriorSystemLoads() {
        return systemLoadReader.getSystemLoadValues(startOfDay, endOfDay);
    }

    /**
     * Write out the new data for the given slice, replacing whatever is currently in the database.
     *
     * @param slice      slice to record
     * @param systemLoad current overall system load value
     * @throws InterruptedException if interrupted
     */
    private void writeSystemLoadData(final long slice, double systemLoad) throws InterruptedException {
        try (Connection conn = basedbIO.transConnection()) {
            // remove all current data for this slice from database
            deleteCurrentRecords(slice, conn);
            // copy VM bought/sold commodity records from the transient data
            copyTransientRecords(slice, conn);
            // create records with aggregated capacity and usage values for all SystemLoadCommodity
            // types for this slice
            if (!sliceCapacities.containsKey(slice)) {
                logger.warn("Did not accumluate any slice capacities for cluster {}; using zeros", slice);
            }
            if (!sliceUsages.containsKey(slice)) {
                logger.warn("Did not accumluate any slice usages for cluster {}; using zeros", slice);
            }
            final double[] capacities = sliceCapacities.containsKey(slice)
                    ? sliceCapacities.get(slice) : new double[SYSTEM_LOAD_COMMODITIES_COUNT];
            final double[] usages = sliceUsages.containsKey(slice)
                    ? sliceUsages.get(slice) : new double[SYSTEM_LOAD_COMMODITIES_COUNT];
            writeUtilizationRecords(capacities, usages, slice);
            // and one last record for the overall system load value for this slice
            writeSystemLoadRecords(slice, systemLoad);
            conn.commit();
        } catch (VmtDbException | SQLException | DataAccessException e) {
            logger.error("Failed to write system log data", e);
        }
    }

    /**
     * Create any system load records for today for the given slice from the database.
     *
     * @param slice slice to remove
     * @param conn  database connection
     * @throws DataAccessException if there's a database error
     */
    private void deleteCurrentRecords(final long slice, Connection conn)
            throws DataAccessException {
        basedbIO.using(conn).deleteFrom(SYSTEM_LOAD)
                .where(SYSTEM_LOAD.SLICE.eq(Long.toString(slice)))
                .and(SYSTEM_LOAD.SNAPSHOT_TIME.between(startOfDay, endOfDay))
                .execute();
    }

    /**
     * Copy records that have been written to the transient table for this slice into the main
     * system_load table.
     *
     * @param slice slice to copy
     * @param conn  database connection
     * @throws DataAccessException if there's a database error
     */
    private void copyTransientRecords(final long slice, Connection conn)
            throws DataAccessException {
        Table<SystemLoadRecord> transientTable = (Table<SystemLoadRecord>)transientLoader.getOutTable();
        final DSLContext dsl = basedbIO.using(conn);
        dsl.insertInto(SYSTEM_LOAD)
                .select(dsl.selectFrom(DSL.table(transientTable.getName()))
                        .where(JooqUtils.getStringField(transientTable, SYSTEM_LOAD.SLICE.getName())
                                .eq(Long.toString(slice))))
                .execute();
    }

    /**
     * Write utilization records for the given slice to the system_load table.
     *
     * @param capacities capacity values for all system-load commodities
     * @param usages     usage values for all system-load commodities
     * @param slice      slice for these values
     * @throws InterruptedException if interrupted
     */
    private void writeUtilizationRecords(double[] capacities, double[] usages, long slice)
            throws InterruptedException {
        for (final SystemLoadCommodity slType : SystemLoadCommodity.values()) {
            loader.insert(createRecord(slice, null, null,
                    StringConstants.SYSTEM_LOAD, slType.name(), null,
                    capacities[slType.ordinal()], usages[slType.ordinal()], usages[slType.ordinal()],
                    RelationType.COMMODITIESBOUGHT));
        }
    }

    /**
     * Write the overall system load for this slice to the system_load table.
     *
     * @param slice      the slice
     * @param systemLoad the calculated system load for the slice
     * @throws InterruptedException if interrupted
     */
    private void writeSystemLoadRecords(long slice, double systemLoad) throws InterruptedException {
        loader.insert(createRecord(slice, null, null,
                StringConstants.SYSTEM_LOAD, StringConstants.SYSTEM_LOAD, null,
                null, systemLoad, systemLoad, RelationType.COMMODITIES));
    }

    /**
     * Create a system load record to be inserted into either the system_load table or the
     * transient table (caller does insertion).
     *
     * @param slice           slice id
     * @param entityId        entity id, or null
     * @param producer        producer id, or null
     * @param propertyType    property type
     * @param propertySubtype property subtype
     * @param commodityKey    commodity key, if any
     * @param capacity        capacity value, or null
     * @param value           used value, or null
     * @param peakValue       peak used value, or null
     * @param relationType    relation type COMMODITIES (sold) or COMMODITIESBOUGHT
     * @return the constructed SystemLoadRecord
     */
    private SystemLoadRecord createRecord(long slice, Long entityId, Long producer,
            String propertyType, String propertySubtype, String commodityKey,
            Double capacity, Double value, Double peakValue,
            RelationType relationType) {
        SystemLoadRecord record = SYSTEM_LOAD.newRecord();
        record.setSlice(Long.toString(slice));
        record.setSnapshotTime(snapshotTime);
        if (entityId != null) {
            record.setUuid(Long.toString(entityId));
        }
        if (producer != null) {
            record.setProducerUuid(producer.toString());
        }
        record.setPropertyType(propertyType);
        record.setPropertySubtype(propertySubtype);
        record.setCommodityKey(commodityKey);
        if (capacity != null) {
            record.setCapacity(capacity);
        }
        if (value != null) {
            record.setAvgValue(value);
            record.setMinValue(value);
        }
        if (peakValue != null) {
            record.setMaxValue(peakValue);
        }
        record.setRelation(relationType);
        return record;
    }

    /**
     * Factory that creates {@link SystemLoadWriter} instances.
     */
    public static class Factory extends TopologyWriterBase.Factory {
        private static Logger logger = LogManager.getLogger();

        private final GroupServiceBlockingStub groupService;
        private final HistorydbIO historydbIO;
        private final SystemLoadReader systemLoadReader;

        /**
         * Create a new factory instance.
         *
         * @param groupService     group service endpoint
         * @param systemLoadReader system load reader instance
         * @param historydbIO      access to history DB helpers
         */
        public Factory(GroupServiceBlockingStub groupService,
                SystemLoadReader systemLoadReader, HistorydbIO historydbIO) {
            this.groupService = groupService;
            this.systemLoadReader = systemLoadReader;
            this.historydbIO = historydbIO;
        }

        @Override
        public Optional<IChunkProcessor<DataSegment>> getChunkProcessor(
                final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory loaders) {
            try {
                return Optional.of(new SystemLoadWriter(
                        groupService, historydbIO, systemLoadReader, loaders, topologyInfo));
            } catch (SQLException | InstantiationException | VmtDbException | IllegalAccessException e) {
                // the non-DB exceptions can happen if the reflective table intance creation required
                // for the transient record loader fails
                logger.error("Failed to instantiate {} instance to process topology",
                        SystemLoadWriter.class.getSimpleName(), e);
                return Optional.empty();
            }
        }
    }
}
