package com.vmturbo.history.utils;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The class SystemLoadHelper implements some data structures and methods to keep information
 * and make calculations regarding the system load of the system.
 */
public class SystemLoadHelper {
    // TODO unify: Any 7.21 changes needed?

    private static final Logger logger = LogManager.getLogger();
    private static final String USED = PropertySubType.Used.getApiParameterName();

    private SystemLoadReader systemLoadReader = null;

    private final HistorydbIO historydbIO;

    private final SetOnce<Map<String, Pair<Double, Date>>> dbSystemLoadCache = new SetOnce<>();

    /**
     * Create a new instance.
     * @param systemLoadReader system load reader
     * @param historydbIO      db methods
     */
    public SystemLoadHelper(SystemLoadReader systemLoadReader, HistorydbIO historydbIO) {
        systemLoadReader.setSystemLoadUtils(this);
        setSystemLoadReader(systemLoadReader);
        this.historydbIO = historydbIO;
    }

    public void setSystemLoadReader(SystemLoadReader systemLoadReader) {
        this.systemLoadReader = systemLoadReader;
    }

    private Map<String, Grouping> slice2groups = null;

    public void setSliceToGroups(Map<String, Grouping> slice2groups) {
        this.slice2groups = slice2groups;
    }

    /**
     * The method finds the slices (host clusters) for a specific entity. It may be more
     * one in case of storage entities shared to more than one hosts.
     *
     * @param entity The entity which sells the commodity.
     * @return The slices where the entity which sells the commodity belongs to.
     */
     @Nonnull public List<String> getSlices(TopologyEntityDTO entity) {
        List<String> slices = new ArrayList<>();
        List<Long> entities_oids = new ArrayList<>();

        // Finding the host(s) related to the entity.
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE.ordinal()) {
            List<CommoditiesBoughtFromProvider> commoditiesBought = entity.getCommoditiesBoughtFromProvidersList();
            for (CommoditiesBoughtFromProvider commBought : commoditiesBought) {
                if (commBought.getProviderEntityType() == EntityType.PHYSICAL_MACHINE.ordinal()) {
                    entities_oids.add(commBought.getProviderId());
                    break;
                }
            }
        } else if (entity.getEntityType() == EntityType.PHYSICAL_MACHINE.ordinal()) {
            entities_oids.add(entity.getOid());
        } else if (entity.getEntityType() == EntityType.STORAGE.ordinal()) {
            List<CommoditySoldDTO> commoditiesSold = entity.getCommoditySoldListList();
            for (CommoditySoldDTO commSold : commoditiesSold) {
                if (commSold.getCommodityType().getType() == CommodityType.DSPM_ACCESS.ordinal()) {
                    entities_oids.add(commSold.getAccesses());
                }
            }
        }

        // Finding the slice(s) where the host(s) belong to.
        if (entities_oids.size() == 0) {
            return slices;
        } else {
            for (Map.Entry<String, Grouping> entry : slice2groups.entrySet()) {
                String slice = entry.getKey();
                Grouping group = entry.getValue();
                List<Long> hosts = GroupProtoUtil.getStaticMembers(group);
                for (long host : hosts) {
                    for (long entity_oid : entities_oids) {
                        if (host == entity_oid) {
                            if (!slices.contains(slice)) {
                                slices.add(slice);
                            }
                        }
                    }
                }
            }
            return slices;
        }
    }

    /**
     * The method updates the input map with the sums of the capacities of the commodities
     * sold by hosts and storages.
     *
     * @param perSliceCapacities The map of commodities capacities per slice.
     */
    public void updateCapacities(Map<String, Double[]> perSliceCapacities) {
        for (String slice : perSliceCapacities.keySet()) {
            Double[] capacities = perSliceCapacities.get(slice);
            capacities[SystemLoadCommodities.CPU.ordinal()] = HostsSumCapacities.getCpu().get(slice);
            capacities[SystemLoadCommodities.MEM.ordinal()] = HostsSumCapacities.getMem().get(slice);
            capacities[SystemLoadCommodities.IO_THROUGHPUT.ordinal()] = HostsSumCapacities.getIoThroughput().get(slice);
            capacities[SystemLoadCommodities.NET_THROUGHPUT.ordinal()] = HostsSumCapacities.getNetThroughput().get(slice);
            capacities[SystemLoadCommodities.CPU_PROVISIONED.ordinal()] = HostsSumCapacities.getCpuProvisioned().get(slice);
            capacities[SystemLoadCommodities.MEM_PROVISIONED.ordinal()] = HostsSumCapacities.getMemProvisioned().get(slice);
            capacities[SystemLoadCommodities.STORAGE_ACCESS.ordinal()] = StoragesSumCapacities.getStorageAccess().get(slice);
            capacities[SystemLoadCommodities.STORAGE_PROVISIONED.ordinal()] = StoragesSumCapacities.getStorageProvisioned().get(slice);
            capacities[SystemLoadCommodities.STORAGE_AMOUNT.ordinal()] = StoragesSumCapacities.getStorageAmount().get(slice);
            perSliceCapacities.put(slice, capacities);
        }
    }

    /**
     * The method updates the system load information for all slices in the DB when needed.
     *
     * @param loader bulk loader for system_load table
     * @param perSliceUsed The sum of the used valued per slice and per system load commodity.
     * @param perSliceCapacities The sum of capacities per slice and per system load commodity.
     * @param updatedSoldCommMultimap The sold system load commodities per slice.
     * @param updatedBoughtCommMultimap The bought system load commodities per slice.
     */
    public void updateSystemLoad(BulkLoader<SystemLoadRecord> loader,
                                 final Map<String, Double[]> perSliceUsed,
                                 final Map<String, Double[]> perSliceCapacities,
                                 final Multimap<String, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommMultimap,
                                 final Multimap<String, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommMultimap) {
        final long currTimeMillis = System.currentTimeMillis();
        try {
            // Check if update is necessary per slice
            for (String slice : perSliceUsed.keySet()) {
                Double[] sliceUsed = perSliceUsed.get(slice);
                Double[] sliceCapacities = perSliceCapacities.get(slice);

                updateSystemLoad(loader, slice, sliceUsed, sliceCapacities, currTimeMillis,
                    updatedSoldCommMultimap.get(slice), updatedBoughtCommMultimap.get(slice));
            }
        } catch (Exception e) {
            logger.error("Exception when updating system load", e);
        }
    }

    /**
     * The method updates the system load information for one slice in the DB when needed.
     *
     * @param loader bulk loader for system_load table
     * @param slice The slice we want to update.
     * @param sliceUsed The sum of the used valued in this slice per system load commodity.
     * @param sliceCapacities The sum of capacities in this slice per system load commodity.
     * @param snapshotTime The time of the update.
     * @param soldCollection The sold system load commodities in this slice.
     * @param boughtCollection The bought system load commodities in this slice.
     * @throws InterruptedException if interrupted
     */
    private void updateSystemLoad(BulkLoader<SystemLoadRecord> loader,
                                  String slice,
                                  Double[] sliceUsed,
                                  Double[] sliceCapacities,
                                  long snapshotTime,
                                  Collection<Pair<CommoditySoldDTO, TopologyEntityDTO>> soldCollection,
                                  Collection<Pair<CommodityBoughtDTO, TopologyEntityDTO>> boughtCollection)
        throws InterruptedException {

        boolean shouldWriteToDb = true;
        Double currentLoad = null;
        Double previousLoad = null;
        Date currentDate = null;
        Date previousDate = null;
        Map<String, Pair<Double, Date>> cachedSystemLoad = dbSystemLoadCache.ensureSet(() -> systemLoadReader.initSystemLoad());
        try {
            currentLoad = calcSystemLoad(sliceUsed, sliceCapacities, slice);

            if (currentLoad == null) {
                logger.warn("The calculated system load for " + slice
                        + " is null since the providing sums are invalid.");
                return;
            }

            currentDate = new Date(snapshotTime);

            if (cachedSystemLoad.containsKey(slice)) {
                previousLoad = cachedSystemLoad.get(slice).first;
                previousDate = cachedSystemLoad.get(slice).second;
            }

            shouldWriteToDb = true;

            if (currentDate != null && previousDate != null && DateUtils.isSameDay(currentDate, previousDate)) {
                if (previousLoad < currentLoad) {
                    cachedSystemLoad.put(slice, new Pair<>(currentLoad, currentDate));
                    deleteSystemLoadFromDb(slice, snapshotTime);
                } else {
                    shouldWriteToDb = false;
                }
            } else {
                cachedSystemLoad.put(slice, new Pair<>(currentLoad, currentDate));
            }
        } catch (Exception e) {
            logger.error(
                    "Error in updateSystemLoad() for cluster {}, currentLoad = {}, "
                    + "previousLoad = {}, currentDate = {}, previousDate = {} : {}",
                    slice, currentLoad, previousLoad, currentDate, previousDate, e);
        }

        // Writing the current system load to the DB
        if (shouldWriteToDb) {
            logger.debug("Writing system load to DB for cluster {} at {} : {}",
                    slice, currentDate.toString(), currentLoad);
            try {
                writeSystemLoadToDb(loader, slice, sliceUsed, sliceCapacities, snapshotTime,
                    soldCollection, boughtCollection);
            } catch (VmtDbException e) {
                logger.error("Error when writing to DB for cluster {} : {}", slice, e);
            }
        }

    }

    /**
     * The method calculates the system load for the given values.
     *
     * @param usedSums The sums of the used values per system load commodity.
     * @param capacitiesSums The sums of capacities per system load commodity.
     * @param clusterId id of cluster to calculate system load for.
     * @return The system load value.
     */
    public Double calcSystemLoad(Double[] usedSums, Double[] capacitiesSums, String clusterId) {
        checkArgument(usedSums != null && usedSums.length > 0,
                "Invalid used sum values when calculating system load");
        checkArgument(capacitiesSums != null && capacitiesSums.length > 0,
                "Invalid capacitites sum values when calculating system load");

        logger.debug("SYSLOAD- Capacities are: {}", Arrays.deepToString(capacitiesSums));
        logger.debug("SYSLOAD- Used are: {}", Arrays.deepToString(usedSums));

        Set<Integer> commodityIndexWithErrors = new HashSet<Integer>();
        // If the input sums are invalid, set the value 0.0
        for (int i = 0; i < SystemLoadCommodities.SIZE; i++) {
            if (usedSums[i] == null) {
                commodityIndexWithErrors.add(i);
                usedSums[i] = 0.0;
            }

            if (capacitiesSums[i] == null) {
                commodityIndexWithErrors.add(i);
                capacitiesSums[i] = 0.0;
            }
        }

        if (!CollectionUtils.isEmpty(commodityIndexWithErrors)) {
            StringBuilder errorMessage = new StringBuilder("For cluster id : " + clusterId
                    + " commodities with null used/capacity values are : ");
            commodityIndexWithErrors.stream()
                .forEach(index -> errorMessage.append(SystemLoadCommodities.get(index).toString())
                        .append(" "));
            logger.warn(errorMessage);
        }

        Double[] sysUtils = new Double[SystemLoadCommodities.SIZE];
        Double maxUtil = 0.0;

        // Calculate system wide utilization values per commodity
        for  (int i = 0; i < SystemLoadCommodities.SIZE; i++) {
            if (capacitiesSums[i] == 0) {
                sysUtils[i] = 0.0;
            } else {
                // Total demand over total capacity
                sysUtils[i] = usedSums[i] / capacitiesSums[i];
            }
            maxUtil = Math.max(maxUtil, sysUtils[i]);
        }

        return maxUtil;
    }

    /**
     * The method stores the system load information for a specific slice to the DB.
     *
     * @param loader bulk loader for system_load table
     * @param slice The slice we want to update.
     * @param sliceUsed used values for slices
     * @param sliceCapacities capacities for slices
     * @param snapshotTime snapshot timestmp
     * @param soldCollection sold commodities
     * @param boughtCollection bought commodities
     * @throws VmtDbException if a db operation fails
     * @throws InterruptedException if interrupted
     */
    public void writeSystemLoadToDb(
        BulkLoader<SystemLoadRecord> loader,
        String slice,
        Double[] sliceUsed,
        Double[] sliceCapacities,
        long snapshotTime,
        Collection<Pair<CommoditySoldDTO, TopologyEntityDTO>> soldCollection,
        Collection<Pair<CommodityBoughtDTO, TopologyEntityDTO>> boughtCollection)
        throws VmtDbException, InterruptedException {

        //Send the metrics out to be written to the DB:
        Date date = new Date(snapshotTime);

        for (Pair<CommoditySoldDTO, TopologyEntityDTO> comm : soldCollection) {
            final String uuid = Long.toString(comm.second.getOid());
            final String propertyType = SystemLoadCommodities.toSystemLoadCommodity(comm.first.getCommodityType().getType()).toString();

            final SystemLoadRecord record = SystemLoadDbUtil.createSystemLoadRecord(slice,
                            new Timestamp(date.getTime()), uuid, null, propertyType, USED,
                            comm.first.getCapacity(), comm.first.getUsed(), comm.first.getUsed(),
                            comm.first.getPeak(), RelationType.COMMODITIES,
                            comm.first.getCommodityType().getKey());
            loader.insert(record);
        }

        for (Pair<CommodityBoughtDTO, TopologyEntityDTO> comm : boughtCollection) {
            final String uuid = Long.toString(comm.second.getOid());
            final String producerUuid = Long.toString(comm.second.getCommoditiesBoughtFromProviders(0).getProviderId());
            final String propertyType = SystemLoadCommodities.toSystemLoadCommodity(comm.first.getCommodityType().getType()).toString();

            final SystemLoadRecord record = SystemLoadDbUtil.createSystemLoadRecord(slice,
                            new Timestamp(date.getTime()), uuid, producerUuid, propertyType, USED,
                            null, comm.first.getUsed(), comm.first.getUsed(), comm.first.getPeak(),
                            RelationType.COMMODITIESBOUGHT, comm.first.getCommodityType().getKey());
            loader.insert(record);
        }

        for (int i = 0; i < SystemLoadCommodities.SIZE; i++) {
            double used = sliceUsed[i] != null ? sliceUsed[i] : 0.0;
            double capacity = sliceCapacities[i] != null ? sliceCapacities[i] : 0.0;
            final SystemLoadRecord record = SystemLoadDbUtil.createSystemLoadRecord(
                slice, new Timestamp(date.getTime()), null, null, "system_load",
                SystemLoadCommodities.get(i).toString(), capacity, used, used, used,
                RelationType.COMMODITIES, null);
            loader.insert(record);
        }
    }

    /**
     * The method deleted all the system load related records for a specific slice and the
     * current day.
     *
     * @param slice The slice we want to delete.
     * @param snapshot The current snapshot of the system.
     */
    public void deleteSystemLoadFromDb(String slice, long snapshot) {
        try {
            SystemLoadDbUtil.deleteSystemLoadRecords(slice, snapshot, historydbIO);
        } catch (VmtDbException e) {
            logger.error("Error when deleting system load form DB for cluster {} : {}", slice, e);
        } catch (ParseException e) {
            logger.error("Error when converting snapshot to current date for cluster {} : {}", slice, e);
        }
    }

}
