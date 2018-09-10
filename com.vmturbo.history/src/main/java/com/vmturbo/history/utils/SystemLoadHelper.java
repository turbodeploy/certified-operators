package com.vmturbo.history.utils;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.live.SystemLoadWriter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.auth.api.Pair;

/**
 * The class SystemLoadHelper implements some data structures and methods to keep information
 * and make calculations regarding the system load of the system.
 */
public class SystemLoadHelper {

    private static final Logger logger = LogManager.getLogger();

    private static SystemLoadWriter systemLoadWriter = null;
    private static SystemLoadReader systemLoadReader = null;

    private final Object curSystemLoadLock = new Object();

    public Map<String, Pair<Double, Date>> previousSystemLoadInDB = null;

    public SystemLoadHelper(SystemLoadReader systemLoadReader, SystemLoadWriter systemLoadWriter) {
        systemLoadReader.setSystemLoadUtils(this);
        systemLoadWriter.setSystemLoadUtils(this);
        setSystemLoadReader(systemLoadReader);
        setSystemLoadWriter(systemLoadWriter);
        previousSystemLoadInDB = systemLoadReader.initSystemLoad();

    }

    public void setSystemLoadReader(SystemLoadReader systemLoadReader) { this.systemLoadReader = systemLoadReader;}

    public void setSystemLoadWriter(SystemLoadWriter systemLoadWriter) { this   .systemLoadWriter = systemLoadWriter; }

    private Map<String, Group> slice2groups = null;

    public void setSliceToGroups(Map<String, Group> slice2groups) { this.slice2groups = slice2groups; }

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
            for (Map.Entry<String, Group> entry : slice2groups.entrySet()) {
                String slice = entry.getKey();
                Group group = entry.getValue();
                List<Long> hosts = group.getCluster().getMembers().getStaticMemberOidsList();
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
            perSliceCapacities.put(slice, capacities);
        }
    }

    /**
     * The method updates the system load information for all slices in the DB when needed.
     *
     * @param perSliceUsed The sum of the used valued per slice and per system load commodity.
     * @param perSliceCapacities The sum of capacities per slice and per system load commodity.
     * @param updatedSoldCommMultimap The sold system load commodities per slice.
     * @param updatedBoughtCommMultimap The bought system load commodities per slice.
     */
    public void updateSystemLoad(final Map<String, Double[]> perSliceUsed,
                                 final Map<String, Double[]> perSliceCapacities,
                                 final Multimap<String, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommMultimap,
                                 final Multimap<String, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommMultimap) {
        new Thread() {
            @Override
            public void run() {
                final long currTimeMillis = System.currentTimeMillis();
                try {
                    synchronized (curSystemLoadLock) {
                        // Check if update is necessary per slice
                        for (String slice : perSliceUsed.keySet()) {
                            Double[] sliceUsed = perSliceUsed.get(slice);
                            Double[] sliceCapacities = perSliceCapacities.get(slice);

                            updateSystemLoad(slice, sliceUsed, sliceCapacities, currTimeMillis,
                                    updatedSoldCommMultimap.get(slice), updatedBoughtCommMultimap.get(slice));
                        }
                    }
                }
                catch (Exception e) {
                    logger.error("Exception when updating system load:" + e);
                }
            }
        }.start();
    }

    /**
     * The method updates the system load information for one slice in the DB when needed.
     *
     * @param slice The slice we want to update.
     * @param sliceUsed The sum of the used valued in this slice per system load commodity.
     * @param sliceCapacities The sum of capacities in this slice per system load commodity.
     * @param snapshotTime The time of the update.
     * @param soldCollection The sold system load commodities in this slice.
     * @param boughtCollection The bought system load commodities in this slice.
     */
    @GuardedBy("curSystemLoadLock")
    private void updateSystemLoad(String slice, Double[] sliceUsed, Double[] sliceCapacities,
                                  long snapshotTime, Collection<Pair<CommoditySoldDTO, TopologyEntityDTO>> soldCollection,
                                  Collection<Pair<CommodityBoughtDTO, TopologyEntityDTO>> boughtCollection) {
        final Double currentLoad = calcSystemLoad(sliceUsed, sliceCapacities);

        if (currentLoad == null) {
            logger.warn("SYSLOAD- The calculated system load for " + slice
                    + " is null since the providing sums are invalid.");
            return;
        }

        logger.debug("SYSLOAD- Current system load for " + slice + ": " + currentLoad);

        final Date currentDate = new Date(snapshotTime);

        Double previousLoad = null;
        Date previousDate = null;
        if (previousSystemLoadInDB.containsKey(slice)) {
            previousLoad = previousSystemLoadInDB.get(slice).first;
            previousDate = previousSystemLoadInDB.get(slice).second;
        }

        boolean shouldWriteToDb = true;

        if (currentDate != null && previousDate != null && DateUtils.isSameDay(currentDate, previousDate)) {
            if (previousLoad < currentLoad) {
                previousSystemLoadInDB.put(slice, new Pair<>(currentLoad, currentDate));
                deleteSystemLoadFromDb(slice, snapshotTime);
            } else {
                shouldWriteToDb = false;
            }
        } else {
            previousSystemLoadInDB.put(slice, new Pair<>(currentLoad, currentDate));
        }

        // Writing the current system load to the DB
        if (shouldWriteToDb) {
            logger.debug(String.format("SYSLOAD- Writing system load to DB for slice %s at %s : %f",
                    slice, currentDate.toString(), currentLoad));
            try {
                writeSystemLoadToDb(slice, sliceUsed, sliceCapacities, snapshotTime, soldCollection, boughtCollection);
            }
            catch (VmtDbException e) {
                logger.error("SYSLOAD- Error when writing to DB : " + e);
            }
        }

    }

    /**
     * The method calculates the system load for the given values.
     *
     * @param usedSums The sums of the used values per system load commodity.
     * @param capacitiesSums The sums of capacities per system load commodity.
     * @return The system load value.
     */
    public Double calcSystemLoad(Double[] usedSums, Double[] capacitiesSums) {
        checkArgument(usedSums != null && usedSums.length > 0,
                "Invalid used sum values when calculating system load");
        checkArgument(capacitiesSums != null && capacitiesSums.length > 0,
                "Invalid capacitites sum values when calculating system load");

        logger.debug("SYSLOAD- Capacities are: " + Arrays.deepToString(capacitiesSums));
        logger.debug("SYSLOAD- Used are: " + Arrays.deepToString(usedSums));

        // If the input sums are invalid, return null system load
        for (Double used : usedSums) {
            if (used == null)
                return null;
        }
        for (Double capacity : capacitiesSums) {
            if (capacity == null)
                return null;
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
     * @param slice The slice we want to update.
     * @throws VmtDbException
     */
    public void writeSystemLoadToDb(String slice, Double[] sliceUsed, Double[] sliceCapacities,
                                    long snapshotTime, Collection<Pair<CommoditySoldDTO, TopologyEntityDTO>> soldCollection,
                                    Collection<Pair<CommodityBoughtDTO, TopologyEntityDTO>> boughtCollection)
            throws VmtDbException {

        //Send the metrics out to be written to the DB:
        Date date = new Date(snapshotTime);

        for (Pair<CommoditySoldDTO, TopologyEntityDTO> comm : soldCollection) {
            final String uuid = Long.toString(comm.second.getOid());
            final String propertyType = SystemLoadCommodities.toSystemLoadCommodity(comm.first.getCommodityType().getType()).toString();

            systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), uuid, null, propertyType,
                    "used", comm.first.getCapacity(), comm.first.getUsed(), comm.first.getUsed(), comm.first.getPeak(),
                    RelationType.COMMODITIES, comm.first.getCommodityType().getKey());
        }

        for (Pair<CommodityBoughtDTO, TopologyEntityDTO> comm : boughtCollection) {
            final String uuid = Long.toString(comm.second.getOid());
            final String producerUuid = Long.toString(comm.second.getCommoditiesBoughtFromProviders(0).getProviderId());
            final String propertyType = SystemLoadCommodities.toSystemLoadCommodity(comm.first.getCommodityType().getType()).toString();

            systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), uuid, producerUuid, propertyType,
                    "used", null, comm.first.getUsed(), comm.first.getUsed(), comm.first.getPeak(),
                    RelationType.COMMODITIESBOUGHT, comm.first.getCommodityType().getKey());
        }

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "CPU",
                "total_capacity", HostsSumCapacities.getCpu().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "MEM",
                "total_capacity", HostsSumCapacities.getMem().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "IO_THROUGHPUT",
                "total_capacity", HostsSumCapacities.getIoThroughput().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "NET_THROUGHPUT",
                "total_capacity", HostsSumCapacities.getNetThroughput().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "CPU_PROVISIONED",
                "total_capacity", HostsSumCapacities.getCpuProvisioned().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "MEM_PROVISIONED",
                "total_capacity", HostsSumCapacities.getMemProvisioned().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "STORAGE_ACCESS",
                "total_capacity", StoragesSumCapacities.getStorageAccess().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "STORAGE_PROVISIONED",
                "total_capacity", StoragesSumCapacities.getStorageProvisioned().get(slice), null, null, null,
                RelationType.COMMODITIESBOUGHT, null);

        for (int i = 0; i < SystemLoadCommodities.SIZE; i ++) {
            double used = sliceUsed[i] != null ? sliceUsed[i] : 0.0;
            double capacity = sliceCapacities[i] != null ? sliceCapacities[i] : 0.0;
            systemLoadWriter.insertSystemLoadRecord(slice, new Timestamp(date.getTime()), null, null, "system_load",
                    SystemLoadCommodities.get(i).toString(), capacity, used, used, used, RelationType.COMMODITIES, null);
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
            systemLoadWriter.deleteSystemLoadRecords(slice, snapshot);
        }
        catch (VmtDbException e) {
            logger.error("SYSLOAD- Error when deleting system load form DB : " + e);
        }
        catch (ParseException e) {
            logger.error("SYSLOAD- Error when converting snapshot to current date : " + e);
        }
    }

}
