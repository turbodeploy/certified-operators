package com.vmturbo.history.ingesters.live.writers;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CPUS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_SOCKETS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS;
import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsLatest.CLUSTER_STATS_LATEST;
import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Ingestion writer to create cluster stats from live topology.
 *
 * <p>Statistics are aggregated over all the entities belonging to a given cluster, and the
 * results are written to cluster stats tables during finish processing.</p>
 *
 * <p>Currently, only compute clusters are supported. Storage clusters are not processed.</p>
 */
public class ClusterStatsWriter extends TopologyWriterBase {
    private static final Logger logger = LogManager.getLogger();

    private static final String COMPUTE_CLUSTER_MEMBER_MAP = "compute_cluster_member_map";

    // timestamp fo the topology we're processing - will be used for all inserted records
    private final Timestamp recordTimestamp;
    // bulk loader factory to get record loaders for tables
    private final BulkLoader<ClusterStatsLatestRecord> loader;
    // assorted database stuff - we use it for getting connections and jooq contexts
    private final HistorydbIO historydbIO;

    /**
     * Create a new instance to process a newly arrived topology.
     *
     * @param topologyInfo metadata for this topology
     * @param loaders      factory to get bulk loaders for tables
     * @param historydbIO  assorted db stuff
     * @param groupService group service endpoint
     */
    public ClusterStatsWriter(
            TopologyInfo topologyInfo,
            SimpleBulkLoaderFactory loaders,
            HistorydbIO historydbIO,
            GroupServiceBlockingStub groupService) {
        this.recordTimestamp = new Timestamp(topologyInfo.getCreationTime());
        this.loader = loaders.getLoader(CLUSTER_STATS_LATEST);
        this.historydbIO = historydbIO;
        // load cluster membership info so we'll know when we see an entity that belongs to or
        // is otherwise related to a cluster
        getClusterMemberships(groupService);
    }

    // Per-cluster aggregators
    private TLongObjectHashMap<ClusterAggregator> clusterAggregators = new TLongObjectHashMap<>(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L);

    @Override
    public ChunkDisposition processEntities(@Nonnull Collection<TopologyEntityDTO> chunk, @Nonnull String infoSummary) {
        for (TopologyEntityDTO entity : chunk) {
            clusterAggregators.forEachValue(aggregator -> {
                aggregator.processEntity(entity);
                return true;
            });
        }
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int objectCount, boolean expedite, String infoSummary) throws InterruptedException {
        clusterAggregators.forEachValue(aggregator -> {
            try {
                aggregator.finish();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        });
    }

    private final Map<GroupType, Consumer<Grouping>> clusterInitializers =
            ImmutableMap.<GroupType, Consumer<Grouping>>builder()
                    .put(GroupType.COMPUTE_HOST_CLUSTER, this::initComputeHostCluster)
                    // TOOD: Implement other cluster types
                    .build();

    /**
     * Load cluster information from group component, and create a {@link ClusterAggregator} for
     * each cluster.
     *
     * @param groupService group service endpoint for retrieving group info
     */
    private void getClusterMemberships(GroupServiceBlockingStub groupService) {
        for (GroupType clusterType : clusterInitializers.keySet()) {
            final Iterator<Grouping> groups = groupService.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder().setGroupType(clusterType))
                    .build());
            groups.forEachRemaining(cluster -> {
                clusterInitializers.get(clusterType).accept(cluster);
            });
        }
    }

    private void initComputeHostCluster(Grouping cluster) {
        clusterAggregators.put(cluster.getId(),
                new ComputeClusterAggregator(cluster, loader, recordTimestamp));
    }


    /**
     * Base class for classes that aggregate data pertaining to a given cluster during the
     * processing of relevant entities encountered in the topology.
     */
    private abstract static class ClusterAggregator {
        // oid of this cluster
        protected final long clusterId;
        // oids of hosts belonging to this cluster
        protected final TLongSet members =
                new TLongHashSet(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L);
        // bulk loader we'll use when it comes time to write cluster stats records
        protected final BulkLoader<ClusterStatsLatestRecord> loader;
        // topology creation time, to be used as `recorded_on` value in cluster_stats records
        protected final Timestamp recordTimestamp;

        /**
         * Create a new instance for a cluster.
         *
         * @param cluster         {@link Grouping} structure with details of this cluster
         * @param loader          bulk loader to create new cluster_stats records
         * @param recordTimestamp timestamp for inserted records
         */
        ClusterAggregator(Grouping cluster,
                BulkLoader<ClusterStatsLatestRecord> loader, Timestamp recordTimestamp) {
            this.clusterId = cluster.getId();
            this.members.addAll(GroupProtoUtil.getAllStaticMembers(cluster.getDefinition()));
            this.loader = loader;
            this.recordTimestamp = recordTimestamp;
        }

        /**
         * Update this aggregator with data from the given entity, encountered in the topology.
         *
         * <p>All entities will be presented to all aggregators, so in many cases a given
         * aggregator will have no intrest in a presented entity and should simply return.</p>
         *
         * @param entity entity to be processed
         */
        abstract void processEntity(TopologyEntityDTO entity);

        /**
         * Finish operation of this aggregator, after the entire topology has been processed.
         *
         * <p>This is where cluster_stats records are written.</p>
         *
         * @throws InterruptedException if interrupted
         */
        abstract void finish() throws InterruptedException;
    }

    /**
     * Aggregator for a compute cluster, whose members are physical hosts.
     */
    private static class ComputeClusterAggregator extends ClusterAggregator {
        // total number of cpus across all hosts
        private int numCpus = 0;
        // total number of sockets across all hosts
        private int numSockets = 0;
        // total of used CPU resource across all hosts
        private double cpuUsed = 0.0;
        // total of CPU capacity across all hosts
        private double cpuCapacity = 0.0;
        // total of used memory across all hosts
        private double memUsed = 0.0;
        // total of memory capacity across all hosts
        private double memCapacity = 0.0;
        // total # of VM entities that buy commodities from member hosts
        private int vmCount = 0;

        ComputeClusterAggregator(Grouping cluster,
                BulkLoader<ClusterStatsLatestRecord> loader, Timestamp recordTimestamp) {
            super(cluster, loader, recordTimestamp);
        }

        /**
         * We process entities representing hosts or VMs.
         *
         * @param entity entity to be processed
         */
        @Override
        void processEntity(final TopologyEntityDTO entity) {
            switch (entity.getEntityType()) {
                case EntityType.PHYSICAL_MACHINE_VALUE:
                    processHost(entity);
                    break;
                case EntityType.VIRTUAL_MACHINE_VALUE:
                    processVm(entity);
                    break;
                default:
                    // no other entities are of interest
                    break;
            }
        }

        /**
         * Process a host entity.
         *
         * @param host host entity
         */
        private void processHost(TopologyEntityDTO host) {
            if (members.contains(host.getOid())) {
                // this host belongs to this cluster, so process it
                numCpus += host.getTypeSpecificInfo().getPhysicalMachine().getNumCpus();
                numSockets += host.getTypeSpecificInfo().getPhysicalMachine().getNumCpuSockets();
                host.getCommoditySoldListList().forEach(sold -> {
                    switch (CommodityType.forNumber(sold.getCommodityType().getType())) {
                        case CPU:
                            cpuUsed += sold.getUsed();
                            cpuCapacity += sold.getCapacity();
                            break;
                        case MEM:
                            memUsed += sold.getUsed();
                            memCapacity += sold.getCapacity();
                            break;
                        default:
                            // nothing else is of interest
                    }
                });
            }
        }

        /**
         * Process a VM entity.
         *
         * <p>Processing is limited to counting this VM in this cluster's `numVM` statistic, if
         * the VM buys any commodities from any of our member hosts.</p>
         *
         * @param vm VM entity
         */
        private void processVm(TopologyEntityDTO vm) {
            if (vm.getCommoditiesBoughtFromProvidersList().stream()
                    .anyMatch(cbfp -> members.contains(cbfp.getProviderId()))) {
                // this VM buys from a host in this cluster, so we count it
                vmCount += 1;
            }
        }

        /**
         * Write cluster stats records for all the aggregated stats for this cluster.
         *
         * @throws InterruptedException if interrupted
         */
        @Override
        void finish() throws InterruptedException {
            writeRecord(CPU, USED, cpuUsed);
            writeRecord(CPU, CAPACITY, cpuCapacity);
            writeRecord(NUM_CPUS, NUM_CPUS, numCpus);
            writeRecord(NUM_SOCKETS, NUM_SOCKETS, numSockets);
            writeRecord(MEM, USED, memUsed);
            writeRecord(MEM, CAPACITY, memCapacity);
            writeRecord(NUM_HOSTS, NUM_HOSTS, members.size());
            writeRecord(NUM_VMS, NUM_VMS, vmCount);
        }

        /**
         * Write a record to the `cluster_stats_latest` table for this cluster.
         *
         * @param propertyType    value for `property_type` column
         * @param propertySubtype value for `property_subtype` column
         * @param value           stats value
         * @throws InterruptedException if interrupted
         */
        void writeRecord(String propertyType, String propertySubtype, double value)
                throws InterruptedException {
            // create and populate a new record
            ClusterStatsLatestRecord record = CLUSTER_STATS_LATEST.newRecord();
            record.setRecordedOn(recordTimestamp);
            record.setInternalName(Long.toString(clusterId));
            record.setPropertyType(propertyType);
            record.setPropertySubtype(propertySubtype);
            record.setValue(value);
            // and write it to the database
            loader.insert(record);
        }

    }

    /**
     * Factory class for {@link ClusterStatsWriter}.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final HistorydbIO historydbIO;
        private final GroupServiceBlockingStub groupService;

        /**
         * Create a new factory.
         *
         * @param historydbIO  db stuff
         * @param groupService group service endpoint
         */
        public Factory(HistorydbIO historydbIO, GroupServiceBlockingStub groupService) {
            this.historydbIO = historydbIO;
            this.groupService = groupService;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>> getChunkProcessor(
                TopologyInfo topologyInfo,
                SimpleBulkLoaderFactory loaders) {
            return Optional.of(new ClusterStatsWriter(topologyInfo, loaders, historydbIO, groupService));
        }
    }
}
