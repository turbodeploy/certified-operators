package com.vmturbo.cloud.commitment.analysis.demand.store;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.SetMultimap;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Stores the per-entity allocations on compute tier. The allocations represent the time during which an
 * entity (virtual machine) is provisioned on a compute tier (instance size) and powered on. The store
 * does not contain the allocated demand for a compute tier while the associated entity is powered off.
 *
 */
public interface ComputeTierAllocationStore {

    /**
     * Persists the allocation datapoints. The datapoints represent the allocated demand for a single
     * point in time, as determined by the creation time of the {@code topologyInfo}.
     *
     * <p>If the immediately prior topology was also persisted, any matching allocated records are extended
     * between the two points, representing allocated demand the duration between the two topologies.
     * this behavior can be chained among multiple consecutive topologies to treat point in time datapoints
     * from a topology as continuous demand over a period of time.
     *
     * @param topologyInfo The {@link TopologyInfo} associated with the allocated demand.
     * @param allocationDatapoints The compute tier allocation demand. Allocated demand should be only
     *                             the source topology and should only represent powered on entities.
     */
    void persistAllocations(@Nonnull TopologyInfo topologyInfo,
                            @Nonnull Collection<ComputeTierAllocationDatapoint> allocationDatapoints);


    /**
     * Stream the compute tier allocations that meet the {@code filter} criteria. Each {@link EntityComputeTierAllocation}
     * record represents an entity allocated on a compute tier for a continuous period of time. There may
     * be multiple {@link EntityComputeTierAllocation} instances per entity, representing a change in the
     * allocated compute tier of an entity or suspension of the entity.
     *
     * @param filter The filter for returned {@link EntityComputeTierAllocation} records.
     * @return A {@link Stream} containing {@link EntityComputeTierAllocation} records that meet the
     * {@code filter} criteria.
     */
    @Nonnull
    Stream<EntityComputeTierAllocation> streamAllocations(@Nonnull EntityComputeTierAllocationFilter filter);

    /**
     * Retrieves a set of {@link EntityComputeTierAllocation} instances that match {@code filter}. The
     * query will block and therefore guarantees the records contain only completely processed topology
     * data.
     * @param filter The {@link EntityComputeTierAllocationFilter}.
     * @return The {@link EntityComputeTierAllocationSet}, containing all allocation records and the
     * latest processed {@link TopologyInfo}.
     */
    @Nonnull
    EntityComputeTierAllocationSet getAllocations(@Nonnull EntityComputeTierAllocationFilter filter);


    /**
     * Removes any {@link EntityComputeTierAllocation} that meet the {@code filter} criteria.
     *
     * @param filter The filter used to scope the {@link EntityComputeTierAllocation} records to delete.
     * @return The number of records that were successfully deleted.
     *
     */
    int deleteAllocations(@Nonnull EntityComputeTierAllocationFilter filter);

    /**
     * Registers an update listener. The listener will be invoked after a new set of allocations
     * have been persisted.
     * @param listener The {@link ComputeTierAllocationUpdateListener}.
     */
    void registerUpdateListener(@Nonnull ComputeTierAllocationUpdateListener listener);

    /**
     * A listener for updates to the set of stored allocations.
     */
    interface ComputeTierAllocationUpdateListener {

        /**
         * A callback method, invoked once a set of allocation records have been persisted.
         * @param topologyInfo The {@link TopologyInfo} associated with the latest set
         *                     of stored allocation records.
         */
        void onAllocationUpdate(@Nonnull TopologyInfo topologyInfo);
    }

    /**
     * A set of {@link EntityComputeTierAllocation} records, along with the latest stored
     * topology info.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface EntityComputeTierAllocationSet {

        /**
         * The set of {@link EntityComputeTierAllocation} by entity OID.
         * @return An immutable set of {@link EntityComputeTierAllocation} by entity OID.
         */
        @Nonnull
        SetMultimap<Long, EntityComputeTierAllocation> allocations();

        /**
         * The latest processed/stored {@link TopologyInfo}. This will be empty in the case where
         * the store has not persisted allocation data since startup.
         * @return The latest processed/stored {@link TopologyInfo}.
         */
        @Nonnull
        Optional<TopologyInfo> latestTopologyInfo();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link EntityComputeTierAllocationSet} instances.
         */
        class Builder extends ImmutableEntityComputeTierAllocationSet.Builder {}
    }
}
