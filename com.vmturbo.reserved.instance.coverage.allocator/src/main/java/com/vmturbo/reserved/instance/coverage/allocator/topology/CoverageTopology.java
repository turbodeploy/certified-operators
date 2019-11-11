package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * A sub-interface of {@link CloudTopology}, in which access to instances of {@link ReservedInstanceBought}
 * and {@link ReservedInstanceSpec} is provided in a similar manner to {@link TopologyEntityDTO}
 * instances.
 */
@Immutable
public interface CoverageTopology extends CloudTopology<TopologyEntityDTO> {

    /**
     * @return An unmodifiable map of all instances of {@link ReservedInstanceBought} in this topology,
     * indexe by the OID of each instance
     */
    @Nonnull
    Map<Long, ReservedInstanceBought> getAllReservedInstances();

    /**
     * Determines the tier of an entity. Depending on the entity, this may be either a compute or
     * database tier. Providers a wrapper around {@link CloudTopology#getComputeTier(long)} and
     * {@link CloudTopology#getDatabaseTier(long)}.
     *
     * <p>
     * Note: An assumption is made about the correct tier type based on the entity type e.g. only
     * attempts to determine a compute tier if the entity is a VM
     *
     * @param entityOid The OID of the entity
     * @return An optional containing the tier, if one is found directly connected to the entity. An
     * empty optional if the entity cannot be found or is not connected to an appropriate tier
     */
    @Nonnull
    Optional<TopologyEntityDTO> getProviderTier(long entityOid);

    /**
     * Determines the tier associated with a {@link ReservedInstanceBought}. There is no check that
     * tier ID associated with the {@link ReservedInstanceBought} instance is a valid tier entity type.
     *
     * @param riOid The OID of the RI
     * @return An optional containing an entity (presumably a tier type) representing {@code riOid's}
     * tier. An empty optional if the {@link ReservedInstanceSpec} for {@code riOid} cannot be found
     * or the associated tier cannot be found.
     */
    @Nonnull
    Optional<TopologyEntityDTO> getReservedInstanceProviderTier(long riOid);

    /**
     * Determines the {@link ReservedInstanceSpec} instance associated with a {@link ReservedInstanceBought}
     * instance.
     *
     * @param riOid The OID of the RI
     * @return An optional containing the {@link ReservedInstanceSpec} instance, if it can be found.
     * Otherwise, an empty optional
     */
    @Nonnull
    Optional<ReservedInstanceSpec> getSpecForReservedInstance(long riOid);

    /**
     * @param riOid The OID of the RI
     * @return An optional containing the {@link ReservedInstanceBought} instance associated with
     * {@code riOid}. An empty optional if the RI cannot be found
     */
    @Nonnull
    Optional<ReservedInstanceBought> getReservedInstanceBought(long riOid);

    /**
     * Analogous to {@link CloudTopology#getOwner(long)} for a {@link ReservedInstanceBought} instance.
     *
     * @param riOid The OID of the RI
     * @return An optional containing the direct owner of the RI, if it can be found (determined through
     * RI info). Otherwise, an empty optional
     */
    @Nonnull
    Optional<TopologyEntityDTO> getReservedInstanceOwner(long riOid);

    /**
     * Analogous to {@link CloudTopology#getConnectedRegion(long)} for a {@link ReservedInstanceBought}
     * instance.
     *
     * @param riOid The OID of the RI
     * @return An optional containing the region of the RI, if it can be found (determined through
     * RI spec). Otherwise, an empty optional
     */
    @Nonnull
    Optional<TopologyEntityDTO> getReservedInstanceRegion(long riOid);

    /**
     * Analogous to {@link CloudTopology#getConnectedAvailabilityZone(long)} for a {@link ReservedInstanceBought}
     * instance.
     *
     * @param riOid The OID of the RI
     * @return An optional containing the availability zone of the RI, if it can be found (determined
     * through RI info). Otherwise, an empty optional
     */
    @Nonnull
    Optional<TopologyEntityDTO> getReservedInstanceAvailabilityZone(long riOid);

    /**
     * Resolves the capacity for each instance of {@link ReservedInstanceBought} contained within
     * this topology
     * @return An immutable map of {@literal <ReservedInstanceBought OID, Coverage Capacity>}
     */
    @Nonnull
    Map<Long, Long> getReservedInstanceCapacityByOid();


    /**
     * Resolves the probe types for an entity. This will be resolved through the discovered
     * targets of the entity, querying the topology-processor for the target into (including the probe
     * type)
     * @param entityOid The OID fo the entity
     * @return A set of {@link SDKProbeType} from the discovery origin target list of the entity
     */
    Set<SDKProbeType> getProbeTypesForEntity(long entityOid);
}
