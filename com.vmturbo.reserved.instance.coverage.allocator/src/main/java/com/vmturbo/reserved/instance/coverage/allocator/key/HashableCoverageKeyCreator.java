package com.vmturbo.reserved.instance.coverage.allocator.key;

import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.ACCOUNT_SCOPE_OID_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.AVAILABILITY_ZONE_OID_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.PLATFORM_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.REGION_OID_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.TENANCY_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.TIER_FAMILY_KEY;
import static com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyMaterial.TIER_OID_KEY;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A {@link CoverageKeyCreator} that creates {@link HashableCoverageKey} instances
 */
public class HashableCoverageKeyCreator implements CoverageKeyCreator {

    private final Logger logger = LogManager.getLogger();


    private final CoverageTopology coverageTopology;

    private final CoverageKeyCreationConfig keyCreationConfig;

    private HashableCoverageKeyCreator(@Nonnull CoverageTopology coverageTopology,
                                      @Nonnull CoverageKeyCreationConfig keyCreationConfig) {

        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.keyCreationConfig = Objects.requireNonNull(keyCreationConfig);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<CoverageKey> createKeyForEntity(final long entityOid) {

        final CoverageKey.Builder keyBuilder = HashableCoverageKey.newBuilder();
        boolean validKey = addEntityInfo(coverageTopology.getEntity(entityOid), keyBuilder);
        validKey &= addOwnershipInfo(coverageTopology.getOwner(entityOid), keyBuilder);
        validKey &= addTierInfo(coverageTopology.getProviderTier(entityOid), keyBuilder);
        validKey &= addRegionInfo(coverageTopology.getConnectedRegion(entityOid), keyBuilder);
        validKey &= addAvailabilityZoneInfo(coverageTopology.getConnectedAvailabilityZone(entityOid),
                keyBuilder);

        return validKey ? Optional.of(keyBuilder.build()) : Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<CoverageKey> createKeyForReservedInstance(final long riOid) {
        final CoverageKey.Builder keyBuilder = HashableCoverageKey.newBuilder();
        boolean validKey = addReservedInstanceSpecInfo(
                coverageTopology.getSpecForReservedInstance(riOid), keyBuilder);
        validKey &= addOwnershipInfo(coverageTopology.getReservedInstanceOwner(riOid), keyBuilder);
        validKey &= addTierInfo(coverageTopology.getReservedInstanceProviderTier(riOid), keyBuilder);
        validKey &= addRegionInfo(coverageTopology.getReservedInstanceRegion(riOid), keyBuilder);
        validKey &= addAvailabilityZoneInfo(coverageTopology.getReservedInstanceAvailabilityZone(riOid),
                keyBuilder);

        return validKey ? Optional.of(keyBuilder.build()) : Optional.empty();
    }

    /**
     * @return An instance of {@link CoverageKeyCreatorFactory}, which creates instances of
     * {@link HashableCoverageKeyCreator}
     */
    public static CoverageKeyCreatorFactory newFactory() {
        return HashableCoverageKeyCreator::new;
    }

    private boolean addEntityInfo(@Nonnull Optional<TopologyEntityDTO> optEntity,
                                  @Nonnull CoverageKey.Builder keyBuilder) {
        return optEntity
                .map(entity -> {
                    switch (entity.getEntityType()) {

                        case EntityType.VIRTUAL_MACHINE_VALUE:
                            final VirtualMachineInfo vmInfo = entity.getTypeSpecificInfo()
                                    .getVirtualMachine();
                            keyBuilder.keyMaterial(TENANCY_KEY, vmInfo.getTenancy());

                            if (!keyCreationConfig.isPlatformFlexible()) {
                                keyBuilder.keyMaterial(PLATFORM_KEY,
                                        vmInfo.getGuestOsInfo().getGuestOsType());
                            }

                            return true;
                        default:
                            throw new UnsupportedOperationException();
                    }
                }).orElse(false);
    }

    private boolean addReservedInstanceSpecInfo(@Nonnull Optional<ReservedInstanceSpec> optRISpec,
                                            @Nonnull CoverageKey.Builder keyBuilder) {
        return optRISpec.map(riSpec -> {
            final ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();

            keyBuilder.keyMaterial(TENANCY_KEY, riSpecInfo.getTenancy());
            if (!keyCreationConfig.isPlatformFlexible()) {
                keyBuilder.keyMaterial(PLATFORM_KEY, riSpecInfo.getOs());
            }

            return true;
        }).orElse(false);
    }

    private boolean addOwnershipInfo(@Nonnull Optional<TopologyEntityDTO> optDirectOwner,
                                       @Nonnull CoverageKey.Builder keyBuilder) {
        return optDirectOwner.map(directOwner -> {

            if (keyCreationConfig.isSharedScope()) {
                final long sharedOwnerOid = coverageTopology.getBillingFamilyForEntity(directOwner.getOid())
                        .map(g -> g.group().getId())
                        .orElse(directOwner.getOid());

                keyBuilder.keyMaterial(ACCOUNT_SCOPE_OID_KEY, sharedOwnerOid);
            } else {
                keyBuilder.keyMaterial(ACCOUNT_SCOPE_OID_KEY, directOwner.getOid());
            }

            return true;

        }).orElse(false);

    }

    private boolean addRegionInfo(@Nonnull Optional<TopologyEntityDTO> optRegion,
                                    @Nonnull CoverageKey.Builder keyBuilder) {
        return optRegion.map(region -> {
            keyBuilder.keyMaterial(REGION_OID_KEY, region.getOid());
            return true;
        }).orElse(false);
    }

    private boolean addAvailabilityZoneInfo(@Nonnull Optional<TopologyEntityDTO> optZone,
                                              @Nonnull CoverageKey.Builder keyBuilder) {
        if (keyCreationConfig.isZoneScoped()) {
            return optZone.map(zone -> {
                keyBuilder.keyMaterial(AVAILABILITY_ZONE_OID_KEY, zone.getOid());
                return true;
            }).orElse(false);
        } else {
            return true;
        }
    }

    private boolean addTierInfo(@Nonnull Optional<TopologyEntityDTO> optTier,
                                  @Nonnull CoverageKey.Builder keyBuilder) {
        return optTier.map(tier -> {
            switch (tier.getEntityType()) {

                case EntityType.COMPUTE_TIER_VALUE:

                    if (keyCreationConfig.isInstanceSizeFlexible()) {
                        final ComputeTierInfo computeTierInfo =
                                tier.getTypeSpecificInfo().getComputeTier();
                        keyBuilder.keyMaterial(TIER_FAMILY_KEY, computeTierInfo.getFamily());
                    } else {
                        keyBuilder.keyMaterial(TIER_OID_KEY, tier.getOid());
                    }

                    return true;
                default:
                    throw new UnsupportedOperationException();
            }
        }).orElse(false);
    }
}
