package com.vmturbo.cost.calculation.topology;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * An {@link EntityInfoExtractor} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 *
 * TODO (roman, Aug 16 2018): Move this to the cost component. It's provided here for illustration
 * purposes.
 */
public class TopologyEntityInfoExtractor implements EntityInfoExtractor<TopologyEntityDTO> {

    @Override
    public int getEntityType(@Nonnull final TopologyEntityDTO entity) {
        return entity.getEntityType();
    }

    @Override
    public long getId(@Nonnull final TopologyEntityDTO entity) {
        return entity.getOid();
    }

    @Override
    public String getName(@Nonnull final TopologyEntityDTO entity) {
        return entity.getDisplayName();
    }

    @Nonnull
    @Override
    public Optional<ComputeConfig> getComputeConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return Optional.empty();
        }

        if (!entity.hasTypeSpecificInfo()) {
            return Optional.empty();
        }

        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE &&
                entity.getTypeSpecificInfo().hasVirtualMachine()) {
            VirtualMachineInfo vmConfig = entity.getTypeSpecificInfo().getVirtualMachine();
            return Optional.of(new ComputeConfig(vmConfig.getGuestOsType(), vmConfig.getTenancy()));
        }

        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<NetworkConfig> getNetworkConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return Optional.empty();
        }
        VirtualMachineInfo vmConfig = entity.getTypeSpecificInfo().getVirtualMachine();
        return Optional.of(new NetworkConfig(vmConfig.getIpAddressesList()));
    }

    @Override
    @Nonnull
    public Optional<VirtualVolumeConfig> getVolumeConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.VIRTUAL_VOLUME_VALUE) {
            return Optional.empty();
        }

        if (entity.getTypeSpecificInfo().hasVirtualVolume()) {
            VirtualVolumeInfo volumeConfig = entity.getTypeSpecificInfo().getVirtualVolume();
            return Optional.of(new VirtualVolumeConfig(
                    volumeConfig.getStorageAccessCapacity(),
                    volumeConfig.getStorageAmountCapacity()));
        } else {
            return Optional.empty();
        }
    }

    @Override
    @Nonnull
    public Optional<ComputeTierConfig> getComputeTierConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            return Optional.empty();
        }
        final ComputeTierInfo tierInfo = entity.getTypeSpecificInfo().getComputeTier();
        return Optional.of(new ComputeTierConfig(tierInfo.getNumCoupons()));
    }

    @Override
    public Optional<DatabaseConfig> getDatabaseConfig(
            TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.DATABASE_SERVER_VALUE ||
                entity.getEntityType() != EntityType.DATABASE_VALUE) {
            return Optional.empty();
        }

        if (!entity.hasTypeSpecificInfo()) {
            return Optional.empty();
        }

        if (entity.getTypeSpecificInfo().hasDatabase()) {
            DatabaseInfo dbConfig = entity.getTypeSpecificInfo().getDatabase();
            return Optional.of(new DatabaseConfig(dbConfig.getEdition(),
                                                  dbConfig.getEngine(),
                                                  dbConfig.getLicenseModel(),
                                                  dbConfig.getDeploymentType()));
        }

        return Optional.empty();

    }

}
