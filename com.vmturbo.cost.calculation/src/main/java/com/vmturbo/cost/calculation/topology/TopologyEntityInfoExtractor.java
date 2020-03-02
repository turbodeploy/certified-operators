package com.vmturbo.cost.calculation.topology;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
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

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityState getEntityState(@Nonnull final TopologyEntityDTO entity) {
        return entity.getEntityState();
    }

    @Nonnull
    @Override
    public Optional<ComputeConfig> getComputeConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
            && entity.hasTypeSpecificInfo() && entity.getTypeSpecificInfo().hasVirtualMachine()) {
            VirtualMachineInfo vmConfig = entity.getTypeSpecificInfo().getVirtualMachine();
            return Optional.of(new ComputeConfig(vmConfig.getGuestOsInfo().getGuestOsType(),
                    vmConfig.getTenancy(),
                    vmConfig.getBillingType(),
                    vmConfig.getNumCpus(),
                    vmConfig.getLicenseModel()));
        } else {
            return Optional.empty();
        }
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
                    volumeConfig.getStorageAmountCapacity(),
                    volumeConfig.getIsEphemeral()));
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
        return Optional.of(new ComputeTierConfig(tierInfo.getNumCoupons(), tierInfo.getNumCores(), tierInfo.getBurstableCPU()));
    }

    @Override
    @Nonnull
    public Optional<DatabaseConfig> getDatabaseConfig(
            TopologyEntityDTO entity) {
        if ((entity.getEntityType() == EntityType.DATABASE_SERVER_VALUE
            || entity.getEntityType() == EntityType.DATABASE_VALUE)
            && entity.hasTypeSpecificInfo()) {
            if (entity.getTypeSpecificInfo().hasDatabase()) {
                DatabaseInfo dbConfig = entity.getTypeSpecificInfo().getDatabase();
                return Optional.of(new DatabaseConfig(dbConfig.getEdition(),
                    dbConfig.getEngine(),
                    dbConfig.hasLicenseModel() ? dbConfig.getLicenseModel() : null,
                    dbConfig.hasDeploymentType() ? dbConfig.getDeploymentType() : null));
            }
        }
        return Optional.empty();
    }

}
