package com.vmturbo.cost.calculation.topology;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Units;

/**
 * An {@link EntityInfoExtractor} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 */
public class TopologyEntityInfoExtractor implements EntityInfoExtractor<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

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
            final VirtualVolumeInfo volumeConfig = entity.getTypeSpecificInfo().getVirtualVolume();
            return Optional.of(new VirtualVolumeConfig(
                    getCommodityCapacity(entity, CommodityType.STORAGE_ACCESS),
                    getCommodityCapacity(entity, CommodityType.STORAGE_AMOUNT),
                    getCommodityCapacity(entity, CommodityType.IO_THROUGHPUT) / Units.KBYTE,
                    (float)volumeConfig.getHourlyBilledOps(),
                    volumeConfig.getIsEphemeral(),
                    volumeConfig.hasRedundancyType() ? volumeConfig.getRedundancyType() : null));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    @Override
    public Optional<Float> getRDBCommodityCapacity(@Nonnull final TopologyEntityDTO entity, @Nonnull CommodityType commodityType) {
        switch (entity.getEntityType()) {
            case EntityType.DATABASE_VALUE:
            case EntityType.DATABASE_SERVER_VALUE:
                return getRawCommodityCapacity(entity, commodityType);
            default:
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
        return Optional.of(ComputeTierConfig.builder()
                .computeTierOid(entity.getOid())
                .numCores(tierInfo.getNumCores())
                .numCoupons(tierInfo.getNumCoupons())
                .isBurstableCPU(tierInfo.getBurstableCPU())
                .build());
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
                    dbConfig.hasDeploymentType() ? dbConfig.getDeploymentType() : null,
                    dbConfig.hasHourlyBilledOps() ? dbConfig.getHourlyBilledOps() : null));
            }
        }
        return Optional.empty();
    }

    private static Optional<Float> getRawCommodityCapacity(
            @Nonnull final TopologyEntityDTO topologyEntityDTO,
            @Nonnull final CommodityType commodityType) {
        final Optional<Double> capacity = topologyEntityDTO.getCommoditySoldListList().stream()
                .filter(commodity -> commodity.getCommodityType().getType()
                        == commodityType.getNumber())
                .map(CommoditySoldDTO::getCapacity)
                .findAny();
        return capacity.map(Double::floatValue);
    }

    private static float getCommodityCapacity(
            @Nonnull final TopologyEntityDTO topologyEntityDTO,
            @Nonnull final CommodityType commodityType) {
        final Optional<Float> capacity = getRawCommodityCapacity(topologyEntityDTO, commodityType);
        if (!capacity.isPresent() && !isEphemeralVolume(topologyEntityDTO)) {
            // This is not an error for AWS ephemeral (instance store) volumes because they have no
            // Storage Access and IO Throughput commodities.
            logger.warn("Missing commodity {} for topologyEntityDTO {} (OID: {})", commodityType,
                    topologyEntityDTO.getDisplayName(), topologyEntityDTO.getOid());
        }
        return capacity.orElse(0f);
    }


    private static boolean isEphemeralVolume(@Nonnull final TopologyEntityDTO volume) {
        if (!volume.hasTypeSpecificInfo()) {
            return false;
        }
        final TypeSpecificInfo typeSpecificInfo = volume.getTypeSpecificInfo();
        if (!typeSpecificInfo.hasVirtualVolume()) {
            return false;
        }
        return typeSpecificInfo.getVirtualVolume().getIsEphemeral();
    }
}
