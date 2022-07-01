package com.vmturbo.cost.calculation.topology;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.Units;

/**
 * An {@link EntityInfoExtractor} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 */
public class TopologyEntityInfoExtractor implements EntityInfoExtractor<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();
    private static final List<Integer> commodityTypesSupported = Arrays.asList(CommodityType.NUM_VCORE_VALUE, CommodityType.MEM_PROVISIONED_VALUE);

    /**
     * Name of os type entity property name.
     * TODO : this should be removed once ASP move from ApplicationComponent to VirtualMachineSpec: OM-83212.
     */
    public static final String OS_TYPE = "OS_TYPE";

    /**
     * Allowed OS type for Application Component.
     * TODO : this should be removed once ASP move from ApplicationComponent to VirtualMachineSpec: OM-83212.
     */
    public static final Collection<String> APPLICATION_SERVICE_ALLOWED_OS =
            Stream.of(OSType.LINUX, OSType.WINDOWS).map(Enum::name).collect(Collectors.toList());

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
            final Map<CommodityType, Double> pricedCommoditiesBought = entity.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                    .filter(c -> c.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                    .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                    .flatMap(Collection::stream)
                    .filter(commodity -> (commodityTypesSupported.contains(commodity.getCommodityType().getType())))
                    .collect(Collectors.toMap(c -> CommodityType.valueOf(c.getCommodityType().getType()), c -> c.getUsed()));
            return Optional.of(new ComputeConfig(vmConfig.getGuestOsInfo().getGuestOsType(),
                    vmConfig.getTenancy(),
                    vmConfig.getBillingType(),
                    vmConfig.getNumCpus(),
                    vmConfig.getLicenseModel(),
                    pricedCommoditiesBought));
        } else if (entity.getEntityType() == EntityType.APPLICATION_COMPONENT_VALUE) {
            //TODO : Roop remove this block once we move away from legacy model.
            return getApplicationServiceConfig(entity);
        } else if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_SPEC_VALUE) {
            return getApplicationServiceConfig(entity);
        }
        return Optional.empty();
    }

    @Nonnull
    private Optional<ComputeConfig> getApplicationServiceConfig(@Nonnull final TopologyEntityDTO entity) {
        Optional<Map<String, String>> entityPropertyMap = getEntityPropertyMap(entity);
        if (entityPropertyMap.isPresent()
                && APPLICATION_SERVICE_ALLOWED_OS.contains(entityPropertyMap.get().get(OS_TYPE))) {
            final OSType aspOsType = OSType.valueOf(entityPropertyMap.get().get(OS_TYPE));
            return Optional.of(
                    new ComputeConfig(aspOsType, Tenancy.DEFAULT, null, 0,
                            null, null));
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<Map<String, String>> getEntityPropertyMap(@Nonnull final TopologyEntityDTO entityDTO) {
        return Optional.of(entityDTO.getEntityPropertyMapMap());
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
                .numCores(tierInfo.getNumOfCores())
                .numCoupons(tierInfo.getNumCoupons())
                .isBurstableCPU(tierInfo.getBurstableCPU())
                .build());
    }

    @Override
    @Nonnull
    public Optional<DatabaseTierConfig> getDatabaseTierConfig(@Nonnull final TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.DATABASE_TIER_VALUE) {
            return Optional.empty();
        }
        final DatabaseTierInfo tierInfo = entity.getTypeSpecificInfo().getDatabaseTier();
        return Optional.of(DatabaseTierConfig.builder()
                .databaseTierOid(entity.getOid())
                .edition(tierInfo.getEdition())
                .family(tierInfo.getFamily())
                .build());
    }

    @Nonnull
    @Override
    public Optional<Map<CommodityType, Double>> getComputeTierPricingCommodities(
            @Nonnull TopologyEntityDTO entity) {
        return TopologyEntityInfoExtractor.getComputeTierPricingCommoditiesStatic(entity);
    }

    /**
     * Get pricing commodities for an entity based on its compute tier.
     *
     * @param entity compute tier
     * @return the commodities if passed entity is of type computetier.
     */
    @Nonnull
    public static Optional<Map<CommodityType, Double>> getComputeTierPricingCommoditiesStatic(
            @Nonnull TopologyEntityDTO entity) {
        if (entity.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            return Optional.empty();
        }
        Map<CommodityType, Double> computeTierPricingCommodities = new HashMap<>();
        final ComputeTierInfo tierInfo = entity.getTypeSpecificInfo().getComputeTier();
        computeTierPricingCommodities.put(CommodityType.NUM_VCORE,
                Double.valueOf(tierInfo.getNumOfCores()));
        Optional<Float> memProvisioned = getRawCommodityCapacity(entity, CommodityType.MEM_PROVISIONED);
        if (memProvisioned.isPresent()) {
            computeTierPricingCommodities.put(CommodityType.MEM_PROVISIONED,
                    memProvisioned.get().doubleValue());
        } else {
            logger.warn("Could not find the mem provisioned commodity for tier {}", entity.getDisplayName());
        }
        return  Optional.of(computeTierPricingCommodities);
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
                DatabaseConfig databaseConfig = new DatabaseConfig(dbConfig.getEdition(),
                        dbConfig.getEngine(),
                        dbConfig.hasLicenseModel() ? dbConfig.getLicenseModel() : null,
                        dbConfig.hasDeploymentType() ? dbConfig.getDeploymentType() : null,
                        dbConfig.hasHourlyBilledOps() ? dbConfig.getHourlyBilledOps() : null);
                if (dbConfig.hasHaReplicaCount()) {
                    databaseConfig.setHaReplicaCount((int)dbConfig.getHaReplicaCount());
                }
                return Optional.of(databaseConfig);
            }
        }
        return Optional.empty();
    }

    @Override
    @Nonnull
    public Optional<AppServicePlanConfig> getAppServicePlanConfig(TopologyEntityDTO entity) {
        if ((entity.getEntityType() == EntityType.APPLICATION_COMPONENT_VALUE
                || entity.getEntityType() == EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                && entity.hasTypeSpecificInfo()
                && entity.getTypeSpecificInfo().hasApplicationService()) {
            ApplicationServiceInfo appSvcInfo = entity.getTypeSpecificInfo().getApplicationService();
            return Optional.of(new AppServicePlanConfig(appSvcInfo.getCurrentInstanceCount()));
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
