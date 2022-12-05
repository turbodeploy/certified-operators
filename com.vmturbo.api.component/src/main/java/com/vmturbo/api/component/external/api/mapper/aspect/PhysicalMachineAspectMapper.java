package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMDiskAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMDiskGroupAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.AutomationLevel;
import com.vmturbo.api.enums.DiskRoleType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskGroupData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskRole;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Physical Machine aspect mapper.
 **/
public class PhysicalMachineAspectMapper extends AbstractAspectMapper {
    private static final Logger logger = LogManager.getLogger();

    private static final Map<EntityDTO.AutomationLevel, AutomationLevel> PLATFORM_TO_API_AUTOMATION_LEVEL =
        ImmutableMap.of(EntityDTO.AutomationLevel.FULLY_AUTOMATED, AutomationLevel.FULLY_AUTOMATED,
            EntityDTO.AutomationLevel.PARTIALLY_AUTOMATED, AutomationLevel.PARTIALLY_AUTOMATED,
            EntityDTO.AutomationLevel.NOT_AUTOMATED, AutomationLevel.NOT_AUTOMATED,
            EntityDTO.AutomationLevel.DISABLED, AutomationLevel.DISABLED);

    private final RepositoryApi repositoryApi;

    private static final String TOTAL_PHYSICAL_PU = "totalInstalledPhysicalProcessorUnits";
    private static final String LATEST_SUPPORTED_PROCESSOR_GEN = "latestSupportedProcessorGeneration";
    private static final String TOTAL_PHYSICAL_MEM = "totalInstalledPhysicalMemory";
    private static final String LOGICAL_MEM_BLOCK_SIZE = "logicalMemoryBlockSize";
    private static final String SERIAL_NUMBER = "serialNumber";

    public PhysicalMachineAspectMapper(final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    @Nullable
    @Override
    public PMEntityAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        PMEntityAspectApiDTO aspect = new PMEntityAspectApiDTO();
        // the 'processorPools' aspect is set from the displayName of any ProcessorPool entities
        // that are "connected" to the given PM
        final Set<Long> processorPoolOids = entity.getConnectedEntityListList().stream()
            .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                    EntityType.PROCESSOR_POOL_VALUE)
            .map(ConnectedEntity::getConnectedEntityId)
            .collect(Collectors.toSet());
        if (!processorPoolOids.isEmpty()) {
            final List<String> processorPoolDisplayNames = repositoryApi.entitiesRequest(processorPoolOids)
                .getMinimalEntities()
                .map(MinimalEntity::getDisplayName)
                .collect(Collectors.toList());
            aspect.setProcessorPools(processorPoolDisplayNames);
        }

        final TypeSpecificInfo typeInfo = entity.getTypeSpecificInfo();
        final Map<String, String> entityPropertyMap = entity.getEntityPropertyMapMap();

        // Convert disk groups for vSAN host
        try {
            List<PMDiskGroupAspectApiDTO> diskGroups = mapDiskGroups(typeInfo);
            if (!diskGroups.isEmpty()) {
                aspect.setDiskGroups(diskGroups);
            }
        } catch (AspectMapperException e) {
            logger.error("Error converting disk groups for Physical Machine '"
                    + entity.getDisplayName() + "': " + typeInfo);
        }

        if (typeInfo.hasPhysicalMachine()) {
            final PhysicalMachineInfo physicalMachineInfo = typeInfo.getPhysicalMachine();
            if (physicalMachineInfo.hasCpuModel()) {
                aspect.setCpuModel(physicalMachineInfo.getCpuModel());
            }

            // map automation and migration level
            if (physicalMachineInfo.hasAutomationLevel()) {
                final AutomationLevel automationLevel = PLATFORM_TO_API_AUTOMATION_LEVEL.get(
                    physicalMachineInfo.getAutomationLevel());
                if (automationLevel != null) {
                    aspect.setAutomationLevel(automationLevel);
                } else {
                    logger.warn("Unexpected automation level {} for physical machine {}, oid: {}",
                        () -> physicalMachineInfo.getAutomationLevel().name(), entity::getDisplayName,
                        entity::getOid);
                }
            }

            if (physicalMachineInfo.hasMigrationLevel()) {
                aspect.setMigrationLevel(physicalMachineInfo.getMigrationLevel());
            }

            if (physicalMachineInfo.hasProcessorCompatibilityModes()) {
                aspect.setSupportedProcessorCompatibilityModes(physicalMachineInfo
                        .getProcessorCompatibilityModes());
            }
            if (physicalMachineInfo.hasModel()) {
                aspect.setMachineTypeAndModel(physicalMachineInfo.getModel());
            }
        }
        if (entityPropertyMap.containsKey(TOTAL_PHYSICAL_PU)) {
            aspect.setTotalInstalledPhysicalProcessorUnits(
                    Integer.valueOf(entityPropertyMap.get(TOTAL_PHYSICAL_PU)));
        }
        if (entityPropertyMap.containsKey(LATEST_SUPPORTED_PROCESSOR_GEN)) {
            aspect.setLatestSupportedProcessorGeneration(entityPropertyMap.get(LATEST_SUPPORTED_PROCESSOR_GEN));
        }
        if (entityPropertyMap.containsKey(TOTAL_PHYSICAL_MEM)) {
            aspect.setTotalInstalledPhysicalMemory(
                    Integer.valueOf(entityPropertyMap.get(TOTAL_PHYSICAL_MEM)));
        }
        if (entityPropertyMap.containsKey(LOGICAL_MEM_BLOCK_SIZE)) {
            aspect.setLogicalMemoryBlockSize(
                    Integer.valueOf(entityPropertyMap.get(LOGICAL_MEM_BLOCK_SIZE)));
        }
        if (entityPropertyMap.containsKey(SERIAL_NUMBER)) {
            aspect.setSerialNumber(entityPropertyMap.get(SERIAL_NUMBER));
        }

        aspect.setDedicatedFailoverHost(typeInfo.getPhysicalMachine().getDedicatedFailover());

        // map networks
        aspect.setConnectedNetworks(new ArrayList<>(getNetworks(entity)));
        
        return aspect;
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatch(
        @Nonnull List<TopologyEntityDTO> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatchPartial(
        @Nonnull List<ApiPartialEntity> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
    }

    @Nonnull
    private static List<PMDiskGroupAspectApiDTO> mapDiskGroups(@Nonnull TypeSpecificInfo typeInfo)
            throws AspectMapperException {
        List<PMDiskGroupAspectApiDTO> result = new ArrayList<>();
        PhysicalMachineInfo pmInfo = typeInfo.getPhysicalMachine();

        for (DiskGroupData group : pmInfo.getDiskGroupList()) {
            List<PMDiskAspectApiDTO> diskAspects = new ArrayList<>();

            for (DiskData disk : group.getDiskList()) {
                PMDiskAspectApiDTO diskAspect = new PMDiskAspectApiDTO();
                diskAspect.setDiskRole(mapDiskType(disk.getRole()));
                diskAspect.setDiskCapacity(disk.getCapacity());
                diskAspects.add(diskAspect);
            }

            PMDiskGroupAspectApiDTO groupAspect = new PMDiskGroupAspectApiDTO();
            groupAspect.setDisks(diskAspects);
            result.add(groupAspect);
        }

        return result;
    }

    /**
     * Map com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskRole to
     * com.vmturbo.api.enums.DiskRoleType.
     *
     * @param role Platform enum
     * @return API enum
     * @throws AspectMapperException error mapping enums
     */
    @Nonnull
    private static DiskRoleType mapDiskType(@Nonnull DiskRole role) throws AspectMapperException {
        if (role == DiskRole.ROLE_CACHE) {
            return DiskRoleType.CACHE;
        }

        if (role == DiskRole.ROLE_CAPACITY) {
            return DiskRoleType.CAPACITY;
        }

        throw new AspectMapperException("Error converting disk role " + role);
    }

    /**
     * Get names of all network commodities sold by this PM.
     *
     * @param entity PM topology entity
     * @return Collection of sold network commodity names
     */
    private static Collection<String> getNetworks(final TopologyEntityDTO entity) {
        return entity.getCommoditySoldListList().stream()
            .filter(commSold ->
                CommodityType.NETWORK_VALUE == commSold.getCommodityType().getType())
            // not all probes will have display name set for network commodities
            .filter(CommoditySoldDTO::hasDisplayName)
            .map(CommoditySoldDTO::getDisplayName)
            .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.PHYSICAL_MACHINE;
    }
}
