package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMDiskAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMDiskGroupAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.DiskRoleType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskGroupData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskRole;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Physical Machine aspect mapper.
 **/
public class PhysicalMachineAspectMapper extends AbstractAspectMapper {
    private static final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

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

        TypeSpecificInfo typeInfo = entity.getTypeSpecificInfo();

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

        if (typeInfo != null && typeInfo.hasPhysicalMachine()) {
            PhysicalMachineInfo physicalMachineInfo = typeInfo.getPhysicalMachine();
            if (physicalMachineInfo.hasCpuModel()) {
                aspect.setCpuModel(physicalMachineInfo.getCpuModel());
            }
        }

        aspect.setDedicatedFailoverHost(typeInfo.getPhysicalMachine().getDedicatedFailover());
        return aspect;
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

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.PHYSICAL_MACHINE;
    }
}
