package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Topology Extension data related to Virtual Machine.
 **/
public class PhysicalMachineAspectMapper implements IAspectMapper {
    private final RepositoryApi repositoryApi;

    public PhysicalMachineAspectMapper(final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final PMEntityAspectApiDTO aspect = new PMEntityAspectApiDTO();
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
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "physicalMachineAspect";
    }
}
