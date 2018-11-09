package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Topology Extension data related to Virtual Machine.
 **/
public class PhysicalMachineAspectMapper implements IAspectMapper {
    private final SearchServiceBlockingStub searchServiceBlockingStub;

    public PhysicalMachineAspectMapper(final SearchServiceBlockingStub searchServiceBlockingStub) {
        this.searchServiceBlockingStub = searchServiceBlockingStub;
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        PMEntityAspectApiDTO aspect = new PMEntityAspectApiDTO();
        // the 'processorPools' aspect is set from the displayName of any ProcessorPool entities
        // that are "connected" to the given PM
        final List<ConnectedEntity> l = entity.getConnectedEntityListList();
        List<Long> processorPoolOids = entity.getConnectedEntityListList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                        EntityType.PROCESSOR_POOL_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toList());
        if (!processorPoolOids.isEmpty()) {
            final SearchEntitiesResponse processorPoolResponse =
                    searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                            .addAllEntityOid(processorPoolOids).build());
            List<String> processorPoolDisplayNames = processorPoolResponse.getEntitiesList().stream()
                    .map(Entity::getDisplayName)
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
