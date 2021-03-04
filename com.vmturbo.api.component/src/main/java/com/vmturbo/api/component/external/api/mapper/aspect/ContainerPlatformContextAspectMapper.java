package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.ContainerPlatformContextMapper;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class ContainerPlatformContextAspectMapper extends AbstractAspectMapper {

    private final ContainerPlatformContextMapper containerPlatformContextMapper;

    /**
     * Constructor for the ContainerPlatformContextAspectMapper.
     *
     * @param supplyChainRpcService Supply chain search service
     * @param repositoryRpcService  Repository search service
     * @param realtimeTopologyContextId The real time topology context id.
     *                                 Note: This only permits the lookup of aspects on the
     *                                 realtime topology and not plan entities.
     */
    public ContainerPlatformContextAspectMapper(@Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                          @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                                          @Nonnull final Long realtimeTopologyContextId) {
        containerPlatformContextMapper = new ContainerPlatformContextMapper(supplyChainRpcService,
                                                                            repositoryRpcService,
                                                                        realtimeTopologyContextId);
    }

    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatchPartial(@Nonnull List<ApiPartialEntity> entities)
            throws InterruptedException, ConversionException {
        Map<Long, EntityAspect> aspectMap = containerPlatformContextMapper.bulkMapContainerPlatformContext(entities);
        return Optional.of(aspectMap);
    }

    /**
     * Map a list of TopologyEntityDTO objects belonging to Container Platform to the corresponding
     * ContainerPlatformContextAspectApiDTO objects.
     *
     * @param entities a list of TopologyEntityDTO objects belonging to Container Platform.
     *                 Each identified by unique oid.
     * @return A map of oid -> ContainerPlatformContextAspectApiDTO objects.
     */
    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull final List<TopologyEntityDTO> entities){
        Map<Long, EntityAspect> aspectMap = containerPlatformContextMapper.getContainerPlatformContext(entities);
        return Optional.of(aspectMap);
    }

    /**
     * Map a single {@link TopologyEntityDTO} into one entity aspect object.
     *
     * @param entity the {@link TopologyEntityDTO} to get aspect for
     * @return the entity aspect for the given entity, or null if no aspect for this entity
     */
    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        Optional<Map<Long, EntityAspect>> result = mapEntityToAspectBatch(Arrays.asList(entity));

        if (!result.isPresent()) {
            return result.get().get(entity.getOid());

        }
        return null;
    }

    /**
     * Returns the aspect name that can be used for filtering.
     *
     * @return the name of the aspect
     */
    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.CONTAINER_PLATFORM_CONTEXT;
    }
}
