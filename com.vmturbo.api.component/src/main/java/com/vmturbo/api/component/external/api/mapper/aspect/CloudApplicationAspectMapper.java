package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.CloudApplicationAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudApplicationInfo;

/**
 * Map a topology entity DTO to an application aspect.
 */
public class CloudApplicationAspectMapper extends AbstractAspectMapper {

    /**
     * Maps a topology entity DTO to an application aspect.
     *
     * @param entity the {@link TopologyEntityDTO} to get aspect for
     * @return mapped app component aspect
     */
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        final CloudApplicationAspectApiDTO aspect = new CloudApplicationAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasCloudApplication()) {
            CloudApplicationInfo info =
                    entity.getTypeSpecificInfo().getCloudApplication();
            if (info.hasDeploymentSlotCount()) {
                aspect.setDeploymentSlotCount(info.getDeploymentSlotCount());
            }
            if (info.hasHybridConnectionCount()) {
                aspect.setHybridConnectionCount(info.getHybridConnectionCount());
            }
        }
        return aspect;
    }

    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(
            @Nonnull List<TopologyEntityDTO> entities) {
        List<TopologyEntityDTO> cloudEntities = entities.stream().filter(
                AbstractAspectMapper::isCloudEntity).collect(Collectors.toList());
        Map<Long, EntityAspect> resp = new HashMap<>();
        cloudEntities.forEach(entity -> {
            resp.put(entity.getOid(), mapEntityToAspect(entity));
        });
        return Optional.of(resp);
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.CLOUD_APPLICATION;
    }
}
