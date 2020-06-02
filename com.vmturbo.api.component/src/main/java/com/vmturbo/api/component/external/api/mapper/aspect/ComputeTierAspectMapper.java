package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.ComputeTierAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Mapper for getting {@link ComputeTierAspectApiDTO}.
 */
public class ComputeTierAspectMapper extends AbstractAspectMapper {
    /**
     * Map entity to compute tier aspect.
     *
     * @param entity the {@link TopologyDTO.TopologyEntityDTO} to get aspect for
     * @return ComputeTierAspectApiDTO
     */
    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        // this aspect only applies to compute tiers
        if (entity.getEntityType() != CommonDTO.EntityDTO.EntityType.COMPUTE_TIER_VALUE) {
            return null;
        }
        ComputeTierAspectApiDTO computeTierAspectApiDTO = new ComputeTierAspectApiDTO();
        TopologyDTO.TypeSpecificInfo.ComputeTierInfo computeTier = entity.getTypeSpecificInfo().getComputeTier();
        computeTierAspectApiDTO.setTierFamily(computeTier.getFamily());
        computeTierAspectApiDTO.setNumInstanceStorages((float)computeTier.getNumInstanceDisks());
        computeTierAspectApiDTO.setInstanceStorageSize((float)computeTier.getInstanceDiskSizeGb());
        return computeTierAspectApiDTO;
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.COMPUTE_TIER;
    }
}
