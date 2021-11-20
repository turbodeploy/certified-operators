package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.ComputeTierAspectApiDTO;
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
    public ComputeTierAspectApiDTO mapEntityToAspect(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        // this aspect only applies to compute tiers
        if (entity.getEntityType() != CommonDTO.EntityDTO.EntityType.COMPUTE_TIER_VALUE) {
            return null;
        }
        ComputeTierAspectApiDTO computeTierAspectApiDTO = new ComputeTierAspectApiDTO();
        TopologyDTO.TypeSpecificInfo.ComputeTierInfo computeTier = entity.getTypeSpecificInfo().getComputeTier();
        computeTierAspectApiDTO.setTierFamily(computeTier.getFamily());
        // This should continue to work as before for AWS instance tier, but for GCP which supports
        // multiple combinations of local SSDs (e.g 2, 4, 16), we keep the max allowed count (16).
        // This to avoid breaking backward compatibility with /entities/{entity_Uuid}/aspects API.

        // How many disk size options are available for this tier, e.g 3 for above case.
        int diskOptions = computeTier.getInstanceDiskCountsCount();
        // Largest disk count size, e.g 16 for above case. Counts are sorted ascending count order.
        int diskCounts = diskOptions > 0 ? computeTier.getInstanceDiskCounts(diskOptions - 1) : 0;
        computeTierAspectApiDTO.setNumInstanceStorages((float)diskCounts);
        computeTierAspectApiDTO.setInstanceStorageSize((float)computeTier.getInstanceDiskSizeGb());
        return computeTierAspectApiDTO;
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.COMPUTE_TIER;
    }
}
