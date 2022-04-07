package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.WorkloadControllerAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;

/**
 * Topology Extension data related to workload controller.
 **/
public class WorkloadControllerAspectMapper extends AbstractAspectMapper {

    /**
     * Map a list of TopologyEntityDTO objects (of type WorkloadController) to the corresponding
     * WorkloadControllerAspectApiDTO objects.
     *
     * @param entities a list of TopologyEntityDTO objects of type WorkloadController.
     *                 Each identified by unique oid.
     * @return A map of oid -> WorkloadControllerAspectApiDTO objects.
     */
    @Override
    @Nonnull
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull final List<TopologyEntityDTO> entities) {
        Map<Long, EntityAspect> response = new HashMap<>();

        entities.forEach(entity -> {
            final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
            if (typeSpecificInfo != null && typeSpecificInfo.hasWorkloadController()) {
                response.put(entity.getOid(), mapEntityToAspect(entity));
            }
        });

        return Optional.of(response);
    }

    @Override
    public WorkloadControllerAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final WorkloadControllerAspectApiDTO aspect = new WorkloadControllerAspectApiDTO();
        if (!entity.getTypeSpecificInfo().hasWorkloadController()) {
            return aspect;
        }
        final WorkloadControllerInfo wcInfo = entity.getTypeSpecificInfo().getWorkloadController();
        aspect.setControllerType(wcInfo.getControllerTypeCase().name());
        if (wcInfo.hasCustomControllerInfo()) {
            aspect.setCustomControllerType(wcInfo.getCustomControllerInfo().getCustomControllerType());
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.WORKLOAD_CONTROLLER;
    }
}
