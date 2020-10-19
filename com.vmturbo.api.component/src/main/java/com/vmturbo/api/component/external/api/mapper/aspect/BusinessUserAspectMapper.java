package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.Nullable;

import com.vmturbo.api.dto.entityaspect.BusinessUserEntityAspectApiDTO;
import com.vmturbo.api.dto.user.BusinessUserSessionApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;

/**
 * Mapper for getting {@link com.vmturbo.api.dto.entityaspect.BusinessUserEntityAspectApiDTO}.
 */
public class BusinessUserAspectMapper extends AbstractAspectMapper {
    @Nullable
    @Override
    public BusinessUserEntityAspectApiDTO mapEntityToAspect(@Nonnull TopologyDTO.TopologyEntityDTO entity) {
        final BusinessUserEntityAspectApiDTO aspect = new BusinessUserEntityAspectApiDTO();
        final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
        if (typeSpecificInfo.hasBusinessUser()) {
            final List<BusinessUserSessionApiDTO> sessions = new LinkedList<>();
            typeSpecificInfo.getBusinessUser().getVmOidToSessionDurationMap().forEach(
                    (key, value) -> {
                        final BusinessUserSessionApiDTO dto = new BusinessUserSessionApiDTO();
                        dto.setBusinessUserUuid(String.valueOf(entity.getOid()));
                        dto.setConnectedEntityUuid(String.valueOf(key));
                        dto.setDuration(value);
                        sessions.add(dto);
                    });
            aspect.setSessions(sessions);
        }
        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.BUSINESS_USER;
    }
}
