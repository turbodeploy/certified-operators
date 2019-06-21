package com.vmturbo.api.component.external.api.mapper.aspect;

import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Capture base methods for testing Aspect Mappers
 **/
public class BaseAspectMapperTest {

    protected static final long TEST_OID = 123L;
    protected static final String TEST_DISPLAY_NAME = "TEST_DISPLAY_NAME";

    protected TopologyEntityDTO.Builder topologyEntityDTOBuilder(final EntityType entityType,
        final TypeSpecificInfo typeSpecificInfo) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(entityType.getNumber())
            .setOid(TEST_OID)
            .setDisplayName(TEST_DISPLAY_NAME)
            .setTypeSpecificInfo(typeSpecificInfo);
    }

    protected ApiPartialEntity.Builder apiEntityBuilder(final EntityType entityType) {
        return ApiPartialEntity.newBuilder()
            .setEntityType(entityType.getNumber())
            .setOid(TEST_OID)
            .setDisplayName(TEST_DISPLAY_NAME);
    }
}
