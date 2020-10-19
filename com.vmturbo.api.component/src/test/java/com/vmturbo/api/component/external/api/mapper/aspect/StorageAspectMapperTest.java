package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;

public class StorageAspectMapperTest extends BaseAspectMapperTest {

    private static final String TEST_EXTERNAL_NAME = "TEST_NAME";
    private static final StorageType TEST_STORAGE_TYPE = StorageType.CIFS_SMB;

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.STORAGE,
            TypeSpecificInfo.newBuilder()
                .setStorage(StorageInfo.newBuilder()
                    .setStorageType(TEST_STORAGE_TYPE)
                    .addExternalName(TEST_EXTERNAL_NAME))
                .build());

        StorageAspectMapper testMapper = new StorageAspectMapper();
        // act
        final STEntityAspectApiDTO storageAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertEquals(Lists.newArrayList(TEST_EXTERNAL_NAME), storageAspect.getExternalNames());
        assertEquals(TEST_DISPLAY_NAME, storageAspect.getDisplayName());
    }
}