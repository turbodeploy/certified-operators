package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class StorageInfoMapperTest {


    private static final String EXTERNAL_NAME_1 = "EXT name 1";
    private static final String EXTERNAL_NAME_2 = "EXT name 2";
    private static final List<String> EXTERNAL_NAME_LIST = Lists.newArrayList(
            EXTERNAL_NAME_1,
            EXTERNAL_NAME_2
    );

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder storageEntityDTO = EntityDTO.newBuilder()
                .setStorageData(StorageData.newBuilder()
                        .addAllExternalName(EXTERNAL_NAME_LIST)
                        .setStorageType(StorageType.FIBER_CHANNEL)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setStorage(StorageInfo.newBuilder()
                        .addAllExternalName(EXTERNAL_NAME_LIST)
                        .setStorageType(StorageType.FIBER_CHANNEL)
                        .setIsLocal(false)
                        .build())
                .build();
        final StorageInfoMapper testBuilder = new StorageInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(storageEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}