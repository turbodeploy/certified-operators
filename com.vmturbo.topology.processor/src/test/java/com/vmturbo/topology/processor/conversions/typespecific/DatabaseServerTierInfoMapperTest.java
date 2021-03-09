package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Test class for DatabaseServerTierInfoMapper.
 */
public class DatabaseServerTierInfoMapperTest {


    private static final String FAMILY = "db.m1";

    /**
     * Test to verify correct extraction of type info.
     */
    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder databaseServerTierEntityDTO = createEntityDTOBuilder();
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setDatabaseServerTier(DatabaseServerTierInfo.newBuilder()
                        .setFamily(FAMILY)
                        .build())
                .build();
        final DatabaseServerTierInfoMapper testBuilder = new DatabaseServerTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(databaseServerTierEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }

    private EntityDTOOrBuilder createEntityDTOBuilder() {
        return EntityDTO.newBuilder()
            .setDatabaseServerTierData(DatabaseServerTierData.newBuilder()
                .setFamily(FAMILY)
                .build());
    }
}