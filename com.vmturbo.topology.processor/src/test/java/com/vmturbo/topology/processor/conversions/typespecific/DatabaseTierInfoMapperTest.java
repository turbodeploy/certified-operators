package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Test class for  DatabaseTierInfoMapper.
 */
public class DatabaseTierInfoMapperTest {


    private static final String FAMILY = "Standard";
    private static final String EDITION = "Standard_S2";

    /**
     * Test to verify correct extraction of type info.
     */
    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder databaseTierEntityDTO = createEntityDTOBuilder();
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setDatabaseTier(DatabaseTierInfo.newBuilder()
                        .setFamily(FAMILY)
                        .setEdition(EDITION)
                        .build())
                .build();
        final DatabaseTierInfoMapper testBuilder = new DatabaseTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(databaseTierEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }

    private EntityDTOOrBuilder createEntityDTOBuilder() {
        return EntityDTO.newBuilder()
                .setDatabaseTierData(DatabaseTierData.newBuilder()
                        .setFamily(FAMILY)
                        .setEdition(EDITION)
                        .build());
    }
}