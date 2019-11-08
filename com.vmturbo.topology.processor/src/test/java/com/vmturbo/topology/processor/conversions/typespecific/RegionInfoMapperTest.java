package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.RegionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.GeoData;

/**
 * RegionInfoMapperTest testing class for region info mapper.
 */
public class RegionInfoMapperTest {

    private static final String REGION_DISCOVERED_UUID = "aws::us-east-1::DC::us-east-1";

    /**
     * Create an entity dto builder.
     *
     * @return EntityDTOOrBuilder as region for testing
     */
    private EntityDTOOrBuilder createEntityDTOBuilder() {
        return EntityDTO.newBuilder()
                .setId(REGION_DISCOVERED_UUID)
                .setEntityType(EntityType.REGION)
                .setRegionData(RegionData.newBuilder()
                        .setGeoData(GeoData.newBuilder()
                                .setLatitude(10.0)
                                .setLongitude(10.0)
                                .build()
                        )
                        .build()
                )
                .build();
    }

    /**
     * Create type specific information.
     *
     * @return TypeSpecificInfo as region type specific info for testing
     */
    private TypeSpecificInfo createTypeSpecificInfo() {
        return TypeSpecificInfo.newBuilder()
                .setRegion(RegionInfo.newBuilder()
                        .setGeoData(
                                GeoDataInfo.newBuilder()
                                .setLatitude(10.0)
                                .setLongitude(10.0)
                        )
                )
                .build();
    }

    /**
     * Test if mapping entity dto to type specific info works.
     */
    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder regionEntityDTO = createEntityDTOBuilder();
        TypeSpecificInfo expected = createTypeSpecificInfo();
        final RegionInfoMapper testBuilder = new RegionInfoMapper();

        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(regionEntityDTO,
                Collections.emptyMap());

        // assert
        assertThat(result, equalTo(expected));
    }
}