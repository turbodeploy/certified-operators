package com.vmturbo.extractor.search;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.EntityTypeMapper;

/**
 * Test EnumUtils. todo: move to metadata package?
 */
public class EnumUtilsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testProtoGroupTypeToDbType() {
        for (GroupType groupType : GroupType.values()) {
            assertThat(EnumUtils.protoGroupTypeToDbType(groupType), is(notNullValue()));
        }
    }

    @Test
    public void testProtoIntEntityTypeToDbType() {
        for (EntityDTO.EntityType entityType : EntityTypeMapper.SUPPORTED_ENTITY_TYPE_MAPPING.values()) {
            assertThat(EnumUtils.protoIntEntityTypeToDbType(entityType.getNumber()), is(notNullValue()));
        }
    }

    @Test
    public void testProtoEntityStateToDbState() {
        for (EntityState entityState : EntityState.values()) {
            assertThat(EnumUtils.protoEntityStateToDbState(entityState), is(notNullValue()));
        }
    }

    @Test
    public void testProtoEnvironmentTypeToDbType() {
        for (EnvironmentType environmentType : EnvironmentType.values()) {
            assertThat(EnumUtils.protoEnvironmentTypeToDbType(environmentType), is(notNullValue()));
        }
    }

    @Test
    public void testApiCommodityTypeToProtoInt() {
        for (CommodityType commodityType : CommodityType.values()) {
            assertThat(EnumUtils.apiCommodityTypeToProtoInt(commodityType), is(notNullValue()));
        }
    }
}
