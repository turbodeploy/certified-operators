package com.vmturbo.extractor.search;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Test EnumUtils.
 */
public class EnumUtilsTest {

    /** Support for tests that should throw. */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test that we can convert protobuf group types to DB group types.
     */
    @Test
    public void testProtoGroupTypeToDbType() {
        for (GroupType groupType : GroupType.values()) {
            assertThat(GroupTypeUtils.protoToDb(groupType), is(notNullValue()));
        }
    }

    /**
     * Test that we can convert Protobuf entity types to DB entity types.
     */
    @Test
    public void testProtoIntEntityTypeToDbType() {
        // verify that all entity types which we have metadata for are defined in the db entity types enum
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            int protoEntityType = SearchEntityTypeUtils.apiToProto(
                    searchEntityMetadata.getEntityType()).getNumber();
            assertThat(SearchEntityTypeUtils.protoIntToDb(protoEntityType), is(notNullValue()));
        }
    }

    /**
     * Test that we can convert Protobuf entity states to DB entity states.
     */
    @Test
    public void testProtoEntityStateToDbState() {
        for (EntityState entityState : EntityState.values()) {
            assertThat(EntityStateUtils.protoToDb(entityState), is(notNullValue()));
        }
    }

    /**
     * Test that we can convert Protobuf environment types to DB environment types.
     */
    @Test
    public void testProtoEnvironmentTypeToDbType() {
        for (EnvironmentType environmentType : EnvironmentType.values()) {
            assertThat(EnvironmentTypeUtils.protoToDb(environmentType), is(notNullValue()));
        }
    }

    /**
     * Test that we can convert API commodity types to Protobuf commodity types.
     */
    @Test
    public void testApiCommodityTypeToProto() {
        for (CommodityType commodityType : CommodityType.values()) {
            assertThat(CommodityTypeUtils.apiToProto(commodityType).getNumber(), is(notNullValue()));
        }
        // verify all commodities defined in metadata have correct mapping
        Arrays.stream(SearchEntityMetadata.values())
                .map(searchEntityMetadata -> searchEntityMetadata.getMetadataMappingMap().values())
                .flatMap(Collection::stream)
                .filter(m -> m.getCommodityType() != null)
                .map(SearchMetadataMapping::getCommodityType)
                .forEach(apiCommodityType ->
                        assertThat(CommodityTypeUtils.apiToProto(apiCommodityType).getNumber(),
                                is(notNullValue())));
    }

    /**
     * Test that we can convert API group types to Protobuf group types.
     */
    @Test
    public void testApiGroupTypeToProtoType() {
        for (com.vmturbo.api.enums.GroupType groupType : com.vmturbo.api.enums.GroupType.values()) {
            assertThat(GroupTypeUtils.apiToProto(groupType), is(notNullValue()));
        }
        // verify all grouptype defined in metadata have correct mapping
        Arrays.stream(SearchEntityMetadata.values())
                .map(searchEntityMetadata -> searchEntityMetadata.getMetadataMappingMap().values())
                .flatMap(Collection::stream)
                .filter(m -> m.getRelatedGroupType() != null)
                .map(SearchMetadataMapping::getRelatedGroupType)
                .forEach(groupType ->
                        assertThat(GroupTypeUtils.apiToProto(groupType), is(notNullValue())));
    }

    /**
     * Test that all api {@link EntityType}s involved in {@link SearchMetadataMapping} have
     * correct mapping to proto {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}
     * in EnumUtils.
     */
    @Test
    public void testApiEntityTypeToProtoType() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            // mapping for this entity type
            verifyApiEntityTypeToProto(searchEntityMetadata.getEntityType());
            // group member type
            searchEntityMetadata.getMetadataMappingMap().values().forEach(mapping -> {
               if (mapping.getMemberType() != null) {
                   verifyApiEntityTypeToProto(mapping.getMemberType());
               }
               if (mapping.getRelatedEntityTypes() != null) {
                   mapping.getRelatedEntityTypes().forEach(this::verifyApiEntityTypeToProto);
               }
            });
        }
    }

    /**
     * Verify fromApiEntityTypeToProto for the given entity type.
     *
     * @param apiEntityType api entity type
     */
    private void verifyApiEntityTypeToProto(EntityType apiEntityType) {
        assertThat(SearchEntityTypeUtils.apiToProto(apiEntityType), is(notNullValue()));
    }
}
