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
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Test EnumUtils.
 */
public class EnumUtilsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testProtoGroupTypeToDbType() {
        for (GroupType groupType : GroupType.values()) {
            assertThat(EnumUtils.groupTypeFromProtoToDb(groupType), is(notNullValue()));
        }
    }

    @Test
    public void testProtoIntEntityTypeToDbType() {
        // verify that all entity types which we have metadata for are defined in the db entity types enum
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            int protoEntityType = EnumUtils.entityTypeFromApiToProto(
                    searchEntityMetadata.getEntityType()).getNumber();
            assertThat(EnumUtils.entityTypeFromProtoIntToDb(protoEntityType), is(notNullValue()));
        }
    }

    @Test
    public void testProtoEntityStateToDbState() {
        for (EntityState entityState : EntityState.values()) {
            assertThat(EnumUtils.entityStateFromProtoToDb(entityState), is(notNullValue()));
        }
    }

    @Test
    public void testProtoEnvironmentTypeToDbType() {
        for (EnvironmentType environmentType : EnvironmentType.values()) {
            assertThat(EnumUtils.environmentTypeFromProtoToDb(environmentType), is(notNullValue()));
        }
    }

    @Test
    public void testApiCommodityTypeToProtoInt() {
        for (CommodityType commodityType : CommodityType.values()) {
            assertThat(EnumUtils.commodityTypeFromApiToProtoInt(commodityType), is(notNullValue()));
        }
        // verify all commodities defined in metadata have correct mapping
        Arrays.stream(SearchEntityMetadata.values())
                .map(searchEntityMetadata -> searchEntityMetadata.getMetadataMappingMap().values())
                .flatMap(Collection::stream)
                .filter(m -> m.getCommodityType() != null)
                .map(SearchMetadataMapping::getCommodityType)
                .forEach(apiCommodityType ->
                        assertThat(EnumUtils.commodityTypeFromApiToProtoInt(apiCommodityType), is(notNullValue())));
    }

    @Test
    public void testApiGroupTypeToProtoType() {
        for (com.vmturbo.api.enums.GroupType groupType : com.vmturbo.api.enums.GroupType.values()) {
            assertThat(EnumUtils.groupTypeFromApiToProto(groupType), is(notNullValue()));
        }
        // verify all grouptype defined in metadata have correct mapping
        Arrays.stream(SearchEntityMetadata.values())
                .map(searchEntityMetadata -> searchEntityMetadata.getMetadataMappingMap().values())
                .flatMap(Collection::stream)
                .filter(m -> m.getRelatedGroupType() != null)
                .map(SearchMetadataMapping::getRelatedGroupType)
                .forEach(groupType ->
                        assertThat(EnumUtils.groupTypeFromApiToProto(groupType), is(notNullValue())));
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
        assertThat(EnumUtils.entityTypeFromApiToProto(apiEntityType), is(notNullValue()));
    }
}
