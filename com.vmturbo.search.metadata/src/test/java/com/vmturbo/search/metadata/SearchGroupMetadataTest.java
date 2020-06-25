package com.vmturbo.search.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.enums.GroupType;

/**
 * Unit tests verify that all fields in SearchEntityMetadataMapping for different field types
 * are set as expected. This is needed to ensure correct data ingestion and search query.
 */
public class SearchGroupMetadataTest {

    /**
     * Verifiers for different field types.
     */
    private static final Map<FieldType, MetadataVerifier> METADATA_VERIFIERS =
            new ImmutableMap.Builder<FieldType, MetadataVerifier>()
                    .put(FieldType.PRIMITIVE, new PrimitiveMetadataVerifier())
                    .put(FieldType.AGGREGATE_COMMODITY, new CommodityMetadataVerifier())
                    .put(FieldType.RELATED_ACTION, new RelatedActionMetadataVerifier())
                    .put(FieldType.MEMBER, new MemberMetadataVerifier())
                    .build();

    /**
     * Check tha all fields in SearchEntityMetadataMapping are set as expected.
     */
    @Test
    public void testMetadataFieldsSetCorrectly() {
        for (SearchGroupMetadata searchEntityMetadata : SearchGroupMetadata.values()) {
            for (Map.Entry<FieldApiDTO, SearchMetadataMapping> entry
                    : searchEntityMetadata.getMetadataMappingMap().entrySet()) {
                MetadataVerifier metadataVerifier =
                        METADATA_VERIFIERS.get(entry.getKey().getFieldType());
                // verify that MetadataVerifier for this filed type exists
                assertNotNull("Metadata verifier for field type " +
                        entry.getKey().getFieldType() + " is not provided!", metadataVerifier);
                // verify the metadata is correct
                metadataVerifier.verify(entry.getValue());
            }
        }
    }

    /**
     * Test that metadata list contains the mandatory fields which are defined as non null in
     * database table.
     */
    @Test
    public void testMetadataFieldsContainMandatoryFields() {
        for (SearchGroupMetadata searchGroupMetadata : SearchGroupMetadata.values()) {
            Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap =
                    searchGroupMetadata.getMetadataMappingMap();
            //todo: use jooq to find all nonnull table columns dynamically and check
            assertEquals("oid", metadataMappingMap.get(PrimitiveFieldApiDTO.oid()).getColumnName());
            assertEquals("name", metadataMappingMap.get(PrimitiveFieldApiDTO.name()).getColumnName());
            assertEquals("type", metadataMappingMap.get(PrimitiveFieldApiDTO.groupType()).getColumnName());
        }
    }

    /**
     * Enum names must match {@link GroupType} names
     */
    @Test
    public void testMetadataEnumMatchesEntityTypeEnumName() {
        for (SearchGroupMetadata searchGroupMetadata : SearchGroupMetadata.values()) {
            assertEquals(searchGroupMetadata.name(), searchGroupMetadata.getGroupType().name());
        }
    }

    @FunctionalInterface
    private interface MetadataVerifier {

        void verify(SearchMetadataMapping metadata);

        default void commonVerify(SearchMetadataMapping metadata) {
            assertNotNull(metadata.getColumnName());
            assertNotNull(metadata.getApiDatatype());
            if (metadata.getApiDatatype() == Type.ENUM) {
                assertNotNull(metadata.getEnumClass());
            } else {
                assertNull(metadata.getEnumClass());
            }
        }
    }

    public static class CommodityMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getMemberType());
            assertNotNull(metadata.getCommodityType());
            assertNotNull(metadata.getCommodityAttribute());
            assertNotNull(metadata.getCommodityAggregation());
            assertNotNull(metadata.getCommodityUnit());
        }
    }

    public static class PrimitiveMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            if (metadata == SearchMetadataMapping.PRIMITIVE_SEVERITY) {
                assertNull(metadata.getGroupFieldFunction());
            } else {
                assertNotNull(metadata.getGroupFieldFunction());
            }
        }
    }

    public static class RelatedActionMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNull(metadata.getJsonKeyName());
        }
    }

    public static class MemberMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNull(metadata.getGroupFieldFunction());
        }
    }
}
