package com.vmturbo.search.metadata;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;

/**
 * Unit tests verify that all fields in SearchEntityMetadataMapping for different field types
 * are set as expected. This is needed to ensure correct data ingestion and search query.
 */
public class SearchEntityMetadataTest {

    /**
     * Verifiers for different field types.
     */
    private static final Map<FieldType, MetadataVerifier> METADATA_VERIFIERS =
            new ImmutableMap.Builder<FieldType, MetadataVerifier>()
                    .put(FieldType.COMMODITY, new CommodityMetadataVerifier())
                    .put(FieldType.PRIMITIVE, new PrimitiveMetadataVerifier())
                    .put(FieldType.RELATED_ACTION, new RelatedActionMetadataVerifier())
                    .put(FieldType.RELATED_ENTITY, new RelatedEntityMetadataVerifier())
                    .put(FieldType.RELATED_GROUP, new RelatedGroupMetadataVerifier())
                    .build();

    /**
     * Check tha all fields in SearchEntityMetadataMapping are set as expected.
     */
    @Test
    public void testMetadataFieldsSetCorrectly() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            for (Map.Entry<FieldApiDTO, SearchEntityMetadataMapping> entry
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

    @FunctionalInterface
    private interface MetadataVerifier {

        void verify(SearchEntityMetadataMapping metadata);

        default void commonVerify(SearchEntityMetadataMapping metadata) {
            assertNotNull(metadata.getColumnName());
            assertNotNull(metadata.getSearchEntityFieldDataType());
            if (metadata.getSearchEntityFieldDataType() == SearchEntityFieldDataType.ENUM) {
                assertNotNull(metadata.getEnumClass());
            } else {
                assertNull(metadata.getEnumClass());
            }
        }
    }

    public static class CommodityMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchEntityMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getCommodityType());
            assertNotNull(metadata.getCommodityAttribute());
            assertNotNull(metadata.getCommodityUnit());
        }
    }

    public static class PrimitiveMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchEntityMetadataMapping metadata) {
            commonVerify(metadata);
            if (metadata == SearchEntityMetadataMapping.PRIMITIVE_SEVERITY) {
                assertNull(metadata.getTopoFieldFunction());
            } else {
                assertNotNull(metadata.getTopoFieldFunction());
            }
        }
    }

    public static class RelatedActionMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchEntityMetadataMapping metadata) {
            commonVerify(metadata);
            assertNull(metadata.getJsonKeyName());
        }
    }

    public static class RelatedEntityMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchEntityMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getRelatedEntityTypes());
            assertNotNull(metadata.getRelatedEntityProperty());
        }
    }

    public static class RelatedGroupMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchEntityMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
        }
    }
}
