package com.vmturbo.search.metadata;

import static com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO.primitive;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.search.metadata.utils.SearchFiltersMapper.SearchFilterSpec;

/**
 * Unit tests verify that all fields in {@link SearchEntityMetadata} for different field types
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
     * Check that all fields in {@link SearchMetadataMapping} are set as expected.
     */
    @Test
    public void testMetadataFieldsSetCorrectly() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
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
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap =
                    searchEntityMetadata.getMetadataMappingMap();
            assertEquals("oid", metadataMappingMap.get(PrimitiveFieldApiDTO.oid()).getColumnName());
            assertEquals("name", metadataMappingMap.get(PrimitiveFieldApiDTO.name()).getColumnName());
            assertEquals("type", metadataMappingMap.get(PrimitiveFieldApiDTO.entityType()).getColumnName());
        }
    }

    /**
     * Enum names must match {@link EntityType} names
     */
    @Test
    public void testMetadataEnumMatchesEntityTypeEnumName() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            assertEquals(searchEntityMetadata.name(), searchEntityMetadata.getEntityType().name());
        }
    }

    /**
     * For each entity type, the mapping from {@link FieldApiDTO}s to jsonKeyNames should not
     * map more than one {@link FieldApiDTO} object to the same jsonKeyName.
     */
    @Test
    public void testNoDuplicateFieldMappings() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            final Map<FieldApiDTO, SearchMetadataMapping> metadataMapping =
                        searchEntityMetadata.getMetadataMappingMap();

            // calculate the records that map to json key names
            long jsonKeyNamesRecordCount = metadataMapping.values().stream()
                                                .map(SearchMetadataMapping::getJsonKeyName)
                                                .filter(Objects::nonNull)
                                                .count();

            // calculate how many distinct json key names are mapped
            long jsonKeyNamesDistinctCount = metadataMapping.values().stream()
                    .map(SearchMetadataMapping::getJsonKeyName)
                    .filter(Objects::nonNull)
                    .distinct()
                    .count();

            // there should be no duplicate mapping: therefore the jsonKeyNamesCount
            // should equal the size of the map
            Assert.assertEquals(jsonKeyNamesDistinctCount, jsonKeyNamesRecordCount);
        }
    }

    /**
     * Values must be mapped when present.
     */
    @Test
    public void testValuesAreMappeWhenPresent() {
        // Given
        SearchMetadataMapping numCpusMapping =
            SearchEntityMetadata.VirtualMachine.getMetadataMappingMap().get(primitive("numCpus"));
        Function<TopologyEntityDTO, Optional<Object>> mappingFunction =
            numCpusMapping.getTopoFieldFunction();

        TopologyEntityDTO vmWithNumCpus = TopologyEntityDTO.newBuilder()
            .setOid(9839335003L)
            .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(
                    VirtualMachineInfo.newBuilder()
                        .setNumCpus(4)))
            .build();

        // When
        final Optional<Object> maybeNumCpus = mappingFunction.apply(vmWithNumCpus);

        // Assert
        assertTrue(maybeNumCpus.isPresent());
        assertEquals(4, maybeNumCpus.get());
    }

    /**
     * Missing values must not be mapped (e.g. to default values).
     */
    @Test
    public void testMissingValuesAreNotMapped() {
        // Given
        SearchMetadataMapping numCpusMapping =
            SearchEntityMetadata.VirtualMachine.getMetadataMappingMap().get(primitive("numCpus"));
        Function<TopologyEntityDTO, Optional<Object>> mappingFunction =
            numCpusMapping.getTopoFieldFunction();

        TopologyEntityDTO vmWithoutNumCpus = TopologyEntityDTO.newBuilder()
            .setOid(9839335003L)
            .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        // When
        final Optional<Object> maybeNumCpus = mappingFunction.apply(vmWithoutNumCpus);

        // Assert
        assertFalse(maybeNumCpus.isPresent());
    }

    /**
     * Test that {@link SearchFilterSpec} are defined in metadata as expected.
     */
    @Test
    public void testSearchFilterSpecSetCorrectly() {
        for (SearchEntityMetadata metadata : SearchEntityMetadata.values()) {
            metadata.getMetadataMappingMap().forEach((fieldApiDTO, metadataMapping) -> {
                if (fieldApiDTO.getFieldType() == FieldType.RELATED_ENTITY
                        || fieldApiDTO.getFieldType() == FieldType.RELATED_GROUP) {
                    assertNotNull(metadataMapping.getRelatedEntityTypes());
                }
            });
        }
        for (SearchGroupMetadata metadata : SearchGroupMetadata.values()) {
            metadata.getMetadataMappingMap().entrySet().stream()
                    .filter(entry -> entry.getKey().getFieldType() == FieldType.RELATED_ENTITY)
                    .map(Entry::getValue)
                    .forEach(mapping -> {
                        if (mapping != SearchMetadataMapping.RELATED_BUSINESS_ACCOUNT) {
                            assertNotNull(mapping.toString(), mapping.getMemberType());
                        }
                        assertNotNull(mapping.getRelatedEntityTypes());
                    });
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
            }
        }
    }

    public static class CommodityMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getCommodityType());
            assertNotNull(metadata.getCommodityAttribute());
        }
    }

    public static class PrimitiveMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            if (metadata == SearchMetadataMapping.PRIMITIVE_SEVERITY
            || metadata == SearchMetadataMapping.PRIMITIVE_IS_EPHEMERAL_VOLUME
            || metadata == SearchMetadataMapping.PRIMITIVE_IS_ENCRYPTED_VOLUME) {
                assertNull(metadata.getTopoFieldFunction());
            } else {
                assertNotNull(metadata.getTopoFieldFunction());
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

    public static class RelatedEntityMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getRelatedEntityTypes());
            assertNotNull(metadata.getRelatedEntityProperty());
        }
    }

    public static class RelatedGroupMetadataVerifier implements MetadataVerifier {
        @Override
        public void verify(SearchMetadataMapping metadata) {
            commonVerify(metadata);
            assertNotNull(metadata.getJsonKeyName());
            assertNotNull(metadata.getRelatedGroupType());
            assertNotNull(metadata.getRelatedEntityTypes());
            assertNotNull(metadata.getRelatedGroupProperty());
        }
    }
}
