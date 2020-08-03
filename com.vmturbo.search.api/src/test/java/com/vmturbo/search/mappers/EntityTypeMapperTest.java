package com.vmturbo.search.mappers;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.vmturbo.search.metadata.SearchEntityMetadata;

/**
 * Tests {@link EntityTypeMapper} class.
 */
public class EntityTypeMapperTest {

    /**
     * Tests all entities in {@link SearchEntityMetadata} are mapped.
     */
    @Test
    public void allEntityTypesFromMetaDataAvailable() {
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            assertNotNull(searchEntityMetadata.getEntityType().getDisplayName(), EntityTypeMapper.fromApiToSearchSchema(searchEntityMetadata.getEntityType()));
        }
    }
}
