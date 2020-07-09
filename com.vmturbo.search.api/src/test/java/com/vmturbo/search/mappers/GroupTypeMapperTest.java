package com.vmturbo.search.mappers;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.vmturbo.search.metadata.SearchGroupMetadata;

/**
 * Tests {@link GroupTypeMapper} class.
 */
public class GroupTypeMapperTest {

    /**
     * Tests all groupTypes in {@link SearchGroupMetadata} are mapped.
     */
    @Test
    public void allEntityTypesFromMetaDataAvailable() {
        for (SearchGroupMetadata searchGroupMetadata : SearchGroupMetadata.values()) {
            assertNotNull(GroupTypeMapper.fromApiToSearchSchema(searchGroupMetadata.getGroupType()));
        }
    }
}
