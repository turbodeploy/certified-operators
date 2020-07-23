package com.vmturbo.search.fields;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.vmturbo.api.dto.searchquery.EntityMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueTypeApiDTO;
import com.vmturbo.api.dto.searchquery.GroupMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchGroupMetadata;

/**
 * Tests MetadataFieldsQuery.
 */
public class MetadataFieldsQueryTests {

    /**
     * Test processing request of {@link EntityType}.
     */
    @Test
    public void testProcessingEntityRequest() {
        //GIVEN
        EntityType entityType = EntityType.VIRTUAL_MACHINE;
        EntityMetadataRequestApiDTO request = EntityMetadataRequestApiDTO.entityMetaDataRequest(entityType);

        //When
        List<FieldValueTypeApiDTO> results = MetadataFieldsQuery.processRequest(request);

        //Then
        assertNotNull(results);
        assertEquals(results.size(), SearchEntityMetadata.VIRTUAL_MACHINE.getMetadataMappingMap().size());
        results.stream().forEach(fieldValueTypeApiDTO -> {
            FieldApiDTO fieldApiDTO = fieldValueTypeApiDTO.getField();
            if (fieldApiDTO.equals(PrimitiveFieldApiDTO.entityType())) {
                assertTrue("Allowable Values populated for enums", !fieldValueTypeApiDTO.getAllowableValues().isEmpty());
                assertEquals(fieldValueTypeApiDTO.getAllowableValues().size(), SearchEntityMetadata.values().length);
            }
        });
    }

    /**
     * Test thrown exception of unsupported {@link EntityType}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedEntityRequestThrowsException() {
        //GIVEN
        EntityType entityType = EntityType.INTERNET;
        EntityMetadataRequestApiDTO request = EntityMetadataRequestApiDTO.entityMetaDataRequest(entityType);

        //When
        MetadataFieldsQuery.processRequest(request);
    }

    /**
     * Test processing request of {@link GroupType}.
     */
    @Test
    public void testProcessingGroupRequest() {
        //GIVEN
        GroupType groupType = GroupType.STORAGE_CLUSTER;
        GroupMetadataRequestApiDTO request = GroupMetadataRequestApiDTO.groupMetadataRequest(groupType);

        //When
        List<FieldValueTypeApiDTO> results = MetadataFieldsQuery.processRequest(request);

        //Then
        assertNotNull(results);
        assertEquals(results.size(), SearchGroupMetadata.STORAGE_CLUSTER.getMetadataMappingMap().size());
        results.stream().forEach(fieldValueTypeApiDTO -> {
            FieldApiDTO fieldApiDTO = fieldValueTypeApiDTO.getField();
            if (fieldApiDTO.equals(PrimitiveFieldApiDTO.entityType())) {
                assertTrue("Allowable Values populated for enums", !fieldValueTypeApiDTO.getAllowableValues().isEmpty());
                assertEquals(fieldValueTypeApiDTO.getAllowableValues().size(), SearchGroupMetadata.values().length);
            }
        });
    }
}
