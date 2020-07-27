package com.vmturbo.search.mappers;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;

/**
 * Tests {@link TypeMapper} class.
 */
public class TypeMapperTest {

    /**
     * Test mapping {@link EntityType} to {@link com.vmturbo.extractor.schema.enums.EntityType}.
     */
    @Test
    public void testApiEntityTypeMappedCorrectly() {
        //GIVEN
        EntityType vm = EntityType.VirtualMachine;

        //WHEN
        com.vmturbo.extractor.schema.enums.EntityType type = TypeMapper.fromApiToSearchSchema(vm);

        //THEN
        assertEquals(type, com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE);
    }

    /**
     * Test mapping {@link GroupType} to {@link com.vmturbo.extractor.schema.enums.EntityType}.
     */
    @Test
    public void testApiGroupTypeMappedCorrectly() {
        //GIVEN
        GroupType cluster = GroupType.Cluster;

        //WHEN
        com.vmturbo.extractor.schema.enums.EntityType type = TypeMapper.fromApiToSearchSchema(cluster);

        //THEN
        assertEquals(type, com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER);
    }

    /**
     * Test mapping {@link com.vmturbo.extractor.schema.enums.EntityType} to {@link EntityType}.
     */
    @Test
    public void testJooqEnumMappedCorrectlyToEntityType() {
        //GIVEN
        com.vmturbo.extractor.schema.enums.EntityType type =
                com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE;

        //WHEN
        Enum<?> cluster = TypeMapper.fromSearchSchemaToApi(type);

        //THEN
        assertEquals(cluster, EntityType.VirtualMachine );
    }

    /**
     * Test mapping {@link com.vmturbo.extractor.schema.enums.EntityType} to {@link GroupType}.
     */
    @Test
    public void testJooqEnumMappedCorrectlyToGroupType() {
        //GIVEN
        com.vmturbo.extractor.schema.enums.EntityType type =
                com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER;

        //WHEN
        Enum<?> cluster = TypeMapper.fromSearchSchemaToApi(type);

        //THEN
        assertEquals(cluster, GroupType.Cluster );
    }

    /**
     * Test mapping {@link EntityType} to {@link com.vmturbo.extractor.schema.enums.EntityType}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedEntityTypeThrowsIllegalArgumentError() {
        EntityType vm = EntityType.Internet; //Don't ever expect internet to be search db
        TypeMapper.fromApiToSearchSchema(vm);
    }

}
