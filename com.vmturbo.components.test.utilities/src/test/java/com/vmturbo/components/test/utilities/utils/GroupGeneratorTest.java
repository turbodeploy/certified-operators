package com.vmturbo.components.test.utilities.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GroupGeneratorTest {

    private final GroupGenerator groupGen = new GroupGenerator();

    @Test
    public void testEntityWithName() {
        final GroupDefinition groupDefinition =
                groupGen.entityWithName(EntityType.PHYSICAL_MACHINE, "foo");
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE,
                groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getStartingFilter()
                .getNumericFilter()
                .getValue());
        assertEquals("foo", groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getStringPropertyRegex());
    }

    @Test
    public void testStorageWithName() {
        final GroupDefinition groupDefinition = groupGen.entityWithName(EntityType.STORAGE, "foo");
        assertEquals(EntityType.STORAGE_VALUE,
                groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(EntityType.STORAGE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getStartingFilter()
                .getNumericFilter()
                .getValue());
        assertEquals("foo", groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getStringPropertyRegex());
    }

    @Test
    public void testVmsOnHost() {
        final GroupDefinition groupDefinition = groupGen.vmsOnHost("foo");
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getStartingFilter()
                .getNumericFilter()
                .getValue());
        assertEquals("foo", groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getStringPropertyRegex());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(1)
                .getTraversalFilter()
                .getStoppingCondition()
                .getStoppingPropertyFilter()
                .getNumericFilter()
                .getValue());
    }

    @Test
    public void testVmsOnStorage() {
        final GroupDefinition groupDefinition = groupGen.vmsOnStorage("foo");
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(EntityType.STORAGE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getStartingFilter()
                .getNumericFilter()
                .getValue());
        assertEquals("foo", groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getStringPropertyRegex());

        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(1)
                .getTraversalFilter()
                .getStoppingCondition()
                .getStoppingPropertyFilter()
                .getNumericFilter()
                .getValue());
    }

    @Test
    public void testHostsOnDatacenter() {
        final GroupDefinition groupDefinition = groupGen.hostsOnDatacenter("foo");
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE,
                groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(EntityType.DATACENTER_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getStartingFilter()
                .getNumericFilter()
                .getValue());
        assertEquals("foo", groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(0)
                .getPropertyFilter()
                .getStringFilter()
                .getStringPropertyRegex());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupDefinition.getEntityFilters()
                .getEntityFilter(0)
                .getSearchParametersCollection()
                .getSearchParameters(0)
                .getSearchFilter(1)
                .getTraversalFilter()
                .getStoppingCondition()
                .getStoppingPropertyFilter()
                .getNumericFilter()
                .getValue());
    }
}