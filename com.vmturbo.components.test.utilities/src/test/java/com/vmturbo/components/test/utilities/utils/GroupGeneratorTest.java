package com.vmturbo.components.test.utilities.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GroupGeneratorTest {

    private final GroupGenerator groupGen = new GroupGenerator();

    @Test
    public void testEntityWithName() {
        final GroupInfo groupInfo = groupGen.entityWithName(EntityType.PHYSICAL_MACHINE, "foo");
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupInfo.getEntityType());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getStartingFilter()
            .getNumericFilter()
            .getValue());
        assertEquals("foo", groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(0)
            .getPropertyFilter()
            .getStringFilter()
            .getStringPropertyRegex());
    }

    @Test
    public void testStorageWithName() {
        final GroupInfo groupInfo = groupGen.entityWithName(EntityType.STORAGE, "foo");
        assertEquals(EntityType.STORAGE_VALUE, groupInfo.getEntityType());
        assertEquals(EntityType.STORAGE_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getStartingFilter()
            .getNumericFilter()
            .getValue());
        assertEquals("foo", groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(0)
            .getPropertyFilter()
            .getStringFilter()
            .getStringPropertyRegex());
    }

    @Test
    public void testVmsOnHost() {
        final GroupInfo groupInfo = groupGen.vmsOnHost("foo");
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupInfo.getEntityType());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getStartingFilter()
            .getNumericFilter()
            .getValue());
        assertEquals("foo", groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(0)
            .getPropertyFilter()
            .getStringFilter()
            .getStringPropertyRegex());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupInfo.getSearchParametersCollection()
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
        final GroupInfo groupInfo = groupGen.vmsOnStorage("foo");
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupInfo.getEntityType());
        assertEquals(EntityType.STORAGE_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getStartingFilter()
            .getNumericFilter()
            .getValue());
        assertEquals("foo", groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(0)
            .getPropertyFilter()
            .getStringFilter()
            .getStringPropertyRegex());

        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupInfo.getSearchParametersCollection()
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
        final GroupInfo groupInfo = groupGen.hostsOnDatacenter("foo");
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupInfo.getEntityType());
        assertEquals(EntityType.DATACENTER_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getStartingFilter()
            .getNumericFilter()
            .getValue());
        assertEquals("foo", groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(0)
            .getPropertyFilter()
            .getStringFilter()
            .getStringPropertyRegex());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, groupInfo.getSearchParametersCollection()
            .getSearchParameters(0)
            .getSearchFilter(1)
            .getTraversalFilter()
            .getStoppingCondition()
            .getStoppingPropertyFilter()
            .getNumericFilter()
            .getValue());
    }
}