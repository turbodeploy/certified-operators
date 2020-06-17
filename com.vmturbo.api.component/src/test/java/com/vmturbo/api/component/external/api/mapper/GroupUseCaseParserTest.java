package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;

public class GroupUseCaseParserTest {

    private GroupUseCaseParser groupUseCaseParser;
    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    @Before
    public void init() {
        groupUseCaseParser = new GroupUseCaseParser(groupUseCaseFileName);
    }

    /**
     * Verify that all expected SE types exist.
     */
    @Test
    public void testUseCase() {
        Map<String, GroupUseCase> useCases = groupUseCaseParser.getUseCases();
        assertNotNull(useCases);
        Arrays.stream(new String[] {
                "VirtualMachine", "PhysicalMachine", "VirtualDataCenter", "Storage",
                "Database", "Cluster", "Group",
                "StorageCluster", "VirtualMachineCluster", "DiskArray", "StorageController", "Switch"})
                .forEach(className -> assertTrue(useCases.containsKey(className)));
    }

    /**
     * Verify that the first usecase for each se type is the byName criterion.
     */
    @Test
    public void testFirstIsByName() {
        final Map<String, GroupUseCase> useCases = new HashMap<>(groupUseCaseParser.getUseCases());
        final Set<String> groupTypes = ImmutableSet.of("Group", "Cluster", "StorageCluster",
                "VirtualMachineCluster");
        groupTypes.forEach(type -> {
            Assert.assertEquals(type + EntityFilterMapper.ELEMENTS_DELIMITER + "displayName",
                            useCases.get(type).getCriteria().get(0).getElements());
            useCases.remove(type);
        });
        useCases.forEach((key, useCase) -> {
                    assertEquals("displayName", useCase.getCriteria().get(0).getElements());
                });
    }
}
