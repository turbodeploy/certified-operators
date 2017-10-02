package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

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
                "VirtualMachine", "PhysicalMachine", "VirtualDataCenter", "Storage", "Application",
                "ApplicationServer", "Database", "VirtualApplication", "Cluster", "Group",
                "StorageCluster", "DiskArray", "StorageController", "Switch"})
                .forEach(className -> assertTrue(useCases.containsKey(className)));
    }

    /**
     * Verify that the first usecase for each se type is the byName criterion.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFirstIsByName() {
        Map<String,  GroupUseCase> useCases = groupUseCaseParser.getUseCases();
        useCases.values().stream()
                .map(map -> map.getCriteria().get(0).getElements())
                .forEach(d -> assertEquals("displayName", d));
    }
}
