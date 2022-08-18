package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMPUTE_TIER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;

public class GroupUseCaseParserTest {

    private static final String COMPUTE_TIERS_BY_CONSUMER_ENTITY_TYPE = "computeTiersByConsumerEntityType";

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
                "StorageCluster", "VirtualMachineCluster", "DiskArray", "StorageController",
                "Switch", "Service", "VirtualMachineSpec", "ApplicationComponentSpec"})
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

    /**
     * Verify that criteria that is hidden is not returned when we exclude hidden criteria.
     * Verify that criteria that is hidden is returned when we include all criteria.
     */
    @Test
    public void testCriteriaVisibility() {
        // includes all criteria
        final Map<String, GroupUseCase> allUseCases = groupUseCaseParser.getUseCases(true);
        assertNotNull(allUseCases);
        final GroupUseCase computeTierUseCaseAllCrit = allUseCases.get(COMPUTE_TIER);
        assertNotNull(computeTierUseCaseAllCrit);
        // Does not include hidden criteria
        final Map<String, GroupUseCase> useCasesNoHidden = groupUseCaseParser.getUseCases(false);
        assertNotNull(useCasesNoHidden);
        final GroupUseCase computeTierUseCaseNoHiddenCrit = useCasesNoHidden.get(COMPUTE_TIER);
        assertNotNull(computeTierUseCaseNoHiddenCrit);
        // compare the criteria of the visible-only and unfiltered ComputeTier use case
        final List<GroupUseCaseCriteria> allCriteria = computeTierUseCaseAllCrit.getCriteria();
        final List<GroupUseCaseCriteria> noHiddenCriteria = computeTierUseCaseNoHiddenCrit.getCriteria();
        assertThat(allCriteria.size(), greaterThan(noHiddenCriteria.size()));
        // computeTiersByConsumerEntityType is criteria with visible set to false and is not
        // meant to be shown in the UI.  Create maps of GroupUseCaseCriteria and verify that
        // it is not present in the visible only list of criteria but is present in the unfiltered list.
        final Map<String, GroupUseCaseCriteria> allCritByFilter = allCriteria.stream().collect(
                Collectors.toMap(GroupUseCaseCriteria::getFilterType, Function.identity()));
        assertThat(allCritByFilter.get(COMPUTE_TIERS_BY_CONSUMER_ENTITY_TYPE), notNullValue());
        final Map<String, GroupUseCaseCriteria> noHiddenCritByFilter = noHiddenCriteria.stream().collect(
                Collectors.toMap(GroupUseCaseCriteria::getFilterType, Function.identity()));
        assertThat(noHiddenCritByFilter.get(COMPUTE_TIERS_BY_CONSUMER_ENTITY_TYPE), nullValue());
    }
}
