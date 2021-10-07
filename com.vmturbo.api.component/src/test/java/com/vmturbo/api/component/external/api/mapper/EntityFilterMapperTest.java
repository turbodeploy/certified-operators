package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.MultiTraversalFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * This class tests the functionality of {@link EntityFilterMapper} class.
 */
public class EntityFilterMapperTest {

    private static final Long UDT_ID = 123L;

    private static final String UDT_PROBE_TYPE = "User-defined entities";

    private static final String USER_DEFINED_ENTITY_SERVICE_FILTER_NAME =
            "serviceByUserDefinedEntity";

    private static final ThinTargetInfo TARGET_INFO = mock(ThinTargetInfo.class);

    private static final ThinProbeInfo PROBE_INFO = mock(ThinProbeInfo.class);

    private final ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    private final GroupUseCaseParser groupUseCaseParser =
                    Mockito.spy(new GroupUseCaseParser(groupUseCaseFileName));


    private final EntityFilterMapper entityFilterMapper =
                    new EntityFilterMapper(groupUseCaseParser, thinTargetCache);

    /**
     * Junit rule.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String AND = "AND";
    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String VM_TYPE = "VirtualMachine";
    private static final String SERVICE_TYPE = "Service";
    private static final String PM_TYPE = "PhysicalMachine";
    private static final String DS_TYPE = "Storage";
    private static final String DISK_ARRAY_TYPE = "DiskArray";
    private static final String VDC_TYPE = "VirtualDataCenter";
    private static final String CONTAINER_CLUSTER_TYPE = "ContainerPlatformCluster";
    private static final String NAMESPACE_TYPE = "Namespace";
    private static final SearchFilter DISPLAYNAME_IS_FOO =
                    SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(FOO));
    private static final SearchFilter DISPLAYNAME_IS_BAR =
                    SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(BAR));
    private static final PropertyFilter TYPE_IS_SERVICE =
            SearchProtoUtil.entityTypeFilter(SERVICE_TYPE);
    private static final PropertyFilter TYPE_IS_VM = SearchProtoUtil.entityTypeFilter(VM_TYPE);
    private static final PropertyFilter TYPE_IS_PM = SearchProtoUtil.entityTypeFilter(PM_TYPE);
    private static final PropertyFilter TYPE_IS_DS = SearchProtoUtil.entityTypeFilter(DS_TYPE);
    private static final PropertyFilter TYPE_IS_DISK_ARRAY =
                    SearchProtoUtil.entityTypeFilter(DISK_ARRAY_TYPE);
    private static final PropertyFilter TYPE_IS_VDC = SearchProtoUtil.entityTypeFilter(VDC_TYPE);
    private static final PropertyFilter TYPE_IS_CONTAINER_CLUSTER =
                    SearchProtoUtil.entityTypeFilter(CONTAINER_CLUSTER_TYPE);
    private static final PropertyFilter TYPE_IS_NAMESPACE =
                    SearchProtoUtil.entityTypeFilter(NAMESPACE_TYPE);
    private static final SearchFilter PRODUCES_VMS = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, VM_TYPE));
    private static final SearchFilter PRODUCES_ONE_HOP = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.numberOfHops(TraversalDirection.PRODUCES, 1));
    private static final SearchFilter PRODUCES_ST = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, DS_TYPE));
    private static final SearchFilter PRODUCES_SVC = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, SERVICE_TYPE));

    /**
     * Verify that a simple byName criterion is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByNameSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertTrue(byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Tests the filter for getting vms based on their OS type.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByVMGuestOsType() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "Linux", "vmsByGuestName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());

        List<SearchFilter> expectedSearchFilter = Lists.newArrayList(SearchFilter.newBuilder()
                        .setPropertyFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("virtualMachineInfoRepoDTO")
                                        .setObjectFilter(ObjectFilter.newBuilder().addFilters(
                                                        PropertyFilter.newBuilder().setPropertyName(
                                                                        "guestOsType")
                                                                        .setStringFilter(
                                                                                        StringFilter.newBuilder()
                                                                                                        .addOptions("Linux")
                                                                                                        .setPositiveMatch(
                                                                                                                        true)
                                                                                                        .setCaseSensitive(
                                                                                                                        false)
                                                                                                        .build())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    /**
     * Tests the filter for getting vms based on the number of their cpus.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByVMNumCpus() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "3", "vmsByNumCPUs"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());

        List<SearchFilter> expectedSearchFilter = Lists.newArrayList(SearchFilter.newBuilder()
                        .setPropertyFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("virtualMachineInfoRepoDTO")
                                        .setObjectFilter(ObjectFilter.newBuilder()
                                                        .addFilters(PropertyFilter.newBuilder()
                                                                        .setPropertyName("numCpus")
                                                                        .setNumericFilter(
                                                                                        NumericFilter.newBuilder()
                                                                                                        .setComparisonOperator(
                                                                                                                        ComparisonOperator.EQ)
                                                                                                        .setValue(3)
                                                                                                        .build())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    /**
     * Tests the filter for getting vms based on the VMEM capacity.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByVMCommodityVMemCapacity() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.GREATER_THAN, "1024", "vmsByMem"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());

        List<SearchFilter> expectedSearchFilter = Lists.newArrayList(SearchFilter.newBuilder()
                        .setPropertyFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("commoditySoldList")
                                        .setListFilter(ListFilter.newBuilder().setObjectFilter(
                                                        ObjectFilter.newBuilder().addFilters(
                                                                        PropertyFilter.newBuilder()
                                                                                        .setPropertyName(
                                                                                                        "type")
                                                                                        .setStringFilter(
                                                                                                        StringFilter.newBuilder()
                                                                                                                        .addOptions("VMem")
                                                                                                                        .setPositiveMatch(
                                                                                                                                        true)
                                                                                                                        .setCaseSensitive(
                                                                                                                                        false)
                                                                                                                        .build())
                                                                                        .build())
                                                                        .addFilters(PropertyFilter
                                                                                        .newBuilder()
                                                                                        .setPropertyName(
                                                                                                        "capacity")
                                                                                        .setNumericFilter(
                                                                                                        NumericFilter.newBuilder()
                                                                                                                        .setComparisonOperator(
                                                                                                                                        ComparisonOperator.GT)
                                                                                                                        .setValue(1024)
                                                                                                                        .build())
                                                                                        .build())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    private FilterApiDTO filterDTO(String expType, String expVal, String filterType) {
        FilterApiDTO filter = new FilterApiDTO();
        filter.setExpType(expType);
        filter.setExpVal(expVal);
        filter.setFilterType(filterType);
        return filter;
    }

    private GroupApiDTO groupApiDTO(String logicalOperator, String className,
                    FilterApiDTO... filters) {
        GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setLogicalOperator(logicalOperator);
        inputDTO.setClassName(className);
        inputDTO.setCriteriaList(Arrays.asList(filters));
        return inputDTO;
    }

    /**
     * Verify that exception will be thrown for unknown filter type.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testNotExistingFilter() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "notExistingFilter"));
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Not existing filter type provided: notExistingFilter");
        entityFilterMapper.convertToSearchParameters(inputDTO.getCriteriaList(),
                        inputDTO.getClassName());
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByNameSearchNotEqual() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, FOO, "vmsByName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(FOO, byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Test for searching by equal operator.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByStateSearchEqual() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "IDLE", "vmsByState"));
        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        final SearchParameters filter = parameters.iterator().next();
        final PropertyFilter propertyFilter =
                        filter.getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(1, parameters.size());
        Assert.assertEquals(TYPE_IS_VM, filter.getStartingFilter());
        Assert.assertEquals("state", propertyFilter.getPropertyName());
        Assert.assertEquals("IDLE", propertyFilter.getStringFilter().getOptions(0));
    }

    /**
     * Test for searching by not equal operator.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByStateSearchNotEqual() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, "ACTIVE", "pmsByState"));
        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        final SearchParameters filter = parameters.iterator().next();
        final PropertyFilter propertyFilter =
                        filter.getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(1, parameters.size());
        Assert.assertEquals(TYPE_IS_PM, filter.getStartingFilter());
        Assert.assertEquals("state", propertyFilter.getPropertyName());
        Assert.assertEquals("ACTIVE", propertyFilter.getStringFilter().getOptions(0));
    }

    /**
     * register UDT Probe in {@link ThinTargetCache} for test requirements.
     */
    private void registerUserDefinedEntitiesProbe() {
        Mockito.when(thinTargetCache.getAllTargets()).thenReturn(Lists.newArrayList(TARGET_INFO));
        Mockito.when(TARGET_INFO.probeInfo()).thenReturn(PROBE_INFO);
        Mockito.when(TARGET_INFO.oid()).thenReturn(UDT_ID);
        Mockito.when(PROBE_INFO.type()).thenReturn(UDT_PROBE_TYPE);
    }

    /**
     * Verify correctness of User Defined Entity filter's parameters.
     *
     * @param searchParameters filter's {@link SearchParameters}.
     * @param propertyFilter filter's {@link PropertyFilter}.
     */
    private void verifyUserDefinedEntityFilterParameters(Collection<SearchParameters> searchParameters,
                                                                PropertyFilter propertyFilter) {
        Assert.assertEquals(1, searchParameters.size());
        Assert.assertEquals(TYPE_IS_SERVICE,
                        searchParameters.iterator().next().getStartingFilter());
        Assert.assertEquals(
            SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET, propertyFilter.getPropertyName());
        Assert.assertEquals(1, propertyFilter.getStringFilter().getOptionsCount());
        Assert.assertEquals(
                String.valueOf(UDT_ID), propertyFilter.getStringFilter().getOptions(0));
    }

    @Test
    public void testByUserDefinedEntityEqual() throws OperationFailedException {
        registerUserDefinedEntitiesProbe();
        GroupApiDTO inputDTO = groupApiDTO(AND, SERVICE_TYPE, filterDTO(EntityFilterMapper.EQUAL,
                "True", USER_DEFINED_ENTITY_SERVICE_FILTER_NAME));

        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), inputDTO.getClassName());

        final PropertyFilter propertyFilter =
                parameters.iterator().next().getSearchFilter(0).getPropertyFilter();

        verifyUserDefinedEntityFilterParameters(parameters, propertyFilter);
        Assert.assertEquals(true, propertyFilter.getStringFilter().getPositiveMatch());
    }

    @Test
    public void testByUserDefinedEntityNotEqual() throws OperationFailedException {
        registerUserDefinedEntitiesProbe();
        GroupApiDTO inputDTO = groupApiDTO(AND, SERVICE_TYPE, filterDTO(EntityFilterMapper.EQUAL,
                "False", USER_DEFINED_ENTITY_SERVICE_FILTER_NAME));

        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), inputDTO.getClassName());

        final PropertyFilter propertyFilter =
                parameters.iterator().next().getSearchFilter(0).getPropertyFilter();
        verifyUserDefinedEntityFilterParameters(parameters, propertyFilter);
        Assert.assertFalse(propertyFilter.getStringFilter().getPositiveMatch());
    }

    @Test
    public void testUserDefinedTopologyProbeNotRegistered() throws OperationFailedException {
        Mockito.when(thinTargetCache.getAllTargets()).thenReturn(Lists.newArrayList());
        GroupApiDTO inputDTO = groupApiDTO(AND, SERVICE_TYPE, filterDTO(EntityFilterMapper.EQUAL,
                "True", USER_DEFINED_ENTITY_SERVICE_FILTER_NAME));

        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), inputDTO.getClassName());
        final PropertyFilter propertyFilter =
                parameters.iterator().next().getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(0, propertyFilter.getStringFilter().getOptionsCount());
    }

    /**
     * Verify multiple not equal by name search are converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByNameSearchMultipleNotEqual() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.NOT_EQUAL, BAR, "vmsByPMName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(2, parameters.size());
        final Iterator<SearchParameters> iterator = parameters.iterator();
        SearchParameters firstByName = iterator.next();
        assertEquals(TYPE_IS_VM, firstByName.getStartingFilter());
        assertEquals(1, firstByName.getSearchFilterCount());
        assertEquals(FOO, firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());

        SearchParameters secondByName = iterator.next();
        assertEquals(TYPE_IS_PM, secondByName.getStartingFilter());
        assertEquals(3, secondByName.getSearchFilterCount());
        assertEquals(BAR, secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Verify that a byName criterion combined with a traversal with one hop are converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByNameAndTraversalHopSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(2, parameters.size());
        final Iterator<SearchParameters> it = parameters.iterator();
        SearchParameters byName = it.next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        SearchParameters byPMNameByVMName = it.next();
        assertEquals(TYPE_IS_PM, byPMNameByVMName.getStartingFilter());
        assertEquals(3, byPMNameByVMName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byPMNameByVMName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byPMNameByVMName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM),
                        byPMNameByVMName.getSearchFilter(2));
    }

    /**
     * Verifies that criteria for PMs by number of vms converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByTraversalHopNumConnectedVMs() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "3", "pmsByNumVms"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());

        assertEquals(1, parameters.size());
        SearchParameters byTraversalHopNumConnectedVMs = parameters.iterator().next();
        assertEquals(TYPE_IS_PM, byTraversalHopNumConnectedVMs.getStartingFilter());
        assertEquals(1, byTraversalHopNumConnectedVMs.getSearchFilterCount());

        SearchFilter hopNumConnectedVMs = SearchProtoUtil.searchFilterTraversal(
                        createNumHopsNumConnectedVerticesFilter(TraversalDirection.PRODUCES, 1,
                                        ComparisonOperator.EQ, 3,
                                        EntityType.VIRTUAL_MACHINE.getNumber()));
        assertEquals(hopNumConnectedVMs, byTraversalHopNumConnectedVMs.getSearchFilter(0));
    }

    private static TraversalFilter createNumHopsNumConnectedVerticesFilter(
                    TraversalDirection direction, int hops, ComparisonOperator operator, long value,
                    int entityType) {
        StoppingCondition numHops = StoppingCondition.newBuilder().setNumberHops(hops)
                        .setVerticesCondition(VerticesCondition.newBuilder()
                                        .setNumConnectedVertices(NumericFilter.newBuilder()
                                                        .setComparisonOperator(operator)
                                                        .setValue(value).build())
                                        .setEntityType(entityType).build())
                        .build();
        return TraversalFilter.newBuilder().setTraversalDirection(direction)
                        .setStoppingCondition(numHops).build();
    }

    /**
     * Verifies that criteria for VM by disk array search is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testVmsByDiskArrayNameSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByDiskArrayName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), BAR);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_DISK_ARRAY, byName.getStartingFilter());
        assertEquals(5, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ST, byName.getSearchFilter(1));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(2));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(3));
        assertEquals(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.nameFilterRegex(BAR)),
                        byName.getSearchFilter(4));
    }

    /**
     * Verifies that criteria for VM by VDC name search is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testVmsByVdcNameSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByVDC"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VDC, byName.getStartingFilter());
        assertEquals(3, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(2));
    }

    /**
     * Verifies that criteria for VM by VDC nested name search is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testVmsByVdcNestedNameSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByDCnested"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VDC, byName.getStartingFilter());
        assertEquals(2, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_VMS, byName.getSearchFilter(1));
    }

    /**
     * Verify that a traversal with one hop (without a byName criterion) is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testTraversalSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byPMName = parameters.iterator().next();
        assertEquals(TYPE_IS_PM, byPMName.getStartingFilter());
        assertEquals(3, byPMName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byPMName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byPMName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byPMName.getSearchFilter(2));
    }

    /**
     * Verify that a byName criterion combined with a traversal that stops at a class type,are
     * converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByNameAndTraversalClassSearch() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByStorage"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(2, parameters.size());
        final Iterator<SearchParameters> it = parameters.iterator();
        SearchParameters byName = it.next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        SearchParameters byDSName = it.next();
        assertEquals(TYPE_IS_DS, byDSName.getStartingFilter());
        assertEquals(3, byDSName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byDSName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byDSName.getSearchFilter(1));
        assertEquals(TYPE_IS_VM, byDSName.getSearchFilter(2).getPropertyFilter());
    }

    /**
     * Verify that a serviceByContainerPlatformCluster criterion combined with a serviceByNamespace
     * criterion start at proper entity types with a series of traversal filters that stop at
     * Service type.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testServiceByContainerClusterAndNamespace() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, SERVICE_TYPE,
            filterDTO(EntityFilterMapper.EQUAL, FOO, "serviceByContainerPlatformCluster"),
            filterDTO(EntityFilterMapper.EQUAL, BAR, "serviceByNamespace"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(2, parameters.size());
        final Iterator<SearchParameters> it = parameters.iterator();
        SearchParameters byContainerCluster = it.next();
        assertEquals(TYPE_IS_CONTAINER_CLUSTER, byContainerCluster.getStartingFilter());
        assertEquals(7, byContainerCluster.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byContainerCluster.getSearchFilter(0));
        assertEquals(PRODUCES_SVC, byContainerCluster.getSearchFilter(6));
        SearchParameters byNamespace = it.next();
        assertEquals(TYPE_IS_NAMESPACE, byNamespace.getStartingFilter());
        assertEquals(6, byNamespace.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byNamespace.getSearchFilter(0));
        assertEquals(PRODUCES_SVC, byNamespace.getSearchFilter(5));
    }

    /**
     * Verify that two traversals are converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testTwoTraversals() throws OperationFailedException {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"),
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByStorage"));
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(2, parameters.size());
    }

    /**
     * Verify that traversal that combines more than one relations correctly converts into search
     * parameters collection.
     *
     * @throws OperationFailedException in case specified searching parameters
     *                 cannot be converted.
     */
    @Test
    public void checkCombinedTraversal() throws OperationFailedException {
        final GroupApiDTO inputDTO = groupApiDTO(AND, StringConstants.VIRTUAL_VOLUME,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "volumeByStorage"));
        final Collection<SearchParameters> parameters = entityFilterMapper
                        .convertToSearchParameters(inputDTO.getCriteriaList(),
                                        inputDTO.getClassName());
        Assert.assertThat(1, CoreMatchers.is(parameters.size()));
        final Iterator<SearchParameters> it = parameters.iterator();
        final SearchParameters byName = it.next();
        assertEquals(SearchProtoUtil.entityTypeFilter(DS_TYPE), byName.getStartingFilter());
        assertEquals(3, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        final MultiTraversalFilter multiTraversalFilter =
                        byName.getSearchFilter(1).getMultiTraversalFilter();
        Assert.assertThat(multiTraversalFilter.getTraversalFilterCount(), CoreMatchers.is(2));
        final Collection<TraversalDirection> actualDirections =
                        multiTraversalFilter.getTraversalFilterList().stream()
                                        .map(TraversalFilter::getTraversalDirection)
                                        .collect(Collectors.toSet());
        Assert.assertThat(actualDirections, Matchers.containsInAnyOrder(TraversalDirection.PRODUCES,
                        TraversalDirection.CONNECTED_FROM));
        Assert.assertThat(multiTraversalFilter.getOperator(), CoreMatchers.is(LogicalOperator.OR));
    }

    /**
     * Verifies that criteria for VM by name search is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testVmsWithNameQuery() throws OperationFailedException {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE);
        Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        groupDto.getCriteriaList(), groupDto.getClassName(), FOO);
        assertThat(parameters.size(), is(1));
        SearchParameters param = parameters.iterator().next();
        assertThat(param.getStartingFilter(), is(TYPE_IS_VM));
        assertThat(param.getSearchFilterCount(), is(1));
        assertThat(param.getSearchFilter(0), is(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.nameFilterRegex( FOO ))));
    }

    /**
     * Test that the vendor ID criterion is properly converted into a list filter.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testVendorIdSearch() throws OperationFailedException {
        final Collection<SearchParameters> sp = entityFilterMapper.convertToSearchParameters(
            Collections.singletonList(
                filterDTO(EntityFilterMapper.REGEX_MATCH, "id-1.*", "volumeById")),
            "VirtualVolume");
        assertEquals(1, sp.size());
        final SearchParameters params = sp.iterator().next();
        assertEquals(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_VOLUME),
            params.getStartingFilter());
        assertEquals(1, params.getSearchFilterCount());
        final SearchFilter searchFilter = params.getSearchFilter(0);
        assertTrue(searchFilter.hasPropertyFilter());
        final PropertyFilter propFilter = searchFilter.getPropertyFilter();
        assertEquals("vendorId", propFilter.getPropertyName());
        assertTrue(propFilter.hasListFilter());
        assertTrue(propFilter.getListFilter().hasStringFilter());
        assertEquals("^id-1.*$",
            propFilter.getListFilter().getStringFilter().getStringPropertyRegex());
    }

    /**
     * Verify that a extact string match by-tags criterion is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByTagSearch() throws OperationFailedException {
        final MapFilter tagFilter =
                        checkAndReturnMapFilter(EntityFilterMapper.NOT_EQUAL, "k=v1|k=v2");
        assertEquals("k", tagFilter.getKey());
        assertEquals(2, tagFilter.getValuesCount());
        assertEquals("v1", tagFilter.getValues(0));
        assertEquals("v2", tagFilter.getValues(1));
        assertFalse(tagFilter.getPositiveMatch());
        assertFalse(tagFilter.hasRegex());
    }

    /**
     * Verify that a regex match by-tags criterion is converted properly.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testByTagSearchRegex() throws OperationFailedException {
        final MapFilter tagFilter =
                        checkAndReturnMapFilter(EntityFilterMapper.REGEX_MATCH, "k=.*a.*");
        assertEquals(0, tagFilter.getValuesCount());
        assertTrue(tagFilter.getPositiveMatch());
        assertEquals("^k=.*a.*$", tagFilter.getRegex());
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, in various cases.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testMapFilter() throws OperationFailedException {
        final MapFilter filter1 = checkAndReturnMapFilter(EntityFilterMapper.EQUAL, "AA=B");
        assertEquals("AA", filter1.getKey());
        assertEquals(1, filter1.getValuesCount());
        assertEquals("B", filter1.getValues(0));
        assertTrue(filter1.getPositiveMatch());
        assertFalse(filter1.hasRegex());

        final MapFilter filter2 =
                        checkAndReturnMapFilter(EntityFilterMapper.NOT_EQUAL, "AA=BB|AA=CC");
        assertEquals("AA", filter2.getKey());
        assertEquals(2, filter2.getValuesCount());
        assertTrue(filter2.getValuesList().contains("BB"));
        assertTrue(filter2.getValuesList().contains("CC"));
        assertFalse(filter2.getPositiveMatch());
        assertFalse(filter2.hasRegex());

        final MapFilter filter4 =
                        checkAndReturnMapFilter(EntityFilterMapper.REGEX_NO_MATCH, "k=.*");
        assertEquals(0, filter4.getValuesCount());
        assertEquals("^k=.*$", filter4.getRegex());
        assertFalse(filter4.getPositiveMatch());

        final MapFilter filter5 = checkAndReturnMapFilter(EntityFilterMapper.REGEX_MATCH, "k=.*");
        assertEquals(0, filter5.getValuesCount());
        assertEquals("^k=.*$", filter5.getRegex());
        assertTrue(filter5.getPositiveMatch());
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise an all-rejecting (or all-accepting) filter
     * will be created.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testBadMapFilter1() throws OperationFailedException {
        testByTagSearchBadExpVal(EntityFilterMapper.NOT_EQUAL, "AA=B|DD");
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise no filter will be created.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testBadMapFilter2() throws OperationFailedException {
        testByTagSearchBadExpVal(EntityFilterMapper.NOT_EQUAL, "AA=BB|C=D");
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise no filter will be created.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testBadMapFilter3() throws OperationFailedException {
        testByTagSearchBadExpVal(EntityFilterMapper.EQUAL, "=B|foo=DD");
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, when special characters = | \ are present in the keys or values.
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testMapFilterSpecialCharacters() throws OperationFailedException {
        final MapFilter filter1 = checkAndReturnMapFilter(EntityFilterMapper.EQUAL,
                        "AA\\==B|AA\\==\\=C\\|D");
        assertEquals("AA=", filter1.getKey());
        assertEquals(2, filter1.getValuesCount());
        assertEquals("B", filter1.getValues(0));
        assertEquals("=C|D", filter1.getValues(1));
        assertTrue(filter1.getPositiveMatch());
        assertFalse(filter1.hasRegex());

        final MapFilter filter2 =
                        checkAndReturnMapFilter(EntityFilterMapper.EQUAL, "AA\\=\\\\B=C");
        assertEquals("AA=\\B", filter2.getKey());
        assertEquals(1, filter2.getValuesCount());
        assertEquals("C", filter2.getValues(0));
        assertTrue(filter2.getPositiveMatch());
        assertFalse(filter2.hasRegex());
    }

    private PropertyFilter checkAndReturnOnePropertyFilter(@Nonnull String operator, @Nonnull String expVal)
                    throws OperationFailedException {
        final GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setCriteriaList(
                Collections.singletonList(filterDTO(operator, expVal, "vmsByTag")));
        inputDTO.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        final Collection<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), inputDTO.getClassName());
        assertEquals(1, parameters.size());
        final SearchParameters byName = parameters.iterator().next();
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        return byName.getSearchFilter(0).getPropertyFilter();
    }

    private MapFilter checkAndReturnMapFilter(@Nonnull String operator, @Nonnull String expVal)
                    throws OperationFailedException {
        return checkAndReturnOnePropertyFilter(operator, expVal).getMapFilter();
    }

    private void testByTagSearchBadExpVal(@Nonnull String operator, @Nonnull String expVal)
                    throws OperationFailedException {
        assertEquals(EntityFilterMapper.REJECT_ALL_PROPERTY_FILTER,
                     checkAndReturnOnePropertyFilter(operator, expVal));
    }

    /**
     * Test that the group filter processor creates the correct group membership filter
     * (positive match case).
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testGroupFilterProcessorPositive() throws OperationFailedException {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
            filterDTO(EntityFilterMapper.EQUAL, "asdf", "volumeByResourceGroupName"));
        final Collection<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
            criteriaList, ApiEntityType.VIRTUAL_VOLUME.apiStr());
        assertEquals(1, result.size());
        final SearchParameters params = result.iterator().next();

        assertEquals(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_VOLUME),
            params.getStartingFilter());
        assertEquals(1, params.getSearchFilterCount());
        final GroupFilter memFilter = params.getSearchFilter(0).getGroupFilter();
        assertEquals(GroupType.RESOURCE, memFilter.getGroupType());
        assertEquals(SearchableProperties.DISPLAY_NAME, memFilter.getGroupSpecifier().getPropertyName());
        final StringFilter specifierStringFilter = memFilter.getGroupSpecifier().getStringFilter();
        assertEquals(1, specifierStringFilter.getOptionsCount());
        assertEquals("asdf", specifierStringFilter.getOptions(0));
        assertTrue(specifierStringFilter.getPositiveMatch());
    }

    /**
     * Test that the group filter processor creates the correct group membership filter
     * (negative match case).
     *
     * @throws OperationFailedException in case specified searching parameters cannot be converted.
     */
    @Test
    public void testGroupFilterProcessorNegative() throws OperationFailedException {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
            filterDTO(EntityFilterMapper.NOT_EQUAL, "asdf", "databaseServerByResourceGroupName"));
        final Collection<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
            criteriaList, ApiEntityType.DATABASE_SERVER.apiStr());
        assertEquals(1, result.size());
        final SearchParameters params = result.iterator().next();

        assertEquals(SearchProtoUtil.entityTypeFilter(ApiEntityType.DATABASE_SERVER),
            params.getStartingFilter());
        assertEquals(1, params.getSearchFilterCount());
        final GroupFilter memFilter = params.getSearchFilter(0).getGroupFilter();
        assertEquals(GroupType.RESOURCE, memFilter.getGroupType());
        assertEquals(SearchableProperties.DISPLAY_NAME, memFilter.getGroupSpecifier().getPropertyName());
        final StringFilter specifierStringFilter = memFilter.getGroupSpecifier().getStringFilter();
        assertEquals(1, specifierStringFilter.getOptionsCount());
        assertEquals("asdf", specifierStringFilter.getOptions(0));
        assertFalse(specifierStringFilter.getPositiveMatch());
    }

    /**
     * Test search by tags within group with positive match.
     *
     * @throws OperationFailedException - in case conversion fails
     */
    @Test
    public void testGroupFilterWithTagsPositive() throws OperationFailedException {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
                filterDTO(EntityFilterMapper.EQUAL, "Category\\=1=tagWith\\|strangeCharact\\=ers", "pmsByClusterTag"));
        final Collection<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
                criteriaList, ApiEntityType.PHYSICAL_MACHINE.apiStr());
        verifyTagSearchResultsSingleValue(result, GroupType.COMPUTE_HOST_CLUSTER, SearchableProperties.TAGS_TYPE_PROPERTY_NAME,
                "Category=1", "tagWith|strangeCharact=ers", true);
    }

    /**
     * Test search by tags within group with negative match.
     *
     * @throws OperationFailedException - in case conversion fails
     */
    @Test
    public void testGroupFilterWithTagsNegative() throws OperationFailedException {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
                filterDTO(EntityFilterMapper.NOT_EQUAL, "Category\\=1=tagWith\\|strangeCharact\\=ers", "pmsByClusterTag"));
        final Collection<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
                criteriaList, ApiEntityType.PHYSICAL_MACHINE.apiStr());
        verifyTagSearchResultsSingleValue(result, GroupType.COMPUTE_HOST_CLUSTER, SearchableProperties.TAGS_TYPE_PROPERTY_NAME,
                "Category=1", "tagWith|strangeCharact=ers", false);
    }

    private static void verifyTagSearchResultsSingleValue(final Collection<SearchParameters> result,
           final GroupType groupType, final String propertyName, final String tagKey, final String tagValue,
                                                   boolean isPositiveMatch) {
        assertEquals(1, result.size());
        final SearchParameters params = result.iterator().next();
        final GroupFilter memFilter = params.getSearchFilter(0).getGroupFilter();
        assertEquals(groupType, memFilter.getGroupType());
        assertEquals(propertyName, memFilter.getGroupSpecifier().getPropertyName());
        final MapFilter mapFilter = memFilter.getGroupSpecifier().getMapFilter();
        assertNotNull(mapFilter);
        assertEquals(tagKey, mapFilter.getKey());
        assertEquals(1, mapFilter.getValuesCount());
        assertEquals(tagValue, mapFilter.getValuesList().get(0));
        if (isPositiveMatch) {
            assertTrue(mapFilter.getPositiveMatch());
        } else {
            assertFalse(mapFilter.getPositiveMatch());
        }
    }

}
