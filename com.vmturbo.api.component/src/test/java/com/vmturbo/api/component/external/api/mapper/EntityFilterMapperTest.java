package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.GroupMembershipFilter;
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
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * This class tests the functionality of {@link EntityFilterMapper} class.
 */
public class EntityFilterMapperTest {
    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    private final GroupUseCaseParser groupUseCaseParser =
                    Mockito.spy(new GroupUseCaseParser(groupUseCaseFileName));

    private final EntityFilterMapper entityFilterMapper =
                    new EntityFilterMapper(groupUseCaseParser);

    /**
     * Junit rule.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String AND = "AND";
    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String VM_TYPE = "VirtualMachine";
    private static final String PM_TYPE = "PhysicalMachine";
    private static final String DS_TYPE = "Storage";
    private static final String DISK_ARRAY_TYPE = "DiskArray";
    private static final String VDC_TYPE = "VirtualDataCenter";
    private static final SearchFilter DISPLAYNAME_IS_FOO =
                    SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(FOO));
    private static final SearchFilter DISPLAYNAME_IS_BAR =
                    SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(BAR));
    private static final PropertyFilter TYPE_IS_VM = SearchProtoUtil.entityTypeFilter(VM_TYPE);
    private static final PropertyFilter TYPE_IS_PM = SearchProtoUtil.entityTypeFilter(PM_TYPE);
    private static final PropertyFilter TYPE_IS_DS = SearchProtoUtil.entityTypeFilter(DS_TYPE);
    private static final PropertyFilter TYPE_IS_DISK_ARRAY =
                    SearchProtoUtil.entityTypeFilter(DISK_ARRAY_TYPE);
    private static final PropertyFilter TYPE_IS_VDC = SearchProtoUtil.entityTypeFilter(VDC_TYPE);
    private static final SearchFilter PRODUCES_VMS = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, VM_TYPE));
    private static final SearchFilter PRODUCES_ONE_HOP = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.numberOfHops(TraversalDirection.PRODUCES, 1));
    private static final SearchFilter PRODUCES_ST = SearchProtoUtil.searchFilterTraversal(
                    SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, DS_TYPE));

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertTrue(byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Tests the filter for getting vms based on their OS type.
     */
    @Test
    public void testByVMGuestOsType() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "Linux", "vmsByGuestName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
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
     */
    @Test
    public void testByVMNumCpus() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "3", "vmsByNumCPUs"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
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
     */
    @Test
    public void testByVMCommodityVMemCapacity() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.GREATER_THAN, "1024", "vmsByMem"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
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
     */
    @Test
    public void testNotExistingFilter() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "notExistingFilter"));
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Not existing filter type provided: notExistingFilter");
        entityFilterMapper.convertToSearchParameters(inputDTO.getCriteriaList(),
                        inputDTO.getClassName(), null);
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearchNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(FOO, byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(byName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Test for searching by equal operator.
     */
    @Test
    public void testByStateSearchEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "IDLE", "vmsByState"));
        final List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        final PropertyFilter propertyFilter =
                        parameters.get(0).getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(1, parameters.size());
        Assert.assertEquals(TYPE_IS_VM, parameters.get(0).getStartingFilter());
        Assert.assertEquals("state", propertyFilter.getPropertyName());
        Assert.assertEquals("IDLE", propertyFilter.getStringFilter().getOptions(0));
    }

    /**
     * Test for searching by not equal operator.
     */
    @Test
    public void testByStateSearchNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, "ACTIVE", "pmsByState"));
        final List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        final PropertyFilter propertyFilter =
                        parameters.get(0).getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(1, parameters.size());
        Assert.assertEquals(TYPE_IS_PM, parameters.get(0).getStartingFilter());
        Assert.assertEquals("state", propertyFilter.getPropertyName());
        Assert.assertEquals("ACTIVE", propertyFilter.getStringFilter().getOptions(0));
    }

    /**
     * Verify multiple not equal by name search are converted properly.
     */
    @Test
    public void testByNameSearchMultipleNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.NOT_EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.NOT_EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
        SearchParameters firstByName = parameters.get(0);
        assertEquals(TYPE_IS_VM, firstByName.getStartingFilter());
        assertEquals(1, firstByName.getSearchFilterCount());
        assertEquals(FOO, firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());

        SearchParameters secondByName = parameters.get(1);
        assertEquals(TYPE_IS_PM, secondByName.getStartingFilter());
        assertEquals(3, secondByName.getSearchFilterCount());
        assertEquals(BAR, secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertFalse(secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Verify that a byName criterion combined with a traversal with one hop are converted properly.
     */
    @Test
    public void testByNameAndTraversalHopSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        SearchParameters byPMNameByVMName = parameters.get(1);
        assertEquals(TYPE_IS_PM, byPMNameByVMName.getStartingFilter());
        assertEquals(3, byPMNameByVMName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byPMNameByVMName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byPMNameByVMName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM),
                        byPMNameByVMName.getSearchFilter(2));
    }

    /**
     * Verifies that criteria for PMs by number of vms converted properly.
     */
    @Test
    public void testByTraversalHopNumConnectedVMs() {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, "3", "pmsByNumVms"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);

        assertEquals(1, parameters.size());
        SearchParameters byTraversalHopNumConnectedVMs = parameters.get(0);
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
     */
    @Test
    public void testVmsByDiskArrayNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByDiskArrayName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), BAR);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_DISK_ARRAY, byName.getStartingFilter());
        assertEquals(5, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ST, byName.getSearchFilter(1));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(2));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(3));
        assertEquals(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.nameFilterRegex(".*" + BAR + ".*")),
                        byName.getSearchFilter(4));
    }

    /**
     * Verifies that criteria for VM by VDC name search is converted properly.
     */
    @Test
    public void testVmsByVdcNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByVDC"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VDC, byName.getStartingFilter());
        assertEquals(3, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(2));
    }

    /**
     * Verifies that criteria for VM by VDC nested name search is converted properly.
     */
    @Test
    public void testVmsByVdcNestedNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByDCnested"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VDC, byName.getStartingFilter());
        assertEquals(2, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_VMS, byName.getSearchFilter(1));
    }

    /**
     * Verify that a traversal with one hop (without a byName criterion) is converted properly.
     */
    @Test
    public void testTraversalSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byPMName = parameters.get(0);
        assertEquals(TYPE_IS_PM, byPMName.getStartingFilter());
        assertEquals(3, byPMName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byPMName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byPMName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byPMName.getSearchFilter(2));
    }

    /**
     * Verify that a byName criterion combined with a traversal that stops at a class type,are
     * converted properly.
     */
    @Test
    public void testByNameAndTraversalClassSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByName"),
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByStorage"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        SearchParameters byDSName = parameters.get(1);
        assertEquals(TYPE_IS_DS, byDSName.getStartingFilter());
        assertEquals(3, byDSName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byDSName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byDSName.getSearchFilter(1));
        assertEquals(TYPE_IS_VM, byDSName.getSearchFilter(2).getPropertyFilter());
    }

    /**
     * Verify that two traversals are converted properly.
     */
    @Test
    public void testTwoTraversals() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, BAR, "vmsByPMName"),
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByStorage"));
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
    }

    /**
     * Verifies that criteria for VM by name search is converted properly.
     */
    @Test
    public void testVmsWithNameQuery() {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE);
        List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        groupDto.getCriteriaList(), groupDto.getClassName(), FOO);
        assertThat(parameters.size(), is(1));
        SearchParameters param = parameters.get(0);
        assertThat(param.getStartingFilter(), is(TYPE_IS_VM));
        assertThat(param.getSearchFilterCount(), is(1));
        assertThat(param.getSearchFilter(0), is(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.nameFilterRegex(".*" + FOO + ".*"))));
    }

    /**
     * Test that the vendor ID criterion is properly converted into a list filter.
     */
    @Test
    public void testVendorIdSearch() {
        final List<SearchParameters> sp = entityFilterMapper.convertToSearchParameters(
            Collections.singletonList(
                filterDTO(EntityFilterMapper.REGEX_MATCH, "id-1.*", "volumeById")),
            "VirtualVolume", null);
        assertEquals(1, sp.size());
        final SearchParameters params = sp.get(0);
        assertEquals(SearchProtoUtil.entityTypeFilter(UIEntityType.VIRTUAL_VOLUME),
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
     */
    @Test
    public void testByTagSearch() {
        final MapFilter tagFilter =
                        testByTagSearchGoodExpVal(EntityFilterMapper.NOT_EQUAL, "k=v1|k=v2");
        assertEquals("k", tagFilter.getKey());
        assertEquals(2, tagFilter.getValuesCount());
        assertEquals("v1", tagFilter.getValues(0));
        assertEquals("v2", tagFilter.getValues(1));
        assertFalse(tagFilter.getPositiveMatch());
        assertFalse(tagFilter.hasRegex());
    }

    /**
     * Verify that a regex match by-tags criterion is converted properly.
     */
    @Test
    public void testByTagSearchRegex() {
        final MapFilter tagFilter =
                        testByTagSearchGoodExpVal(EntityFilterMapper.REGEX_MATCH, "k=.*a.*");
        assertEquals("k", tagFilter.getKey());
        assertEquals(0, tagFilter.getValuesCount());
        assertTrue(tagFilter.getPositiveMatch());
        assertEquals("^.*a.*$", tagFilter.getRegex());
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, in various cases.
     */
    @Test
    public void testMapFilter() {
        final MapFilter filter1 = testByTagSearchGoodExpVal(EntityFilterMapper.EQUAL, "AA=B");
        assertEquals("AA", filter1.getKey());
        assertEquals(1, filter1.getValuesCount());
        assertEquals("B", filter1.getValues(0));
        assertTrue(filter1.getPositiveMatch());
        assertFalse(filter1.hasRegex());

        final MapFilter filter2 =
                        testByTagSearchGoodExpVal(EntityFilterMapper.NOT_EQUAL, "AA=BB|AA=CC");
        assertEquals("AA", filter2.getKey());
        assertEquals(2, filter2.getValuesCount());
        assertTrue(filter2.getValuesList().contains("BB"));
        assertTrue(filter2.getValuesList().contains("CC"));
        assertFalse(filter2.getPositiveMatch());
        assertFalse(filter2.hasRegex());

        final MapFilter filter4 =
                        testByTagSearchGoodExpVal(EntityFilterMapper.REGEX_NO_MATCH, "k=.*");
        assertEquals("k", filter4.getKey());
        assertEquals(0, filter4.getValuesCount());
        assertEquals("^.*$", filter4.getRegex());
        assertFalse(filter4.getPositiveMatch());

        final MapFilter filter5 = testByTagSearchGoodExpVal(EntityFilterMapper.REGEX_MATCH, "k=.*");
        assertEquals("k", filter5.getKey());
        assertEquals(0, filter5.getValuesCount());
        assertEquals("^.*$", filter5.getRegex());
        assertTrue(filter5.getPositiveMatch());
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise no filter will be created.
     */
    @Test
    public void testBadMapFilter1() {
        testByTagSearchBadExpVal(EntityFilterMapper.NOT_EQUAL, "AA=B|DD");
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise no filter will be created.
     */
    @Test
    public void testBadMapFilter2() {
        testByTagSearchBadExpVal(EntityFilterMapper.NOT_EQUAL, "AA=BB|C=D");
    }

    /**
     * The expression value that comes from the UI during the creation of a map filter
     * should be well-formed. Otherwise no filter will be created.
     */
    @Test
    public void testBadMapFilter3() {
        testByTagSearchBadExpVal(EntityFilterMapper.NOT_EQUAL, "=B|foo=DD");
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, when special characters = | \ are present in the keys or values.
     */
    @Test
    public void testMapFilterSpecialCharacters() {
        final MapFilter filter1 = testByTagSearchGoodExpVal(EntityFilterMapper.EQUAL,
                        "AA\\==B|AA\\==\\=C\\|D");
        assertEquals("AA=", filter1.getKey());
        assertEquals(2, filter1.getValuesCount());
        assertEquals("B", filter1.getValues(0));
        assertEquals("=C|D", filter1.getValues(1));
        assertTrue(filter1.getPositiveMatch());
        assertFalse(filter1.hasRegex());

        final MapFilter filter2 =
                        testByTagSearchGoodExpVal(EntityFilterMapper.EQUAL, "AA\\=\\\\B=C");
        assertEquals("AA=\\B", filter2.getKey());
        assertEquals(1, filter2.getValuesCount());
        assertEquals("C", filter2.getValues(0));
        assertTrue(filter2.getPositiveMatch());
        assertFalse(filter2.hasRegex());
    }

    private MapFilter testByTagSearchGoodExpVal(@Nonnull String operator, @Nonnull String expVal) {
        final GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setCriteriaList(
                        Collections.singletonList(filterDTO(operator, expVal, "vmsByTag")));
        inputDTO.setClassName(UIEntityType.VIRTUAL_MACHINE.apiStr());
        final List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        final SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        return byName.getSearchFilter(0).getPropertyFilter().getMapFilter();
    }

    private void testByTagSearchBadExpVal(@Nonnull String operator, @Nonnull String expVal) {
        final GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setCriteriaList(
                        Collections.singletonList(filterDTO(operator, expVal, "vmsByTag")));
        inputDTO.setClassName(UIEntityType.VIRTUAL_MACHINE.apiStr());
        final List<SearchParameters> parameters = entityFilterMapper.convertToSearchParameters(
                        inputDTO.getCriteriaList(), inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        final SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(0, byName.getSearchFilterCount());
    }

    /**
     * Test that the group filter processor creates the correct group membership filter
     * (positive match case).
     */
    @Test
    public void testGroupFilterProcessorPositive() {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
            filterDTO(EntityFilterMapper.EQUAL, "asdf", "volumeByResourceGroup"));
        final List<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
            criteriaList, UIEntityType.VIRTUAL_VOLUME.apiStr(), null);
        assertEquals(1, result.size());
        final SearchParameters params = result.get(0);

        assertEquals(SearchProtoUtil.entityTypeFilter(UIEntityType.VIRTUAL_VOLUME),
            params.getStartingFilter());
        assertEquals(1, params.getSearchFilterCount());
        final GroupMembershipFilter memFilter = params.getSearchFilter(0).getGroupMembershipFilter();
        assertEquals(GroupType.RESOURCE, memFilter.getGroupType());
        assertEquals(SearchableProperties.OID, memFilter.getGroupSpecifier().getPropertyName());
        final StringFilter specifierStringFilter = memFilter.getGroupSpecifier().getStringFilter();
        assertEquals(1, specifierStringFilter.getOptionsCount());
        assertEquals("asdf", specifierStringFilter.getOptions(0));
        assertTrue(specifierStringFilter.getPositiveMatch());
    }

    /**
     * Test that the group filter processor creates the correct group membership filter
     * (negative match case).
     */
    @Test
    public void testGroupFilterProcessorNegative() {
        final List<FilterApiDTO> criteriaList = Collections.singletonList(
            filterDTO(EntityFilterMapper.NOT_EQUAL, "asdf", "databaseServerByResourceGroupUuid"));
        final List<SearchParameters> result = entityFilterMapper.convertToSearchParameters(
            criteriaList, UIEntityType.DATABASE_SERVER.apiStr(), null);
        assertEquals(1, result.size());
        final SearchParameters params = result.get(0);

        assertEquals(SearchProtoUtil.entityTypeFilter(UIEntityType.DATABASE_SERVER),
            params.getStartingFilter());
        assertEquals(1, params.getSearchFilterCount());
        final GroupMembershipFilter memFilter = params.getSearchFilter(0).getGroupMembershipFilter();
        assertEquals(GroupType.RESOURCE, memFilter.getGroupType());
        assertEquals(SearchableProperties.OID, memFilter.getGroupSpecifier().getPropertyName());
        final StringFilter specifierStringFilter = memFilter.getGroupSpecifier().getStringFilter();
        assertEquals(1, specifierStringFilter.getOptionsCount());
        assertEquals("asdf", specifierStringFilter.getOptions(0));
        assertFalse(specifierStringFilter.getPositiveMatch());
    }
}
