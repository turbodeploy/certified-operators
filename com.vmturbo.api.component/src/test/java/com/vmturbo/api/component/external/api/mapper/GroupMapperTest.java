package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ImmutableGroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

public class GroupMapperTest {

    private static final SearchParameters.Builder SEARCH_PARAMETERS = SearchParameters.newBuilder()
                    .setStartingFilter(PropertyFilter.newBuilder()
                                    .setPropertyName("entityType").setStringFilter(StringFilter
                                                    .newBuilder()
                                                    .setStringPropertyRegex("PhysicalMachine")));

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    private final GroupUseCaseParser groupUseCaseParser = Mockito.spy(new GroupUseCaseParser(groupUseCaseFileName));

    private SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private GroupMapper groupMapper = new GroupMapper(groupUseCaseParser, supplyChainFetcherFactory,
        groupExpander, topologyProcessor);

    private static String AND = "AND";
    private static String FOO = "foo";
    private static String BAR = "bar";
    private static String VM_TYPE = "VirtualMachine";
    private static String PM_TYPE = "PhysicalMachine";
    private static String DS_TYPE = "Storage";
    private static String DISK_ARRAY_TYPE = "DiskArray";
    private static String VDC_TYPE = "VirtualDataCenter";
    private static SearchFilter DISPLAYNAME_IS_FOO =
            SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(FOO));
    private static SearchFilter DISPLAYNAME_IS_BAR =
            SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterExact(BAR));
    private static PropertyFilter TYPE_IS_VM = SearchProtoUtil.entityTypeFilter(VM_TYPE);
    private static PropertyFilter TYPE_IS_PM = SearchProtoUtil.entityTypeFilter(PM_TYPE);
    private static PropertyFilter TYPE_IS_DS = SearchProtoUtil.entityTypeFilter(DS_TYPE);
    private static PropertyFilter TYPE_IS_DISK_ARRAY = SearchProtoUtil.entityTypeFilter(DISK_ARRAY_TYPE);
    private static PropertyFilter TYPE_IS_VDC = SearchProtoUtil.entityTypeFilter(VDC_TYPE);
    private static SearchFilter PRODUCES_VMS = SearchProtoUtil.searchFilterTraversal(SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, VM_TYPE));
    private static SearchFilter PRODUCES_ONE_HOP = SearchProtoUtil.searchFilterTraversal(SearchProtoUtil.numberOfHops(TraversalDirection.PRODUCES, 1));
    private static SearchFilter PRODUCES_ST = SearchProtoUtil.searchFilterTraversal(SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, DS_TYPE));

    /**
     * Test static group converting GroupApiDTO to GroupInfo
     */
    @Test
    public void testToGroupInfoStaticGroup() {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = true;
        final Optional<String> uuid = Optional.of("123");
        final GroupApiDTO groupDto = new GroupApiDTO();
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setMemberUuidList(Collections.singletonList(uuid.get()));

        final GroupInfo g = groupMapper.toGroupInfo(groupDto);

        assertEquals(displayName, g.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), g.getEntityType());
        assertEquals(SelectionCriteriaCase.STATIC_GROUP_MEMBERS, g.getSelectionCriteriaCase());
        assertEquals(Collections.singletonList(123L), g.getStaticGroupMembers().getStaticMemberOidsList());
    }

    /**
     * Test PM dynamic group which filtered only by PM name
     */
    @Test
    public void testToGroupInfoDynamicGroupByPM() {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.PHYSICAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(GroupMapper.REGEX_MATCH);
        filterApiDTOFirst.setExpVal("PM#1");
        filterApiDTOFirst.setFilterType("pmsByName");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupInfo groupInfo = groupMapper.toGroupInfo(groupDto);

        assertEquals(displayName, groupInfo.getName());
        assertEquals(EntityType.PHYSICAL_MACHINE.getNumber(), groupInfo.getEntityType());
        assertEquals(SelectionCriteriaCase.SEARCH_PARAMETERS_COLLECTION, groupInfo.getSelectionCriteriaCase());
        // Verify the first search parameters' starting filter is PM entity
        assertEquals("entityType", groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.typeNumber(),
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for PM
        assertEquals("displayName", groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("^PM#1$", groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getStringFilter().getStringPropertyRegex());
        assertTrue(
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());
    }

    /**
     * Test VM dynamic group which filtered by VM name and PM name
     */
    @Test
    public void testToGroupInfoDynamicGroupByVM() {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(GroupMapper.REGEX_MATCH);
        filterApiDTOFirst.setExpVal("VM#1");
        filterApiDTOFirst.setFilterType("vmsByName");
        final FilterApiDTO filterApiDTOSecond = new FilterApiDTO();
        filterApiDTOSecond.setExpType(GroupMapper.REGEX_NO_MATCH);
        filterApiDTOSecond.setExpVal("PM#2");
        filterApiDTOSecond.setFilterType("vmsByPMName");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst, filterApiDTOSecond);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupInfo groupInfo = groupMapper.toGroupInfo(groupDto);

        assertEquals(displayName, groupInfo.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), groupInfo.getEntityType());
        assertEquals(SelectionCriteriaCase.SEARCH_PARAMETERS_COLLECTION, groupInfo.getSelectionCriteriaCase());
        assertEquals(2, groupInfo.getSearchParametersCollection().getSearchParametersCount());
        SearchParameters firstSearchParameters = groupInfo.getSearchParametersCollection().getSearchParameters(0);
        SearchParameters secondSearchParameters = groupInfo.getSearchParametersCollection().getSearchParameters(1);
        // Verify the first search parameters' starting filter is VM entity
        assertEquals("entityType", firstSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
            firstSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for VM
        assertEquals("displayName", firstSearchParameters.getSearchFilter(0)
                .getPropertyFilter().getPropertyName());
        assertEquals("^VM#1$", firstSearchParameters.getSearchFilter(0).getPropertyFilter()
                .getStringFilter().getStringPropertyRegex());
        assertTrue(
            firstSearchParameters.getSearchFilter(0)
                .getPropertyFilter().getStringFilter().getPositiveMatch());
        // Verify the second search parameters' starting filter is PM entity
        assertEquals("entityType", secondSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.typeNumber(),
            secondSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search filter is ByName search for PM
        assertEquals("displayName", secondSearchParameters.getSearchFilter(0)
                .getPropertyFilter().getPropertyName());
        assertEquals("^PM#2$", secondSearchParameters.getSearchFilter(0).getPropertyFilter()
                .getStringFilter().getStringPropertyRegex());
        assertFalse(
            secondSearchParameters.getSearchFilter(0).getPropertyFilter()
                .getStringFilter().getPositiveMatch());
        // Verify the second search filter is traversal search and hops number is 1
        assertEquals(TraversalDirection.PRODUCES, secondSearchParameters.getSearchFilter(1)
                .getTraversalFilter().getTraversalDirection());
        assertEquals(1, secondSearchParameters.getSearchFilter(1).getTraversalFilter()
                .getStoppingCondition().getNumberHops());
        // Verify the third search filter is by Entity search for VM
        assertEquals("entityType", secondSearchParameters.getSearchFilter(2)
                .getPropertyFilter().getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
            secondSearchParameters.getSearchFilter(2).getPropertyFilter().getNumericFilter().getValue());
    }

    /**
     * Test converting dynamic group info which only has starting filter to groupApiDTO
     */
    @Test
    public void testToGroupApiDTOOnlyWithStartingFilter() {
        final String displayName = "group-foo";
        final int groupType = EntityType.PHYSICAL_MACHINE.getNumber();
        final long oid = 123L;

        final Group group = Group.newBuilder()
            .setId(oid)
            .setType(Group.Type.GROUP)
            .setGroup(GroupInfo.newBuilder()
                .setName(displayName)
                .setEntityType(groupType)
                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                    .addSearchParameters(SEARCH_PARAMETERS.setSourceFilterSpecs(buildFilterSpecs(
                        "pmsByName", "foo", "foo")))))
            .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group)
            .members(ImmutableSet.of(1L))
            .entities(ImmutableSet.of(2L, 3L))
            .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);

        assertThat(dto.getUuid(), is(Long.toString(oid)));
        assertThat(dto.getDisplayName(), is(displayName));
        assertThat(dto.getGroupType(), is(UIEntityType.PHYSICAL_MACHINE.apiStr()));
        assertFalse(dto.getIsStatic());
        assertThat(dto.getClassName(), is(StringConstants.GROUP));
        assertThat(dto.getCriteriaList().get(0).getFilterType(), is("pmsByName"));
        assertThat(dto.getEnvironmentType(), is(EnvironmentType.ONPREM));
        assertThat(dto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(dto.getMembersCount(), is(1));
        assertThat(dto.getEntitiesCount(), is(2));
    }

    /**
     *  Test converting dynamic group info which has multiple search parameters to groupApiDTO
     */
    @Test
    public void testToGroupApiDTOWithMultipleSearchParameters() {
        final String displayName = "group-foo";
        final int groupType = EntityType.VIRTUAL_MACHINE.getNumber();
        final Boolean isStatic = false;
        final long oid = 123L;

        final SearchParameters.Builder vmParameters = getVmParameters();
        final SearchParameters.Builder pmParameters = getVmByPmParameters();

        final Group group = Group.newBuilder()
                .setId(oid)
                .setType(Group.Type.GROUP)
                .setGroup(GroupInfo.newBuilder()
                    .setName(displayName)
                    .setEntityType(groupType)
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(vmParameters)
                            .addSearchParameters(pmParameters)))
                .build();

        when(groupExpander.getMembersForGroup(group))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);

        assertEquals(Long.toString(oid), dto.getUuid());
        assertEquals(displayName, dto.getDisplayName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(isStatic, dto.getIsStatic());
        assertEquals(StringConstants.GROUP, dto.getClassName());
        assertEquals("vmsByName", dto.getCriteriaList().get(0).getFilterType());
        assertEquals(GroupMapper.REGEX_MATCH, dto.getCriteriaList().get(0).getExpType());
        assertEquals("^VM#2$", dto.getCriteriaList().get(0).getExpVal());
        assertEquals("vmsByPMName", dto.getCriteriaList().get(1).getFilterType());
        assertEquals(GroupMapper.REGEX_MATCH, dto.getCriteriaList().get(1).getExpType());
        assertEquals("^PM#1$", dto.getCriteriaList().get(1).getExpVal());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    private SearchParameters.Builder getVmParameters() {
        return SearchParameters.newBuilder().setSourceFilterSpecs(buildFilterSpecs("vmsByName",
                        GroupMapper.REGEX_MATCH, "^VM#2$")).setStartingFilter(
                        PropertyFilter.newBuilder().setPropertyName("entityType").setNumericFilter(
                                        NumericFilter.newBuilder().setComparisonOperator(
                                                        ComparisonOperator.EQ).setValue(10)))
                        .addSearchFilter(SearchFilter.newBuilder().setPropertyFilter(
                                        PropertyFilter.newBuilder().setPropertyName("displayName")
                                                        .setStringFilter(StringFilter.newBuilder()
                                                                        .setStringPropertyRegex("^VM#2$"))));
    }

    private SearchParameters.Builder getVmByPmParameters() {
        return SearchParameters.newBuilder().setSourceFilterSpecs(buildFilterSpecs("vmsByPMName",
                        GroupMapper.REGEX_MATCH, "^PM#1$"))
                        .setStartingFilter(PropertyFilter.newBuilder().setPropertyName("entityType")
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                        .setComparisonOperator(ComparisonOperator.EQ)
                                                        .setValue(14))).addSearchFilter(
                                        SearchFilter.newBuilder().setPropertyFilter(
                                                        PropertyFilter.newBuilder()
                                                                        .setPropertyName("displayName")
                                                                        .setStringFilter(
                                                                        StringFilter.newBuilder()
                                                                        .setStringPropertyRegex("^PM#1$"))))
                        .addSearchFilter(SearchFilter.newBuilder().setTraversalFilter(
                                        TraversalFilter.newBuilder().setTraversalDirection(
                                                        TraversalDirection.PRODUCES)
                                                        .setStoppingCondition(StoppingCondition
                                                                        .newBuilder()
                                                                        .setNumberHops(1))));
    }

    private SearchParameters.FilterSpecs buildFilterSpecs(@Nonnull String filterType,
                    @Nonnull String expType, @Nonnull String expValue) {
        return SearchParameters.FilterSpecs.newBuilder().setFilterType(filterType)
                        .setExpressionType(expType).setExpressionValue(expValue).build();
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertTrue(byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());
    }

    @Test
    public void testByVMGuestOsType() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, "Linux",
                "vmsByGuestName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());

        List<SearchFilter> expectedSearchFilter = Lists.newArrayList(SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("virtualMachineInfoRepoDTO")
                        .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(PropertyFilter.newBuilder()
                                        .setPropertyName("guestOsType")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .addOptions("Linux")
                                                .setPositiveMatch(true)
                                                .setCaseSensitive(false)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    @Test
    public void testByVMNumCpus() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, "3",
                "vmsByNumCPUs"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
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
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.EQ)
                                                .setValue(3)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    @Test
    public void testByVMCommodityVMemCapacity() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.GREATER_THAN, "1024",
                "vmsByMem"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());

        List<SearchFilter> expectedSearchFilter = Lists.newArrayList(SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("commoditySoldList")
                        .setListFilter(ListFilter.newBuilder()
                                .setObjectFilter(ObjectFilter.newBuilder()
                                        .addFilters(PropertyFilter.newBuilder()
                                                .setPropertyName("type")
                                                .setStringFilter(StringFilter.newBuilder()
                                                        .addOptions("VMem")
                                                        .setPositiveMatch(true)
                                                        .setCaseSensitive(false)
                                                        .build())
                                                .build())
                                        .addFilters(PropertyFilter.newBuilder()
                                                .setPropertyName("capacity")
                                                .setNumericFilter(NumericFilter.newBuilder()
                                                        .setComparisonOperator(ComparisonOperator.GT)
                                                        .setValue(1024)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

        assertThat(byName.getSearchFilterList(), is(expectedSearchFilter));
    }

    /**
     * Verify that exception will be thrown for unknown filter type.
     */
    @Test
    public void testNotExistingFilter() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO,
                        "notExistingFilter"));
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Not existing filter type provided: notExistingFilter");
        groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearchNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.NOT_EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(FOO, byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0));
        assertFalse(byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());
    }

    @Test
    public void testByStateSearchEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL,
                        "IDLE", "vmsByState"));
        final List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO,
                        inputDTO.getClassName(), null);
        final PropertyFilter propertyFilter = parameters.get(0).getSearchFilter(0).getPropertyFilter();
        Assert.assertEquals(1, parameters.size());
        Assert.assertEquals(TYPE_IS_VM, parameters.get(0).getStartingFilter());
        Assert.assertEquals("state", propertyFilter.getPropertyName());
        Assert.assertEquals("IDLE", propertyFilter.getStringFilter().getOptions(0));
    }

    @Test
    public void testByStateSearchNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE, filterDTO(GroupMapper.NOT_EQUAL,
                        "ACTIVE", "pmsByState"));
        final List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO,
                        inputDTO.getClassName(), null);
        final PropertyFilter propertyFilter = parameters.get(0).getSearchFilter(0).getPropertyFilter();
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
            filterDTO(GroupMapper.NOT_EQUAL, FOO, "vmsByName"),
            filterDTO(GroupMapper.NOT_EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
        SearchParameters firstByName = parameters.get(0);
        assertEquals(TYPE_IS_VM, firstByName.getStartingFilter());
        assertEquals(1, firstByName.getSearchFilterCount());
        assertEquals(FOO, firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0));
        assertFalse(
                firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());

        SearchParameters secondByName = parameters.get(1);
        assertEquals(TYPE_IS_PM, secondByName.getStartingFilter());
        assertEquals(3, secondByName.getSearchFilterCount());
        assertEquals(BAR, secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0));
        assertFalse(
                secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());
    }

    /**
     * Verify that a byName criterion combined with a traversal with one hop are converted properly.
     */
    @Test
    public void testByNameAndTraversalHopSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByName"), filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
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
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byPMNameByVMName.getSearchFilter(2));
    }

    @Test
    public void testByTraversalHopNumConnectedVMs() {
        GroupApiDTO inputDTO = groupApiDTO(AND, PM_TYPE, filterDTO(GroupMapper.EQUAL, "3",
                "pmsByNumVms"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO,
                inputDTO.getClassName(), null);

        assertEquals(1, parameters.size());
        SearchParameters byTraversalHopNumConnectedVMs = parameters.get(0);
        assertEquals(TYPE_IS_PM, byTraversalHopNumConnectedVMs.getStartingFilter());
        assertEquals(1, byTraversalHopNumConnectedVMs.getSearchFilterCount());

        SearchFilter hopNumConnectedVMs = SearchProtoUtil.searchFilterTraversal(
                createNumHopsNumConnectedVerticesFilter(TraversalDirection.PRODUCES, 1,
                        ComparisonOperator.EQ, 3, EntityType.VIRTUAL_MACHINE.getNumber()));
        assertEquals(hopNumConnectedVMs, byTraversalHopNumConnectedVMs.getSearchFilter(0));
    }

    private static TraversalFilter createNumHopsNumConnectedVerticesFilter(
            TraversalDirection direction, int hops, ComparisonOperator operator, long value,
            int entityType) {
        StoppingCondition numHops = StoppingCondition.newBuilder()
                .setNumberHops(hops)
                .setVerticesCondition(VerticesCondition.newBuilder()
                        .setNumConnectedVertices(NumericFilter.newBuilder()
                                .setComparisonOperator(operator)
                                .setValue(value)
                                .build())
                        .setEntityType(entityType)
                        .build())
                .build();
        return TraversalFilter.newBuilder()
                .setTraversalDirection(direction)
                .setStoppingCondition(numHops)
                .build();
    }

    @Test
    public void testVmsByDiskArrayNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByDiskArrayName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), BAR);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_DISK_ARRAY, byName.getStartingFilter());
        assertEquals(5, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ST, byName.getSearchFilter(1));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(2));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(3));
        assertEquals(DISPLAYNAME_IS_BAR, byName.getSearchFilter(4));
    }

    @Test
    public void testVmsByVdcNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByVDC"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VDC, byName.getStartingFilter());
        assertEquals(3, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byName.getSearchFilter(1));
        assertEquals(SearchProtoUtil.searchFilterProperty(TYPE_IS_VM), byName.getSearchFilter(2));
    }

    @Test
    public void testVmsByVdcNestedNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByDCnested"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
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
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
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
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByName"),
                filterDTO(GroupMapper.EQUAL, BAR, "vmsByStorage"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
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
                filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"),
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByStorage"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(2, parameters.size());
    }

    @Test
    public void testVmsByClusterNameToSearchParameters() {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByClusterName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(groupDto, groupDto.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);
        // verify that the Cluster Membership Filter was created
        assertTrue(param.getSearchFilter(0).hasClusterMembershipFilter());
        ClusterMembershipFilter clusterMembershipFilter = param.getSearchFilter(0).getClusterMembershipFilter();
        // verify that we are looking for clusters with name FOO
        assertEquals(
                "^" + FOO + "$",
                clusterMembershipFilter.getClusterSpecifier().getStringFilter().getStringPropertyRegex());

        // test conversion from GroupApiDTO back to FilterApiDTO
        groupDto.setDisplayName("TestGroupDto");
        groupDto.setGroupType("VirtualMachine");
        groupDto.setIsStatic(false);
        final GroupInfo groupInfo = groupMapper.toGroupInfo(groupDto);

        List<FilterApiDTO> filterApiDTOS = groupMapper.convertToFilterApis(groupInfo);
        assertEquals(1, filterApiDTOS.size());
        // verify that we have rebuilt the original vmsByClusterName
        FilterApiDTO vmsByClusterNameFilter = filterApiDTOS.get(0);
        assertEquals("vmsByClusterName", vmsByClusterNameFilter.getFilterType());
        assertEquals("EQ", vmsByClusterNameFilter.getExpType());
        assertEquals(FOO, vmsByClusterNameFilter.getExpVal());
    }

    @Test
    public void testVmsWithNameQuery() {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE);
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(groupDto, groupDto.getClassName(), FOO);
        assertThat(parameters.size(), is(1));
        SearchParameters param = parameters.get(0);
        assertThat(param.getStartingFilter(), is(TYPE_IS_VM));
        assertThat(param.getSearchFilterCount(), is(1));
        assertThat(param.getSearchFilter(0),
            is(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterRegex(".*" + FOO + ".*"))));
    }

    /**
     * Tests converting of searchParameters of GroupInfo to filterApiDto.
     */
    @Test
    public void testConvertToFilterApis() {
        final GroupInfo groupInfo = GroupInfo.newBuilder().setSearchParametersCollection(
                        SearchParametersCollection.newBuilder().addSearchParameters(
                                        SEARCH_PARAMETERS.setSourceFilterSpecs(buildFilterSpecs(
                                                        "filterType",
                                                        "expType", "expValue")))
                                        .build()).build();
        final List<FilterApiDTO> filterApiDTOS = groupMapper.convertToFilterApis(groupInfo);
        Assert.assertEquals("filterType", filterApiDTOS.get(0).getFilterType());
        Assert.assertEquals("expType", filterApiDTOS.get(0).getExpType());
        Assert.assertEquals("expValue", filterApiDTOS.get(0).getExpVal());
    }

    private FilterApiDTO filterDTO(String expType, String expVal, String filterType) {
        FilterApiDTO filter = new FilterApiDTO();
        filter.setExpType(expType);
        filter.setExpVal(expVal);
        filter.setFilterType(filterType);
        return filter;
    }

    private GroupApiDTO groupApiDTO(String logicalOperator, String className, FilterApiDTO...filters) {
        GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setLogicalOperator(logicalOperator);
        inputDTO.setClassName(className);
        inputDTO.setCriteriaList(Arrays.asList(filters));
        return inputDTO;
    }

    @Test
    public void testMapComputeCluster() {
        final Group computeCluster = Group.newBuilder()
                .setId(7L)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setName("cool boy")
                        .setClusterType(Type.COMPUTE)
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(10L)))
                .build();

        when(groupExpander.getMembersForGroup(computeCluster))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(computeCluster)
                .members(GroupProtoUtil.getClusterMembers(computeCluster))
                .entities(GroupProtoUtil.getClusterMembers(computeCluster))
                .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(computeCluster);
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool boy", dto.getDisplayName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testMapStorageCluster() {
        final Group storageCluster = Group.newBuilder()
                .setId(7L)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setName("cool girl")
                        .setClusterType(Type.STORAGE)
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(10L)))
                .build();

        when(groupExpander.getMembersForGroup(storageCluster))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(storageCluster)
                .members(GroupProtoUtil.getClusterMembers(storageCluster))
                .entities(GroupProtoUtil.getClusterMembers(storageCluster))
                .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(storageCluster);
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.STORAGE_CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool girl", dto.getDisplayName());
        assertEquals(UIEntityType.STORAGE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testToTempGroupProtoGlobalScope() throws OperationFailedException, InvalidOperationException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        final SupplyChainNodeFetcherBuilder fetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(7L).build())
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        TempGroupInfo groupInfo = groupMapper.toTempGroupProto(apiDTO);
        assertThat(groupInfo.getEntityType(), is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(groupInfo.getMembers().getStaticMemberOidsList(), containsInAnyOrder(7L));
        assertThat(groupInfo.getName(), is("foo"));
        assertTrue(groupInfo.getIsGlobalScopeGroup());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoNotGlobalScope() throws OperationFailedException, InvalidOperationException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList("1"));

        final SupplyChainNodeFetcherBuilder fetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(
                ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addMemberOids(7L).build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        TempGroupInfo groupInfo = groupMapper.toTempGroupProto(apiDTO);
        assertThat(groupInfo.getEntityType(), is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(groupInfo.getMembers().getStaticMemberOidsList(), containsInAnyOrder(7L));
        assertThat(groupInfo.getName(), is("foo"));
        assertFalse(groupInfo.getIsGlobalScopeGroup());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoUuidList() throws InvalidOperationException, OperationFailedException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        // One valid, one invalid.
        apiDTO.setMemberUuidList(Lists.newArrayList("1", "foo"));

        final TempGroupInfo groupInfo = groupMapper.toTempGroupProto(apiDTO);
        assertThat(groupInfo.getEntityType(), is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(groupInfo.getMembers().getStaticMemberOidsList(), containsInAnyOrder(1L));
        assertThat(groupInfo.getName(), is("foo"));
        assertFalse(groupInfo.getIsGlobalScopeGroup());
    }

    @Test
    public void testToTempGroupProtoUuidListInsideScope() throws OperationFailedException, InvalidOperationException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList("1"));
        // 7 should be in the scope, 6 is not in the scope, and foo is an illegal value.
        apiDTO.setMemberUuidList(Lists.newArrayList("7", "6", "foo"));

        final SupplyChainNodeFetcherBuilder fetcherBuilder =
            ApiTestUtils.mockNodeFetcherBuilder(ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(7L).build())
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        TempGroupInfo groupInfo = groupMapper.toTempGroupProto(apiDTO);
        assertThat(groupInfo.getEntityType(), is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(groupInfo.getMembers().getStaticMemberOidsList(), containsInAnyOrder(7L));
        assertThat(groupInfo.getName(), is("foo"));
        assertFalse(groupInfo.getIsGlobalScopeGroup());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testMapTempGroup() {
        final Group group = Group.newBuilder()
            .setType(Group.Type.TEMP_GROUP)
            .setId(8L)
            .setOrigin(Origin.USER)
            .setTempGroup(TempGroupInfo.newBuilder()
                .setName("foo")
                .setEntityType(10)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L)))
            .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group)
            .members(Collections.singleton(1L))
            // Temp groups will never have different entity count, but we want to check the
            // entity count gets set from the right field in GroupAndMembers.
            .entities(ImmutableList.of(1L, 2L))
            .build());

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group);

        assertThat(mappedDto.getTemporary(), is(true));
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(2));
        assertThat(mappedDto.getMembersCount(), is(1));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(mappedDto.getGroupType(), is(VM_TYPE));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.ONPREM));
        assertThat(mappedDto.getClassName(), is("Group"));
    }

    @Test
    public void testMapTempGroupCloud() {
        final Group group = Group.newBuilder()
            .setType(Group.Type.TEMP_GROUP)
            .setId(8L)
            .setOrigin(Origin.USER)
            .setTempGroup(TempGroupInfo.newBuilder()
                .setName("foo")
                .setEntityType(10)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L)))
            .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group)
            .entities(GroupProtoUtil.getStaticMembers(group).get())
            .members(GroupProtoUtil.getStaticMembers(group).get())
            .build());

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.CLOUD);

        assertThat(mappedDto.getTemporary(), is(true));
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(1));
        assertThat(mappedDto.getMembersCount(), is(1));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(mappedDto.getGroupType(), is(VM_TYPE));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(mappedDto.getClassName(), is("Group"));
    }

    @Test
    public void testMapTempGroupONPREM() {
        final Group group = Group.newBuilder()
                .setType(Group.Type.TEMP_GROUP)
                .setId(8L)
                .setOrigin(Origin.USER)
                .setTempGroup(TempGroupInfo.newBuilder()
                        .setName("foo")
                        .setEntityType(10)
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(1L)))
                .build();

        when(groupExpander.getMembersForGroup(group))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(Collections.singleton(1L))
                // Return a different entity set to make sure it gets used for the entity count.
                .entities(ImmutableSet.of(2L, 3L))
                .build());

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);

        assertThat(mappedDto.getTemporary(), is(true));
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(2));
        assertThat(mappedDto.getMembersCount(), is(1));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(mappedDto.getGroupType(), is(VM_TYPE));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.ONPREM));
        assertThat(mappedDto.getClassName(), is("Group"));
    }

    @Test
    public void testMapTempGroupHybridWithAndWithoutCloudTargets() throws Exception {
        final Group group = Group.newBuilder()
            .setType(Group.Type.TEMP_GROUP)
            .setId(8L)
            .setOrigin(Origin.USER)
            .setTempGroup(TempGroupInfo.newBuilder()
                .setName("foo")
                .setEntityType(10)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L)))
            .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group)
            .entities(GroupProtoUtil.getStaticMembers(group).get())
            .members(GroupProtoUtil.getStaticMembers(group).get())
            .build());

        // mock only one vcenter target
        final long probeId = 111L;
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(topologyProcessor.getAllTargets()).thenReturn(ImmutableSet.of(targetInfo));
        final ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);
        when(probeInfo.getId()).thenReturn(probeId);
        when(probeInfo.getType()).thenReturn(SDKProbeType.VCENTER.getProbeType());
        when(topologyProcessor.getAllProbes()).thenReturn(ImmutableSet.of(probeInfo));

        // if no cloud targets, it should be ONPREM
        GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.HYBRID);
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.ONPREM));

        // mock one cloud target and expect the environment type to be HYBRID
        when(probeInfo.getType()).thenReturn(SDKProbeType.AWS.getProbeType());
        mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.HYBRID);
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.HYBRID));
    }

    @Test
    public void testStaticGroupMembersCount() {
        final Group group = Group.newBuilder()
                .setId(7L)
                .setType(Group.Type.GROUP)
                .setGroup(GroupInfo.newBuilder()
                    .setName("group1")
                    .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                    .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                            .addAllStaticMemberOids(Arrays.asList(10L, 20L))
                            .build())
                    .build())
                .build();

        // We use the groupExpander to get members for both static and dynamic groups.
        final Set<Long> members = ImmutableSet.of(10L, 20L);
        when(groupExpander.getMembersForGroup(group))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(members)
                .entities(members)
                .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);
        assertEquals("7", dto.getUuid());
        assertEquals(true, dto.getIsStatic());
        assertThat(dto.getEntitiesCount(), is(2));
        assertThat(dto.getMemberUuidList(), containsInAnyOrder("10", "20"));
    }

    @Test
    public void testDynamicGroupMembersCount() {
        final Group group = Group.newBuilder()
                .setId(7L)
                .setType(Group.Type.GROUP)
                .setGroup(GroupInfo.newBuilder()
                    .setName("group1")
                    .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addSearchParameters(SEARCH_PARAMETERS)
                         .build())
                    .build())
                .build();
        final Set<Long> members = ImmutableSet.of(10L, 20L, 30L);
        when(groupExpander.getMembersForGroup(group))
            .thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(members)
                .entities(members)
                .build());

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);
        assertThat(dto.getEntitiesCount(), is(3));
        assertThat(dto.getMemberUuidList(),
                containsInAnyOrder("10", "20", "30"));
    }

    /**
     * Test a dynamic filter with exact string equality.
     */
    @Test
    public void testExactStringMatchingFilterPositive() {
        testExactStringMatchingFilter(true);
    }

    /**
     * Test a dynamic filter with negated exact string equality.
     */
    @Test
    public void testExactStringMatchingFilterNegative() {
        testExactStringMatchingFilter(false);
    }

    private void testExactStringMatchingFilter(boolean positiveMatching) {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(positiveMatching ? GroupMapper.EQUAL : GroupMapper.NOT_EQUAL);
        filterApiDTOFirst.setExpVal("Idle");
        filterApiDTOFirst.setFilterType("vmsByState");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupInfo groupInfo = groupMapper.toGroupInfo(groupDto);

        assertEquals(displayName, groupInfo.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), groupInfo.getEntityType());
        assertEquals(
            SelectionCriteriaCase.SEARCH_PARAMETERS_COLLECTION, groupInfo.getSelectionCriteriaCase());
        // Verify the first search parameters' starting filter is VM entity
        assertEquals(
            "entityType",
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getStartingFilter().getPropertyName());
        assertEquals(
            UIEntityType.VIRTUAL_MACHINE.typeNumber(),
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are by state search for VM
        assertEquals("state", groupInfo.getSearchParametersCollection().getSearchParameters(0)
            .getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("Idle", groupInfo.getSearchParametersCollection().getSearchParameters(0)
            .getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0));
        assertEquals(
            positiveMatching,
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getStringFilter().getPositiveMatch());
    }

    /**
     * Verify that a extact string match by-tags criterion is converted properly.
     */
    @Test
    public void testByTagSearch() {
        final GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setCriteriaList(
                Collections.singletonList(filterDTO(GroupMapper.NOT_EQUAL, "k=v1|k=v2", "vmsByTag")));
        inputDTO.setClassName(UIEntityType.VIRTUAL_MACHINE.apiStr());
        final List<SearchParameters> parameters =
                groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        final SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        final MapFilter tagFilter = byName.getSearchFilter(0).getPropertyFilter().getMapFilter();
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
        final GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setCriteriaList(
                Collections.singletonList(filterDTO(GroupMapper.REGEX_MATCH, "k=.*a.*", "vmsByTag")));
        inputDTO.setClassName(UIEntityType.VIRTUAL_MACHINE.apiStr());
        final List<SearchParameters> parameters =
                groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), null);
        assertEquals(1, parameters.size());
        final SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        final MapFilter tagFilter = byName.getSearchFilter(0).getPropertyFilter().getMapFilter();
        assertEquals("k", tagFilter.getKey());
        assertEquals(0, tagFilter.getValuesCount());
        assertTrue(tagFilter.getPositiveMatch());
        assertEquals("^.*a.*$", tagFilter.getRegex());
    }

    /**
     * Tests translation of a dynamic nested group with two filter properties
     * (one for name and one for tags) from the API structure to the internal
     * nested group structure.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testDynamicNestedGroupTranslation() throws Exception {
        final FilterApiDTO nameFilter = new FilterApiDTO();
        nameFilter.setFilterType(GroupMapper.CLUSTERS_FILTER_TYPE);
        nameFilter.setExpType(GroupMapper.EQUAL);
        nameFilter.setExpVal("the name");

        final FilterApiDTO tagsFilter = new FilterApiDTO();
        tagsFilter.setFilterType(GroupMapper.CLUSTERS_BY_TAGS_FILTER_TYPE);
        tagsFilter.setExpType(GroupMapper.EQUAL);
        tagsFilter.setExpVal("key=value1|key=value2");

        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setGroupType(StringConstants.CLUSTER);
        groupApiDTO.setIsStatic(false);
        groupApiDTO.setCriteriaList(ImmutableList.of(nameFilter, tagsFilter));
        groupApiDTO.setDisplayName("group of clusters");

        final NestedGroupInfo result = groupMapper.toNestedGroupInfo(groupApiDTO);
        final PropertyFilter namePropertyFilter =
                result.getPropertyFilterList().getPropertyFilters(0);
        final PropertyFilter tagsPropertyFilter =
                result.getPropertyFilterList().getPropertyFilters(1);

        Assert.assertEquals(
                PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.DISPLAY_NAME_ATTR)
                        .setStringFilter(
                                StringFilter.newBuilder()
                                        .setStringPropertyRegex("^the name$")
                                        .setPositiveMatch(true)
                                        .setCaseSensitive(false)
                                        .build())
                        .build(),
                namePropertyFilter);
        Assert.assertEquals(
                PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.TAGS_ATTR)
                        .setMapFilter(
                                MapFilter.newBuilder()
                                        .setKey("key")
                                        .addValues("value1")
                                        .addValues("value2")
                                        .setPositiveMatch(true)
                                        .build())
                        .build(),
                tagsPropertyFilter);
    }
}
