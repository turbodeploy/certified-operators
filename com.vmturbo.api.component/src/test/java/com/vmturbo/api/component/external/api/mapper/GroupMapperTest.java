package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GroupMapperTest {
    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    private final GroupUseCaseParser groupUseCaseParser = new GroupUseCaseParser(groupUseCaseFileName);

    private SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    private GroupMapper groupMapper =
        new GroupMapper(groupUseCaseParser, supplyChainFetcherFactory, groupExpander);

    private static String AND = "AND";
    private static String FOO = "foo";
    private static String BAR = "bar";
    private static String VM_TYPE = "VirtualMachine";
    private static String PM_TYPE = "PhysicalMachine";
    private static String DS_TYPE = "Storage";
    private static SearchFilter DISPLAYNAME_IS_FOO = SearchMapper.searchFilterProperty(SearchMapper.nameFilter(FOO));
    private static SearchFilter DISPLAYNAME_IS_BAR = SearchMapper.searchFilterProperty(SearchMapper.nameFilter(BAR));
    private static PropertyFilter TYPE_IS_VM = SearchMapper.entityTypeFilter(VM_TYPE);
    private static PropertyFilter TYPE_IS_PM = SearchMapper.entityTypeFilter(PM_TYPE);
    private static PropertyFilter TYPE_IS_DS = SearchMapper.entityTypeFilter(DS_TYPE);
    private static SearchFilter PRODUCES_VMS = SearchMapper.searchFilterTraversal(SearchMapper.traverseToType(TraversalDirection.PRODUCES, VM_TYPE));
    private static SearchFilter PRODUCES_ONE_HOP = SearchMapper.searchFilterTraversal(SearchMapper.numberOfHops(TraversalDirection.PRODUCES, 1));

    /**
     * Test static group converting GroupApiDTO to GroupInfo
     */
    @Test
    public void testToGroupInfoStaticGroup() {
        final String displayName = "group-foo";
        final String groupType = ServiceEntityMapper.UIEntityType.VIRTUAL_MACHINE.getValue();
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
        final String groupType = ServiceEntityMapper.UIEntityType.PHYSICAL_MACHINE.getValue();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(GroupMapper.EQUAL);
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
        assertEquals(ServiceEntityMapper.fromUIEntityType("PhysicalMachine"),
            groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for PM
        assertEquals("displayName", groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("PM#1", groupInfo.getSearchParametersCollection().getSearchParameters(0)
                .getSearchFilter(0).getPropertyFilter().getStringFilter().getStringPropertyRegex());
    }

    /**
     * Test VM dynamic group which filtered by VM name and PM name
     */
    @Test
    public void testToGroupInfoDynamicGroupByVM() {
        final String displayName = "group-foo";
        final String groupType = ServiceEntityMapper.UIEntityType.VIRTUAL_MACHINE.getValue();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(GroupMapper.EQUAL);
        filterApiDTOFirst.setExpVal("VM#1");
        filterApiDTOFirst.setFilterType("vmsByName");
        final FilterApiDTO filterApiDTOSecond = new FilterApiDTO();
        filterApiDTOSecond.setExpType(GroupMapper.EQUAL);
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
        assertEquals(ServiceEntityMapper.fromUIEntityType("VirtualMachine"),
            firstSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for VM
        assertEquals("displayName", firstSearchParameters.getSearchFilter(0)
                .getPropertyFilter().getPropertyName());
        assertEquals("VM#1", firstSearchParameters.getSearchFilter(0).getPropertyFilter()
                .getStringFilter().getStringPropertyRegex());
        // Verify the second search parameters' starting filter is PM entity
        assertEquals("entityType", secondSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(ServiceEntityMapper.fromUIEntityType("PhysicalMachine"),
            secondSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search filter is ByName search for PM
        assertEquals("displayName", secondSearchParameters.getSearchFilter(0)
                .getPropertyFilter().getPropertyName());
        assertEquals("PM#2", secondSearchParameters.getSearchFilter(0).getPropertyFilter()
                .getStringFilter().getStringPropertyRegex());
        // Verify the second search filter is traversal search and hops number is 1
        assertEquals(TraversalDirection.PRODUCES, secondSearchParameters.getSearchFilter(1)
                .getTraversalFilter().getTraversalDirection());
        assertEquals(1, secondSearchParameters.getSearchFilter(1).getTraversalFilter()
                .getStoppingCondition().getNumberHops());
        // Verify the third search filter is by Entity search for VM
        assertEquals("entityType", secondSearchParameters.getSearchFilter(2)
                .getPropertyFilter().getPropertyName());
        assertEquals(ServiceEntityMapper.fromUIEntityType("VirtualMachine"),
            secondSearchParameters.getSearchFilter(2).getPropertyFilter().getNumericFilter().getValue());
    }

    /**
     * Test converting dynamic group info which only has starting filter to groupApiDTO
     */
    @Test
    public void testToGroupApiDTOOnlyWithStartingFilter() {
        final String displayName = "group-foo";
        final int groupType = EntityType.PHYSICAL_MACHINE.getNumber();
        final Boolean isStatic = false;
        final long oid = 123L;

        final Group group = Group.newBuilder()
            .setId(oid)
            .setType(Group.Type.GROUP)
            .setGroup(GroupInfo.newBuilder()
                .setName(displayName)
                .setEntityType(groupType)
                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("entityType")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("PhysicalMachine"))))))

            .build();

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);

        assertEquals(Long.toString(oid), dto.getUuid());
        assertEquals(displayName, dto.getDisplayName());
        assertEquals(ServiceEntityMapper.UIEntityType.PHYSICAL_MACHINE.getValue(), dto.getGroupType());
        assertEquals(isStatic, dto.getIsStatic());
        assertEquals(GroupMapper.GROUP, dto.getClassName());
        assertEquals("pmsByName", dto.getCriteriaList().get(0).getFilterType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
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

        final Group group = Group.newBuilder()
                .setId(oid)
                .setType(Group.Type.GROUP)
                .setGroup(GroupInfo.newBuilder()
                    .setName(displayName)
                    .setEntityType(groupType)
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(SearchParameters.newBuilder()
                                    .setStartingFilter(PropertyFilter.newBuilder()
                                            .setPropertyName("entityType")
                                            .setNumericFilter(NumericFilter.newBuilder()
                                                    .setComparisonOperator(ComparisonOperator.EQ)
                                                    .setValue(10)))
                                    .addSearchFilter(SearchFilter.newBuilder()
                                            .setPropertyFilter(PropertyFilter.newBuilder()
                                                    .setPropertyName("displayName")
                                                    .setStringFilter(StringFilter.newBuilder()
                                                            .setStringPropertyRegex("VM#2")))))
                            .addSearchParameters(SearchParameters.newBuilder()
                                    .setStartingFilter(PropertyFilter.newBuilder()
                                            .setPropertyName("entityType")
                                            .setNumericFilter(NumericFilter.newBuilder()
                                                    .setComparisonOperator(ComparisonOperator.EQ)
                                                    .setValue(14)))
                                    .addSearchFilter(SearchFilter.newBuilder()
                                            .setPropertyFilter(PropertyFilter.newBuilder()
                                                    .setPropertyName("displayName")
                                                    .setStringFilter(StringFilter.newBuilder()
                                                            .setStringPropertyRegex("PM#1"))))
                                    .addSearchFilter(SearchFilter.newBuilder()
                                            .setTraversalFilter(TraversalFilter.newBuilder()
                                                    .setTraversalDirection(TraversalDirection.PRODUCES)
                                                    .setStoppingCondition(StoppingCondition.newBuilder()
                                                            .setNumberHops(1))))
                            )))
                .build();

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);

        assertEquals(Long.toString(oid), dto.getUuid());
        assertEquals(displayName, dto.getDisplayName());
        assertEquals(ServiceEntityMapper.UIEntityType.VIRTUAL_MACHINE.getValue(), dto.getGroupType());
        assertEquals(isStatic, dto.getIsStatic());
        assertEquals(GroupMapper.GROUP, dto.getClassName());
        assertEquals("vmsByName", dto.getCriteriaList().get(0).getFilterType());
        assertEquals(GroupMapper.EQUAL, dto.getCriteriaList().get(0).getExpType());
        assertEquals("VM#2", dto.getCriteriaList().get(0).getExpVal());
        assertEquals("vmsByPMName", dto.getCriteriaList().get(1).getFilterType());
        assertEquals(GroupMapper.EQUAL, dto.getCriteriaList().get(1).getExpType());
        assertEquals("PM#1", dto.getCriteriaList().get(1).getExpVal());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        assertTrue(byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getMatch());
    }

    /**
     * Verify that a simple byName criterion is converted properly.
     */
    @Test
    public void testByNameSearchNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.NOT_EQUAL, FOO, "vmsByName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(FOO, byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getStringPropertyRegex());
        assertFalse(byName.getSearchFilter(0).getPropertyFilter().getStringFilter().getMatch());
    }

    /**
     * Verify multiple not equal by name search are converted properly.
     */
    @Test
    public void testByNameSearchMultipleNotEqual() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
            filterDTO(GroupMapper.NOT_EQUAL, FOO, "vmsByName"),
            filterDTO(GroupMapper.NOT_EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(2, parameters.size());
        SearchParameters firstByName = parameters.get(0);
        assertEquals(TYPE_IS_VM, firstByName.getStartingFilter());
        assertEquals(1, firstByName.getSearchFilterCount());
        assertEquals(FOO, firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getStringPropertyRegex());
        assertFalse(firstByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getMatch());

        SearchParameters secondByName = parameters.get(1);
        assertEquals(TYPE_IS_PM, secondByName.getStartingFilter());
        assertEquals(3, secondByName.getSearchFilterCount());
        assertEquals(BAR, secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getStringPropertyRegex());
        assertFalse(secondByName.getSearchFilter(0).getPropertyFilter().getStringFilter().getMatch());
    }

    /**
     * Verify that a byName criterion combined with a traversal with one hop are converted properly.
     */
    @Test
    public void testByNameAndTraversalHopSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByName"), filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
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
        assertEquals(SearchMapper.searchFilterProperty(TYPE_IS_VM), byPMNameByVMName.getSearchFilter(2));
    }

    /**
     * Verify that a traversal with one hop (without a byName criterion) is converted properly.
     */
    @Test
    public void testTraversalSearch() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters byPMName = parameters.get(0);
        assertEquals(TYPE_IS_PM, byPMName.getStartingFilter());
        assertEquals(3, byPMName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byPMName.getSearchFilter(0));
        assertEquals(PRODUCES_ONE_HOP, byPMName.getSearchFilter(1));
        assertEquals(SearchMapper.searchFilterProperty(TYPE_IS_VM), byPMName.getSearchFilter(2));
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
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(2, parameters.size());
        SearchParameters byName = parameters.get(0);
        assertEquals(TYPE_IS_VM, byName.getStartingFilter());
        assertEquals(1, byName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_FOO, byName.getSearchFilter(0));
        SearchParameters byDSName = parameters.get(1);
        assertEquals(TYPE_IS_DS, byDSName.getStartingFilter());
        assertEquals(2, byDSName.getSearchFilterCount());
        assertEquals(DISPLAYNAME_IS_BAR, byDSName.getSearchFilter(0));
        assertEquals(PRODUCES_VMS, byDSName.getSearchFilter(1));
    }

    /**
     * Verify that two traversals are converted properly.
     */
    @Test
    public void testTwoTraversals() {
        GroupApiDTO inputDTO = groupApiDTO(AND, VM_TYPE,
                filterDTO(GroupMapper.EQUAL, BAR, "vmsByPMName"),
                filterDTO(GroupMapper.EQUAL, FOO, "vmsByStorage"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        assertEquals(2, parameters.size());
    }

    @Test
    public void testVmsByClusterNameToSearchParameters() {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE, filterDTO(GroupMapper.EQUAL, FOO, "vmsByClusterName"));
        List<SearchParameters> parameters = groupMapper.convertToSearchParameters(groupDto, groupDto.getClassName());
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);
        // verify that the Cluster Membership Filter was created
        assertTrue(param.getSearchFilter(0).hasClusterMembershipFilter());
        ClusterMembershipFilter clusterMembershipFilter = param.getSearchFilter(0).getClusterMembershipFilter();
        // verify that we are looking for clusters with name FOO
        assertEquals(FOO, clusterMembershipFilter.getClusterSpecifier().getStringFilter().getStringPropertyRegex());

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

        final GroupApiDTO dto = groupMapper.toGroupApiDto(computeCluster);
        assertEquals("7", dto.getUuid());
        assertEquals(GroupMapper.CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool boy", dto.getDisplayName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.getValue(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testMapStorageCluster() {
        final Group computeCluster = Group.newBuilder()
                .setId(7L)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setName("cool girl")
                        .setClusterType(Type.STORAGE)
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(10L)))
                .build();

        final GroupApiDTO dto = groupMapper.toGroupApiDto(computeCluster);
        assertEquals("7", dto.getUuid());
        assertEquals(GroupMapper.CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool girl", dto.getDisplayName());
        assertEquals(UIEntityType.STORAGE.getValue(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testToTempGroupProto() throws OperationFailedException, InvalidOperationException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = mock(SupplyChainNodeFetcherBuilder.class);
        when(fetcherBuilder.addSeedUuids(any())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(any())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.fetch()).thenReturn(ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                .addMemberOids(7L)
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        TempGroupInfo groupInfo = groupMapper.toTempGroupProto(apiDTO);
        assertThat(groupInfo.getEntityType(), is(ServiceEntityMapper.fromUIEntityType(VM_TYPE)));
        assertThat(groupInfo.getMembers().getStaticMemberOidsList(), containsInAnyOrder(7L));
        assertThat(groupInfo.getName(), is("foo"));
        assertTrue(groupInfo.getIsGlobalScopeGroup());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testMapTempGroup() {
        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(Group.newBuilder()
            .setType(Group.Type.TEMP_GROUP)
            .setId(8L)
            .setOrigin(Origin.USER)
            .setTempGroup(TempGroupInfo.newBuilder()
                .setName("foo")
                .setEntityType(10)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L)))
            .build());

        assertThat(mappedDto.getTemporary(), is(true));
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(1));
        assertThat(mappedDto.getMembersCount(), is(1));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(mappedDto.getGroupType(), is(VM_TYPE));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.ONPREM));
        assertThat(mappedDto.getClassName(), is("Group"));
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
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("entityType")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("PhysicalMachine")))
                                .build())
                         .build())
                    .build())
                .build();

        when(groupExpander.expandUuids(ImmutableSet.of(String.valueOf(7L)))).thenReturn(
            ImmutableSet.of(10L, 20L, 30L));
        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);
        assertThat(dto.getEntitiesCount(), is(3));
        assertThat(dto.getMemberUuidList(),
                containsInAnyOrder("10", "20", "30"));
    }
}
