package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.components.common.utils.StringConstants.RESOURCE_GROUP;
import static com.vmturbo.components.common.utils.StringConstants.WORKLOAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.ImmutableGroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Translates to {@link GroupApiDTO}s.
 */
public class GroupMapperTest {

    private static final SearchParameters.Builder SEARCH_PARAMETERS = SearchParameters.newBuilder()
                    .setStartingFilter(PropertyFilter.newBuilder().setPropertyName("entityType")
                                    .setStringFilter(StringFilter.newBuilder()
                                                    .setStringPropertyRegex("PhysicalMachine")));

    private static final long CONTEXT_ID = 7777777;

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String groupUseCaseFileName = "groupBuilderUsecases.json";

    private final GroupUseCaseParser groupUseCaseParser =
                    Mockito.spy(new GroupUseCaseParser(groupUseCaseFileName));

    private SupplyChainFetcherFactory supplyChainFetcherFactory =
                    mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    /**
     * gRPC server to mock out inter-component dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(costServiceMole);

    private EntityFilterMapper entityFilterMapper = new EntityFilterMapper(groupUseCaseParser);

    private GroupFilterMapper groupFilterMapper = new GroupFilterMapper();

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);

    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);

    private final CloudTypeMapper cloudTypeMapper = mock(CloudTypeMapper.class);

    private final BusinessAccountRetriever businessAccountRetriever = mock(BusinessAccountRetriever.class);
    private GroupMapper groupMapper;

    private static final String AND = "AND";
    private static final String FOO = "foo";
    private static final String VM_TYPE = "VirtualMachine";

    @Before
    public void setup() {
        groupMapper = new GroupMapper(supplyChainFetcherFactory, groupExpander, topologyProcessor,
                repositoryApi, entityFilterMapper, groupFilterMapper, severityPopulator,
                businessAccountRetriever, CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                CONTEXT_ID, targetCache, cloudTypeMapper);
        SearchRequest req = ApiTestUtils.mockSearchCountReq(0);
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);
    }

    /**
     * Test static group converting GroupApiDTO to GroupInfo.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testToGroupInfoStaticGroup() throws Exception {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = true;
        final Optional<String> uuid = Optional.of("123");
        final GroupApiDTO groupDto = new GroupApiDTO();
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setMemberUuidList(Collections.singletonList(uuid.get()));

        final GroupDefinition g = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, g.getDisplayName());
        StaticMembersByType staticMembersByType = g.getStaticGroupMembers().getMembersByType(0);
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(),
                        staticMembersByType.getType().getEntity());
        assertEquals(SelectionCriteriaCase.STATIC_GROUP_MEMBERS, g.getSelectionCriteriaCase());
        assertEquals(Collections.singletonList(123L), staticMembersByType.getMembersList());
    }

    /**
     * Test PM dynamic group which filtered only by PM name.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testToGroupInfoDynamicGroupByPM() throws Exception {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.PHYSICAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(EntityFilterMapper.REGEX_MATCH);
        filterApiDTOFirst.setExpVal("PM#1");
        filterApiDTOFirst.setFilterType("pmsByName");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, groupDefinition.getDisplayName());

        final EntityFilter entityFilter = groupDefinition.getEntityFilters().getEntityFilter(0);
        assertEquals(EntityType.PHYSICAL_MACHINE.getNumber(), entityFilter.getEntityType());
        assertEquals(GroupDefinition.SelectionCriteriaCase.ENTITY_FILTERS,
                        groupDefinition.getSelectionCriteriaCase());
        // Verify the first search parameters' starting filter is PM entity
        assertEquals("entityType", entityFilter.getSearchParametersCollection()
                        .getSearchParameters(0).getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.typeNumber(),
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for PM
        assertEquals("displayName",
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("^PM#1$",
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getSearchFilter(0).getPropertyFilter().getStringFilter()
                                        .getStringPropertyRegex());
        assertTrue(entityFilter.getSearchParametersCollection().getSearchParameters(0)
                        .getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Test VM dynamic group which filtered by VM name and PM name.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testToGroupInfoDynamicGroupByVM() throws Exception {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(EntityFilterMapper.REGEX_MATCH);
        filterApiDTOFirst.setExpVal("VM#1");
        filterApiDTOFirst.setFilterType("vmsByName");
        final FilterApiDTO filterApiDTOSecond = new FilterApiDTO();
        filterApiDTOSecond.setExpType(EntityFilterMapper.REGEX_NO_MATCH);
        filterApiDTOSecond.setExpVal("PM#2");
        filterApiDTOSecond.setFilterType("vmsByPMName");
        final List<FilterApiDTO> criteriaList =
                        Lists.newArrayList(filterApiDTOFirst, filterApiDTOSecond);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, groupDefinition.getDisplayName());

        final EntityFilter entityFilter = groupDefinition.getEntityFilters().getEntityFilter(0);

        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), entityFilter.getEntityType());
        assertEquals(GroupDefinition.SelectionCriteriaCase.ENTITY_FILTERS,
                        groupDefinition.getSelectionCriteriaCase());
        assertEquals(2, entityFilter.getSearchParametersCollection().getSearchParametersCount());
        SearchParameters firstSearchParameters =
                        entityFilter.getSearchParametersCollection().getSearchParameters(0);
        // Verify the first search parameters' starting filter is VM entity
        assertEquals("entityType", firstSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                        firstSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are byName search for VM
        assertEquals("displayName", firstSearchParameters.getSearchFilter(0).getPropertyFilter()
                        .getPropertyName());
        assertEquals("^VM#1$", firstSearchParameters.getSearchFilter(0).getPropertyFilter()
                        .getStringFilter().getStringPropertyRegex());
        assertTrue(firstSearchParameters.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
        SearchParameters secondSearchParameters =
                        entityFilter.getSearchParametersCollection().getSearchParameters(1);
        // Verify the second search parameters' starting filter is PM entity
        assertEquals("entityType", secondSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.PHYSICAL_MACHINE.typeNumber(),
                        secondSearchParameters.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search filter is ByName search for PM
        assertEquals("displayName", secondSearchParameters.getSearchFilter(0).getPropertyFilter()
                        .getPropertyName());
        assertEquals("^PM#2$", secondSearchParameters.getSearchFilter(0).getPropertyFilter()
                        .getStringFilter().getStringPropertyRegex());
        assertFalse(secondSearchParameters.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
        // Verify the second search filter is traversal search and hops number is 1
        assertEquals(TraversalDirection.PRODUCES, secondSearchParameters.getSearchFilter(1)
                        .getTraversalFilter().getTraversalDirection());
        assertEquals(1, secondSearchParameters.getSearchFilter(1).getTraversalFilter()
                        .getStoppingCondition().getNumberHops());
        // Verify the third search filter is by Entity search for VM
        assertEquals("entityType", secondSearchParameters.getSearchFilter(2).getPropertyFilter()
                        .getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), secondSearchParameters
                        .getSearchFilter(2).getPropertyFilter().getNumericFilter().getValue());
    }

    /**
     * Test converting dynamic group info which only has starting filter to groupApiDTO.
     */
    @Test
    public void testToGroupApiDTOOnlyWithStartingFilter() {
        final String displayName = "group-foo";
        final long oid = 123L;

        final Grouping group = Grouping.newBuilder().setId(oid)
                        .addExpectedTypes(MemberType
                                        .newBuilder()
                                        .setEntity(UIEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition
                            .newBuilder().setType(GroupType.REGULAR).setDisplayName(displayName)
                            .setEntityFilters(EntityFilters.newBuilder().addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                                        .setSearchParametersCollection(SearchParametersCollection
                                                        .newBuilder()
                                                        .addSearchParameters(SEARCH_PARAMETERS
                                                                        .setSourceFilterSpecs(
                                                                                        buildFilterSpecs(
                                                                                                        "pmsByName",
                                                                                                        "foo",
                                                                                                        "foo")))))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(
                        ImmutableGroupAndMembers.builder().group(group).members(ImmutableSet.of(1L))
                                        .entities(ImmutableSet.of(2L, 3L)).build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);

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
     *  Test converting dynamic group info which has multiple search parameters to groupApiDTO.
     */
    @Test
    public void testToGroupApiDTOWithMultipleSearchParameters() {
        final String displayName = "group-foo";
        final Boolean isStatic = false;
        final long oid = 123L;

        final SearchParameters.Builder vmParameters = getVmParameters();
        final SearchParameters.Builder pmParameters = getVmByPmParameters();

        final Grouping group = Grouping.newBuilder().setId(oid)
                        .addExpectedTypes(MemberType
                                        .newBuilder()
                                        .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition
                        .newBuilder().setType(GroupType.REGULAR).setDisplayName(displayName)
                        .setEntityFilters(EntityFilters.newBuilder().addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                                        .setSearchParametersCollection(SearchParametersCollection
                                                        .newBuilder()
                                                        .addSearchParameters(vmParameters)
                                                        .addSearchParameters(pmParameters)))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(Collections.emptyList())
                        .entities(Collections.emptyList()).build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);

        assertEquals(Long.toString(oid), dto.getUuid());
        assertEquals(displayName, dto.getDisplayName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(isStatic, dto.getIsStatic());
        assertEquals(StringConstants.GROUP, dto.getClassName());
        assertEquals("vmsByName", dto.getCriteriaList().get(0).getFilterType());
        assertEquals(EntityFilterMapper.REGEX_MATCH, dto.getCriteriaList().get(0).getExpType());
        assertEquals("^VM#2$", dto.getCriteriaList().get(0).getExpVal());
        assertEquals("vmsByPMName", dto.getCriteriaList().get(1).getFilterType());
        assertEquals(EntityFilterMapper.REGEX_MATCH, dto.getCriteriaList().get(1).getExpType());
        assertEquals("^PM#1$", dto.getCriteriaList().get(1).getExpVal());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    private SearchParameters.Builder getVmParameters() {
        return SearchParameters.newBuilder()
                        .setSourceFilterSpecs(buildFilterSpecs("vmsByName",
                                        EntityFilterMapper.REGEX_MATCH, "^VM#2$"))
                        .setStartingFilter(PropertyFilter.newBuilder().setPropertyName("entityType")
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                        .setComparisonOperator(
                                                                        ComparisonOperator.EQ)
                                                        .setValue(10)))
                        .addSearchFilter(SearchFilter.newBuilder().setPropertyFilter(PropertyFilter
                                        .newBuilder().setPropertyName("displayName")
                                        .setStringFilter(StringFilter.newBuilder()
                                                        .setStringPropertyRegex("^VM#2$"))));
    }

    private SearchParameters.Builder getVmByPmParameters() {
        return SearchParameters.newBuilder()
                        .setSourceFilterSpecs(buildFilterSpecs("vmsByPMName",
                                        EntityFilterMapper.REGEX_MATCH, "^PM#1$"))
                        .setStartingFilter(PropertyFilter.newBuilder().setPropertyName("entityType")
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                        .setComparisonOperator(
                                                                        ComparisonOperator.EQ)
                                                        .setValue(14)))
                        .addSearchFilter(SearchFilter.newBuilder().setPropertyFilter(PropertyFilter
                                        .newBuilder().setPropertyName("displayName")
                                        .setStringFilter(StringFilter.newBuilder()
                                                        .setStringPropertyRegex("^PM#1$"))))
                        .addSearchFilter(SearchFilter.newBuilder()
                                        .setTraversalFilter(TraversalFilter.newBuilder()
                                                        .setTraversalDirection(
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
     * Test that the VM group criteria by clusters name is converted to SearchParameters correctly.
     * @throws OperationFailedException any error happens
     */
    @Test
    public void testVmsByClusterNameToSearchParameters() throws OperationFailedException {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.EQUAL, FOO, "vmsByClusterName"));
        List<SearchParameters> parameters =
                        entityFilterMapper.convertToSearchParameters(
                                        groupDto.getCriteriaList(), groupDto.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);

        // verify that the starting filter is PM
        assertEquals(SearchProtoUtil.entityTypeFilter(UIEntityType.PHYSICAL_MACHINE), param.getStartingFilter());

        // 2 search filters after starting filter
        assertEquals(2, param.getSearchFilterCount());

        // 1. first one is Cluster Membership Filter, verify that it was created
        assertTrue(param.getSearchFilter(0).hasGroupFilter());
        final GroupFilter groupFilter =
                        param.getSearchFilter(0).getGroupFilter();
        // verify that we are looking for clusters with name FOO
        assertEquals("^" + FOO + "$", groupFilter.getGroupSpecifier()
                        .getStringFilter().getStringPropertyRegex());

        // 2. second one is traversal filter (produces) used to traverse to vm
        assertEquals(param.getSearchFilter(1), SearchProtoUtil.searchFilterTraversal(
                SearchProtoUtil.traverseToType(TraversalDirection.PRODUCES, EntityType.VIRTUAL_MACHINE)));

        // test conversion from GroupApiDTO back to FilterApiDTO
        groupDto.setDisplayName("TestGroupDto");
        groupDto.setGroupType("VirtualMachine");
        groupDto.setIsStatic(false);
        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        List<FilterApiDTO> filterApiDTOS = entityFilterMapper
                        .convertToFilterApis(groupDefinition.getEntityFilters().getEntityFilter(0));
        assertEquals(1, filterApiDTOS.size());
        // verify that we have rebuilt the original vmsByClusterName
        FilterApiDTO vmsByClusterNameFilter = filterApiDTOS.get(0);
        assertEquals("vmsByClusterName", vmsByClusterNameFilter.getFilterType());
        assertEquals("EQ", vmsByClusterNameFilter.getExpType());
        assertEquals(FOO, vmsByClusterNameFilter.getExpVal());
    }

    /**
     * Test that the PM group criteria by clusters name is converted to SearchParameters correctly.
     * @throws OperationFailedException any error happens
     */
    @Test
    public void testPMsByClusterNameToSearchParameters() throws OperationFailedException {
        GroupApiDTO groupDto = groupApiDTO(AND, UIEntityType.PHYSICAL_MACHINE.apiStr(),
                filterDTO(EntityFilterMapper.EQUAL, FOO, "pmsByClusterName"));
        List<SearchParameters> parameters =
                entityFilterMapper.convertToSearchParameters(
                        groupDto.getCriteriaList(), groupDto.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);

        // verify that the starting filter is PM
        assertEquals(SearchProtoUtil.entityTypeFilter(UIEntityType.PHYSICAL_MACHINE),
                param.getStartingFilter());

        // 1 search filters after starting filter
        assertEquals(1, param.getSearchFilterCount());

        // verify that Cluster Membership Filter was created
        assertTrue(param.getSearchFilter(0).hasGroupFilter());
        final GroupFilter clusterMembershipFilter =
                param.getSearchFilter(0).getGroupFilter();
        // verify that we are looking for clusters with name FOO
        assertEquals("^" + FOO + "$", clusterMembershipFilter.getGroupSpecifier()
                .getStringFilter().getStringPropertyRegex());
    }

    /**
     * Tests converting of searchParameters of GroupInfo to filterApiDto.
     */
    @Test
    public void testConvertToFilterApis() {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                        .setEntityFilters(EntityFilters.newBuilder().addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setSearchParametersCollection(SearchParametersCollection
                                                        .newBuilder()
                                                        .addSearchParameters(SEARCH_PARAMETERS
                                                                        .setSourceFilterSpecs(
                                                                                        buildFilterSpecs(
                                                                                                        "filterType",
                                                                                                        "expType",
                                                                                                        "expValue"))))))
                        .build();
        final List<FilterApiDTO> filterApiDTOS = entityFilterMapper
                        .convertToFilterApis(groupDefinition.getEntityFilters().getEntityFilter(0));
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

    private GroupApiDTO groupApiDTO(String logicalOperator, String className,
                    FilterApiDTO... filters) {
        GroupApiDTO inputDTO = new GroupApiDTO();
        inputDTO.setLogicalOperator(logicalOperator);
        inputDTO.setClassName(className);
        inputDTO.setCriteriaList(Arrays.asList(filters));
        return inputDTO;
    }

    @Test
    public void testMapComputeCluster() {

        final Grouping computeCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                                        .setDisplayName("cool boy")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.PHYSICAL_MACHINE
                                                                                                        .typeNumber()))
                                                                        .addMembers(10L))))
                        .build();

        when(groupExpander.getMembersForGroup(computeCluster)).thenReturn(ImmutableGroupAndMembers
                        .builder().group(computeCluster)
                        .members(GroupProtoUtil.getAllStaticMembers(computeCluster.getDefinition()))
                        .entities(GroupProtoUtil
                                        .getAllStaticMembers(computeCluster.getDefinition()))
                        .build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

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
    public void testMapComputeVirtualMachineCluster() {

        final Grouping computeVirtualMachineCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                                        .setDisplayName("red silence")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber()))
                                                                        .addMembers(10L))))
                        .build();

        when(groupExpander.getMembersForGroup(computeVirtualMachineCluster)).thenReturn(
                        ImmutableGroupAndMembers.builder().group(computeVirtualMachineCluster)
                                        .members(GroupProtoUtil.getAllStaticMembers(
                                                        computeVirtualMachineCluster
                                                                        .getDefinition()))
                                        .entities(GroupProtoUtil.getAllStaticMembers(
                                                        computeVirtualMachineCluster
                                                                        .getDefinition()))
                                        .build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto = groupMapper.toGroupApiDto(computeVirtualMachineCluster);
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.VIRTUAL_MACHINE_CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("red silence", dto.getDisplayName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testMapStorageCluster() {

        final Grouping storageCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.STORAGE_CLUSTER)
                                        .setDisplayName("cool girl")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.STORAGE
                                                                                                        .typeNumber()))
                                                                        .addMembers(10L))))
                        .build();

        when(groupExpander.getMembersForGroup(storageCluster)).thenReturn(ImmutableGroupAndMembers
                        .builder().group(storageCluster)
                        .members(GroupProtoUtil.getAllStaticMembers(storageCluster.getDefinition()))
                        .entities(GroupProtoUtil
                                        .getAllStaticMembers(storageCluster.getDefinition()))
                        .build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

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
    public void testToTempGroupProtoGlobalScope() throws OperationFailedException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                        ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList
                                                        .newBuilder().addMemberOids(7L).build())
                                        .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        GroupDefinition groupDefinition = groupMapper.toGroupDefinition(apiDTO);
        final StaticMembersByType memberByType =
                        groupDefinition.getStaticGroupMembers().getMembersByType(0);
        assertThat(memberByType.getType().getEntity(),
                        is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertTrue(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(
                        Collections.singletonList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoNotGlobalScope() throws OperationFailedException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList("1"));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                        ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList
                                                        .newBuilder().addMemberOids(7L).build())
                                        .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        GroupDefinition groupDefinition = groupMapper.toGroupDefinition(apiDTO);
        final StaticMembersByType memberByType =
                        groupDefinition.getStaticGroupMembers().getMembersByType(0);
        assertThat(memberByType.getType().getEntity(),
                        is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoUuidList() throws OperationFailedException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        // One valid, one invalid.
        apiDTO.setMemberUuidList(Lists.newArrayList("1", "foo"));

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(apiDTO);
        final StaticMembersByType memberByType =
                        groupDefinition.getStaticGroupMembers().getMembersByType(0);
        assertThat(memberByType.getType().getEntity(),
                        is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(1L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());
    }

    @Test
    public void testToTempGroupProtoUuidListInsideScope() throws OperationFailedException {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList("1"));
        // 7 should be in the scope, 6 is not in the scope, and foo is an illegal value.
        apiDTO.setMemberUuidList(Lists.newArrayList("7", "6", "foo"));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                        ImmutableMap.of(VM_TYPE, SupplyChainNode.newBuilder()
                                        .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList
                                                        .newBuilder().addMemberOids(7L).build())
                                        .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        GroupDefinition groupDefinition = groupMapper.toGroupDefinition(apiDTO);
        StaticMembersByType memberByType =
                        groupDefinition.getStaticGroupMembers().getMembersByType(0);
        assertThat(memberByType.getType().getEntity(),
                        is(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testMapTempGroup() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo").setIsTemporary(true)
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(10))
                                                                        .addMembers(1L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(Collections.singleton(1L))
                        // Temp groups will never have different entity count, but we want to check the
                        // entity count gets set from the right field in GroupAndMembers.
                        .entities(ImmutableList.of(1L, 2L)).build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

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
    public void testMapTempGroupCloud() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo").setIsTemporary(true)
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(10))
                                                                        .addMembers(1L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(GroupProtoUtil.getStaticMembers(group))
                        .members(GroupProtoUtil.getStaticMembers(group)).build());

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

    /**
     * Tests conversion of a Resource Group message.
     */
    @Test
    public void testMapResourceGroup() {
        final long parentId = 1L;
        final String parentDisplayName = "Test displayName";
        final Grouping group = Grouping.newBuilder().setId(8L)
            .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
            .setDefinition(GroupDefinition.newBuilder().setType(GroupType.RESOURCE)
                .setDisplayName("foo")
                .setOwner(parentId)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addMembers(1L)
                        .addMembers(2L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(UIEntityType.DATABASE.typeNumber()))
                        .addMembers(3L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(UIEntityType.DATABASE_SERVER.typeNumber()))
                        .addMembers(4L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(UIEntityType.VIRTUAL_VOLUME.typeNumber()))
                        .addAllMembers(Arrays.asList(5L, 6L, 7L, 8L, 9L)))
                ))
            .build();

        final SingleEntityRequest testRg = ApiTestUtils.mockSingleEntityRequest(
                MinimalEntity.newBuilder().setOid(parentId).setDisplayName(parentDisplayName).build());
        when(repositoryApi.entityRequest(parentId)).thenReturn(testRg);

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group).entities(GroupProtoUtil.getStaticMembers(group))
            .members(GroupProtoUtil.getStaticMembers(group)).build());

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.CLOUD);

        assertThat(mappedDto.getTemporary(), is(false));
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(9));
        assertThat(mappedDto.getMembersCount(), is(4));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1", "2", "3",
            "4", "5", "6", "7", "8", "9"));
        assertThat(mappedDto.getGroupType(), is(WORKLOAD));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(mappedDto.getClassName(), is(RESOURCE_GROUP));
        assertThat(((ResourceGroupApiDTO)mappedDto).getParentDisplayName(), is(parentDisplayName));
        assertThat(((ResourceGroupApiDTO)mappedDto).getParentUuid(), is(String.valueOf(parentId)));
    }

    @Test
    public void testMapGroupActiveEntities() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber()))
                                                                        .addMembers(1L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(Collections.singleton(1L))
                        .members(Collections.singleton(1L)).build());

        final int count = 10;
        final SearchRequest countReq = ApiTestUtils.mockSearchCountReq(count);
        when(repositoryApi.newSearchRequest(any())).thenReturn(countReq);

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getActiveEntitiesCount(), is(count));

        final ArgumentCaptor<SearchParameters> captor =
                        ArgumentCaptor.forClass(SearchParameters.class);
        verify(repositoryApi).newSearchRequest(captor.capture());
        final SearchParameters params = captor.getValue();
        assertThat(params.getStartingFilter(),
                        is(SearchProtoUtil.idFilter(Collections.singleton(1L))));
        assertThat(params.getSearchFilterList(), containsInAnyOrder(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.stateFilter(UIEntityState.ACTIVE))));
    }

    @Test
    public void testMapGroupActiveEntitiesGlobalTempGroup() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo").setIsTemporary(true)
                                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                                        .setIsGlobalScope(true))
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber())))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(Collections.singleton(1L))
                        .members(Collections.singleton(1L)).build());

        final int count = 10;
        final SearchRequest countReq = ApiTestUtils.mockSearchCountReq(count);
        when(repositoryApi.newSearchRequest(any())).thenReturn(countReq);

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getActiveEntitiesCount(), is(count));

        final ArgumentCaptor<SearchParameters> captor =
                        ArgumentCaptor.forClass(SearchParameters.class);
        verify(repositoryApi).newSearchRequest(captor.capture());
        final SearchParameters params = captor.getValue();
        assertThat(params.getStartingFilter(),
                        is(SearchProtoUtil.entityTypeFilter(UIEntityType.VIRTUAL_MACHINE)));
        assertThat(params.getSearchFilterList(), containsInAnyOrder(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.stateFilter(UIEntityState.ACTIVE))));
    }

    @Test
    public void testMapGroupActiveEntitiesException() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber())))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(Collections.singleton(1L))
                        .members(Collections.singleton(1L)).build());

        final SearchRequest countReq = ApiTestUtils.mockSearchCountReq(0);
        when(countReq.count()).thenThrow(Status.INTERNAL.asRuntimeException());

        when(repositoryApi.newSearchRequest(any())).thenReturn(countReq);

        final GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);
        assertThat(mappedDto.getUuid(), is("8"));
        // The fallback is the number of entities.
        assertThat(mappedDto.getActiveEntitiesCount(), is(1));
    }

    @Test
    public void testMapTempGroupONPREM() {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo").setIsTemporary(true)
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(10))
                                                                        .addMembers(1L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(Collections.singleton(1L))
                        // Return a different entity set to make sure it gets used for the entity count.
                        .entities(ImmutableSet.of(2L, 3L)).build());

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
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo").setIsTemporary(true)
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(10))
                                                                        .addMembers(1L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(GroupProtoUtil.getStaticMembers(group))
                        .members(GroupProtoUtil.getStaticMembers(group)).build());

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
        final Grouping group = Grouping.newBuilder().setId(7L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("group1")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(EntityType.PHYSICAL_MACHINE
                                                                                                        .getNumber()))
                                                                        .addMembers(10L)
                                                                        .addMembers(20L))))
                        .build();

        // We use the groupExpander to get members for both static and dynamic groups.
        final Set<Long> members = ImmutableSet.of(10L, 20L);

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(members).entities(members).build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM);
        assertEquals("7", dto.getUuid());
        assertEquals(true, dto.getIsStatic());
        assertThat(dto.getEntitiesCount(), is(2));
        assertThat(dto.getMemberUuidList(), containsInAnyOrder("10", "20"));
    }

    @Test
    public void testDynamicGroupMembersCount() {
        final Grouping group = Grouping.newBuilder().setId(7L)
            .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
            .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                .setDisplayName("group1")
                .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                        .setEntityType(EntityType.PHYSICAL_MACHINE
                                        .getNumber())
                        .setSearchParametersCollection(
                                        SearchParametersCollection
                                                        .newBuilder()
                                                        .addSearchParameters(
                                                                        SEARCH_PARAMETERS)))))
                        .build();

        final Set<Long> members = ImmutableSet.of(10L, 20L, 30L);
        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(members).entities(members).build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto = groupMapper.toGroupApiDto(group);
        assertThat(dto.getEntitiesCount(), is(3));
        assertThat(dto.getMemberUuidList(), containsInAnyOrder("10", "20", "30"));
    }

    /**
     * Test a dynamic filter with exact string equality.
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testExactStringMatchingFilterPositive()
                    throws Exception {
        testExactStringMatchingFilter(true);
    }

    /**
     * Test a dynamic filter with negated exact string equality.
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testExactStringMatchingFilterNegative()
                    throws Exception {
        testExactStringMatchingFilter(false);
    }

    private void testExactStringMatchingFilter(boolean positiveMatching)
                    throws OperationFailedException {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final Boolean isStatic = false;
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(
                        positiveMatching ? EntityFilterMapper.EQUAL : EntityFilterMapper.NOT_EQUAL);
        filterApiDTOFirst.setExpVal("Idle");
        filterApiDTOFirst.setFilterType("vmsByState");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(isStatic);
        groupDto.setCriteriaList(criteriaList);

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, groupDefinition.getDisplayName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(),
                        groupDefinition.getEntityFilters().getEntityFilter(0).getEntityType());
        assertEquals(GroupDefinition.SelectionCriteriaCase.ENTITY_FILTERS,
                        groupDefinition.getSelectionCriteriaCase());
        // Verify the first search parameters' starting filter is VM entity
        SearchParameters searchParam = groupDefinition.getEntityFilters().getEntityFilter(0)
                        .getSearchParametersCollection().getSearchParameters(0);
        assertEquals("entityType", searchParam.getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                        searchParam.getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are by state search for VM
        assertEquals("state", searchParam.getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("Idle", searchParam.getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getOptions(0));
        assertEquals(positiveMatching, searchParam.getSearchFilter(0).getPropertyFilter()
                        .getStringFilter().getPositiveMatch());
    }

    /**
     * Test proper construction of a filter that connects VMs to networks.
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testVmsConnectedToNetwork() throws Exception {
        final String displayName = "group-foo";
        final String groupType = UIEntityType.VIRTUAL_MACHINE.apiStr();
        final String regex = ".*";
        final GroupApiDTO groupDto = new GroupApiDTO();
        final FilterApiDTO filterApiDTOFirst = new FilterApiDTO();
        filterApiDTOFirst.setExpType(EntityFilterMapper.REGEX_MATCH);
        filterApiDTOFirst.setExpVal(regex);
        filterApiDTOFirst.setFilterType("vmsByNetwork");
        final List<FilterApiDTO> criteriaList = Lists.newArrayList(filterApiDTOFirst);
        groupDto.setDisplayName(displayName);
        groupDto.setGroupType(groupType);
        groupDto.setIsStatic(false);
        groupDto.setCriteriaList(criteriaList);

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, groupDefinition.getDisplayName());

        final EntityFilter entityFilter = groupDefinition.getEntityFilters().getEntityFilter(0);

        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), entityFilter.getEntityType());
        assertEquals(GroupDefinition.SelectionCriteriaCase.ENTITY_FILTERS,
                        groupDefinition.getSelectionCriteriaCase());
        // Verify the first search parameters' starting filter is VM entity
        assertEquals("entityType", entityFilter.getSearchParametersCollection()
                        .getSearchParameters(0).getStartingFilter().getPropertyName());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getStartingFilter().getNumericFilter().getValue());
        // Verify the first search parameters are by state search for VM
        assertEquals(SearchableProperties.VM_CONNECTED_NETWORKS,
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getSearchFilter(0).getPropertyFilter().getPropertyName());
        assertEquals("^" + regex + "$",
                        entityFilter.getSearchParametersCollection().getSearchParameters(0)
                                        .getSearchFilter(0).getPropertyFilter().getListFilter()
                                        .getStringFilter().getStringPropertyRegex());
        assertTrue(entityFilter.getSearchParametersCollection().getSearchParameters(0)
                        .getSearchFilter(0).getPropertyFilter().getStringFilter()
                        .getPositiveMatch());
    }

    /**
     * Test getEnvironmentTypeForGroup returns proper type for a group (ON_PREM, CLOUD, HYBRID).
     */
    @Test
    public void testGetEnvironmentTypeForGroup() {
        final String displayName = "group-foo";
        final int groupType = EntityType.VIRTUAL_MACHINE.getNumber();
        final long oid = 123L;
        final long uuid1 = 2L;
        final long uuid2 = 3L;

        final Grouping group = Grouping.newBuilder()
            .setId(oid)
            .addExpectedTypes(MemberType.newBuilder().setEntity(groupType))
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName(displayName)
                .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter
                        .newBuilder()
                        .setEntityType(groupType)
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(SEARCH_PARAMETERS.setSourceFilterSpecs(
                                buildFilterSpecs("pmsByName", "foo", "foo"))))
                    )

               )
            )
            .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group)
            .members(ImmutableSet.of(uuid1, uuid2))
            .entities(ImmutableSet.of(uuid1, uuid2))
            .build());

        MinimalEntity entVM1 =  MinimalEntity.newBuilder()
                        .setOid(uuid1)
                        .setDisplayName("foo")
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        MinimalEntity entVM2 =  MinimalEntity.newBuilder()
                        .setOid(uuid1)
                        .setDisplayName("foo")
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .build();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);
        listVMs.add(entVM2);

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);
        EntityEnvironment envCloudType = groupMapper.getEnvironmentAndCloudTypeForGroup(groupExpander.getMembersForGroup(group));
        assertEquals(envCloudType.getEnvironmentType(), EnvironmentType.HYBRID);
    }

    /**
     * Test {@link GroupMapper#getEnvironmentAndCloudTypeForGroup(GroupAndMembers)} in case when we
     * have regular group with empty resource group members.
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForRegularGroupWithCloudGroupMembers() {
        final Grouping rg = Grouping.newBuilder()
                .setId(1L)
                .addExpectedTypes(MemberType.newBuilder().setGroup(GroupType.RESOURCE).build())
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR).build())
                .build();
        final ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg)
                .members(Arrays.asList(1L, 2L))
                .entities(Collections.emptySet())
                .build();
        final EntityEnvironment entityEnvironment =
                groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers);
        final EnvironmentType environmentType = entityEnvironment.getEnvironmentType();
        assertEquals(environmentType, EnvironmentType.CLOUD);
    }

    /**
     * Test {@link GroupMapper#getEnvironmentAndCloudTypeForGroup(GroupAndMembers)} in case when we
     * have empty resource group.
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForCloudResourceGroup() {
        final Grouping rg = Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.RESOURCE).build())
                .build();
        final ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg)
                .members(Collections.emptySet())
                .entities(Collections.emptySet())
                .build();
        final EntityEnvironment entityEnvironment =
                groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers);
        final EnvironmentType environmentType = entityEnvironment.getEnvironmentType();
        assertEquals(environmentType, EnvironmentType.CLOUD);
    }

    /**
     * Test {@link GroupMapper#getEnvironmentAndCloudTypeForGroup(GroupAndMembers)} in case when we
     * have empty billing family.
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForBillingFamily() {
        final Grouping rg = Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.BILLING_FAMILY).build())
                .build();
        final ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg)
                .members(Collections.emptySet())
                .entities(Collections.emptySet())
                .build();
        final EntityEnvironment entityEnvironment =
                groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers);
        final EnvironmentType environmentType = entityEnvironment.getEnvironmentType();
        assertEquals(environmentType, EnvironmentType.CLOUD);
    }

    /**
     * Test getEnvironmentTypeForGroup returns proper type for a group (ON_PREM, CLOUD, HYBRID).
     */
    @Test
    public void testGetCloudTypeForGroup() {
        final String displayName = "group-foo";
        final int groupType = EntityType.VIRTUAL_MACHINE.getNumber();
        final long oid = 123L;
        final long uuid1 = 2L;
        final long uuid2 = 3L;
        final long targetId1 = 2141L;
        final long targetId2 = 9485L;
        final long probeId1 = 111L;
        final long probeId2 = 222L;

        final Grouping group = Grouping.newBuilder()
                .setId(oid)
                .addExpectedTypes(MemberType.newBuilder().setEntity(groupType))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName(displayName)
                        .setEntityFilters(EntityFilters.newBuilder()
                                .addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setEntityType(groupType)
                                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                                .addSearchParameters(SEARCH_PARAMETERS.setSourceFilterSpecs(
                                                        buildFilterSpecs("pmsByName", "foo", "foo"))))
                                )

                        )
                )
                .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1, uuid2))
                .entities(ImmutableSet.of(uuid1, uuid2))
                .build());

        final MinimalEntity entVM1 =  MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo1")
                .addDiscoveringTargetIds(targetId1)
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
        final MinimalEntity entVM2 =  MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo2")
                .addDiscoveringTargetIds(targetId2)
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);

        final ThinTargetCache.ThinTargetInfo thinTargetInfo1 = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                        .type(SDKProbeType.AWS.getProbeType())
                        .oid(probeId1)
                        .build())
                .displayName("TARGET_DISPLAY_NAME")
                .oid(targetId1)
                .isHidden(false)
                .build();

        final ThinTargetCache.ThinTargetInfo thinTargetInfo2 = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                        .type(SDKProbeType.AZURE.getProbeType())
                        .oid(probeId2)
                        .build())
                .displayName("TARGET_DISPLAY_NAME")
                .oid(targetId2)
                .isHidden(false)
                .build();

        final TargetInfo targetInfo1 = Mockito.mock(TargetInfo.class);
        when(targetInfo1.getProbeId()).thenReturn(probeId1);

        final ProbeInfo probeInfo1 = Mockito.mock(ProbeInfo.class);
        when(probeInfo1.getType()).thenReturn(SDKProbeType.AWS.getProbeType());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);
        when(targetCache.getTargetInfo(targetId1)).thenReturn(Optional.of(thinTargetInfo1));
        when(cloudTypeMapper.fromTargetType(any())).thenReturn(Optional.of(CloudType.AWS));
        // test with only one type, cloudType should be AWS
        EntityEnvironment envCloudType = groupMapper.getEnvironmentAndCloudTypeForGroup(groupExpander.getMembersForGroup(group));
        assertEquals(envCloudType.getCloudType(), CloudType.AWS);

        listVMs.add(entVM2);
        final TargetInfo targetInfo2 = Mockito.mock(TargetInfo.class);
        when(targetInfo2.getProbeId()).thenReturn(probeId2);

        final ProbeInfo probeInfo2 = Mockito.mock(ProbeInfo.class);
        when(probeInfo2.getType()).thenReturn(SDKProbeType.AZURE_SERVICE_PRINCIPAL.getProbeType());

        MultiEntityRequest req2 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req2);
        when(targetCache.getTargetInfo(targetId1)).thenReturn(Optional.of(thinTargetInfo1));
        when(targetCache.getTargetInfo(targetId2)).thenReturn(Optional.of(thinTargetInfo2));
        when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(Optional.of(CloudType.AWS));
        when(cloudTypeMapper.fromTargetType("Azure Subscription")).thenReturn(Optional.of(CloudType.AZURE));
        // test with both AWS and Azure, cloudType should be Hybrid
        envCloudType = groupMapper.getEnvironmentAndCloudTypeForGroup(groupExpander.getMembersForGroup(group));
        assertEquals(envCloudType.getCloudType(), CloudType.HYBRID);
    }

    /**
     * Test {@link GroupMapper#getEnvironmentAndCloudTypeForGroup(GroupAndMembers)} when group
     * has members discovered from different (on-prem and cloud) targets.
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForHybridGroup() {
        final String displayName = "hybrid-group";
        final int groupType = EntityType.VIRTUAL_MACHINE.getNumber();
        final long oid = 123L;
        final long uuid1 = 1L;
        final long uuid2 = 2L;
        final long uuid3 = 3L;
        final long targetId1 = 2141L;
        final long targetId2 = 9485L;
        final long targetId3 = 9124L;
        final long probeId1 = 111L;
        final long probeId2 = 222L;
        final long probeId3 = 333L;

        final Grouping group = Grouping.newBuilder()
                .setId(oid)
                .addExpectedTypes(MemberType.newBuilder().setEntity(groupType))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName(displayName)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .addAllMembers(Arrays.asList(uuid1, uuid2, uuid3))
                                        .build())
                                .build()))
                .build();

        final MinimalEntity entVM1 = MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo1")
                .addDiscoveringTargetIds(targetId1)
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final MinimalEntity entVM2 = MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo2")
                .addDiscoveringTargetIds(targetId2)
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final MinimalEntity entVM3 = MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo3")
                .addDiscoveringTargetIds(targetId2)
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                .build();

        final ThinTargetCache.ThinTargetInfo thinTargetInfo1 = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                        .type(SDKProbeType.AWS.getProbeType())
                        .oid(probeId1).build())
                .displayName("AWS")
                .oid(targetId1)
                .isHidden(false)
                .build();

        final ThinTargetCache.ThinTargetInfo thinTargetInfo2 = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                        .type(SDKProbeType.AZURE.getProbeType())
                        .oid(probeId2)
                        .build())
                .displayName("Azure")
                .oid(targetId2)
                .isHidden(false)
                .build();

        final ThinTargetCache.ThinTargetInfo thinTargetInfo3 = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(ProbeCategory.HYPERVISOR.getCategoryInUpperCase())
                        .type(SDKProbeType.VCENTER.getProbeType())
                        .oid(probeId3)
                        .build())
                .displayName("Vcenter")
                .oid(targetId3)
                .isHidden(false)
                .build();

        final List<MinimalEntity> listVMs = Arrays.asList(entVM1, entVM2, entVM3);
        final MultiEntityRequest minEntityReq = ApiTestUtils.mockMultiMinEntityReq(listVMs);

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1, uuid2, uuid3))
                .entities(ImmutableSet.of(uuid1, uuid2, uuid3))
                .build());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(minEntityReq);
        when(targetCache.getTargetInfo(targetId1)).thenReturn(Optional.of(thinTargetInfo1));
        when(targetCache.getTargetInfo(targetId2)).thenReturn(Optional.of(thinTargetInfo2));
        when(targetCache.getTargetInfo(targetId3)).thenReturn(Optional.of(thinTargetInfo3));
        when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(Optional.of(CloudType.AWS));
        when(cloudTypeMapper.fromTargetType("Azure Subscription")).thenReturn(Optional.of(CloudType.AZURE));

        final EntityEnvironment envAndCloudType = groupMapper.getEnvironmentAndCloudTypeForGroup(
                groupExpander.getMembersForGroup(group));
        assertEquals(EnvironmentType.HYBRID, envAndCloudType.getEnvironmentType());
        assertEquals(CloudType.HYBRID, envAndCloudType.getCloudType());
    }

    /**
     * Test getEnvironmentTypeForGroup returns proper type for a group of resource groups. When
     * all entities from both group have the same {@link EnvironmentType} then group also has that
     * {@link EnvironmentType}.
     */
    @Test
    public void testGetEnvironmentTypeForGroupOfResourceGroups() {
        final long rg1Oid = 1L;
        final long rg2Oid = 2L;
        final long groupOid = 3L;
        final long rgMember1Oid = 4L;
        final long rgMember2Oid = 5L;

        final GroupDefinition groupDefinition = GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                        .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                                .setType(GroupDTO.MemberType.newBuilder()
                                        .setGroup(GroupType.RESOURCE)
                                        .build())
                                .addAllMembers(Arrays.asList(rg1Oid, rg2Oid))
                                .build())
                        .build())
                .build();

        final Grouping group =
                Grouping.newBuilder().setDefinition(groupDefinition).setId(groupOid).build();

        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(Arrays.asList(rg1Oid, rg2Oid))
                .entities(Arrays.asList(rgMember1Oid, rgMember2Oid))
                .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(groupAndMembers);

        final MinimalEntity entVM1 = MinimalEntity.newBuilder()
                .setOid(rgMember1Oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
        final MinimalEntity entVM2 = MinimalEntity.newBuilder()
                .setOid(rgMember2Oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final List<MinimalEntity> listVMs = Arrays.asList(entVM1, entVM2);

        final MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);
        EntityEnvironment envCloudType = groupMapper.getEnvironmentAndCloudTypeForGroup(groupExpander.getMembersForGroup(group));
        assertEquals(envCloudType.getEnvironmentType(), EnvironmentType.CLOUD);
    }



    /**
     * A cloud entity in a group that belongs to a Cloud target and a Non-cloud target should not
     * cause an exception. We should still be able to determine that the AWS entity has cloud type
     * AWS and CLOUD environment type.
     */
    @Test
    public void testCloudEntityStitchedByNonCloudTarget() {
        GroupAndMembers groupAndMembers = mock(GroupAndMembers.class);
        when(groupAndMembers.entities()).thenReturn(Arrays.asList(1L));

        long awsTargetId = 2L;
        long appDTargetId = 3L;
        final MinimalEntity entVM1 =  MinimalEntity.newBuilder()
            .setDisplayName("VM in cloud stitched to AppD")
            .addDiscoveringTargetIds(appDTargetId)
            .addDiscoveringTargetIds(awsTargetId)
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
            .buildPartial();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);

        long awsProbeId = 4L;
        final ThinTargetCache.ThinTargetInfo awsTargetInfo = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                .type(SDKProbeType.AWS.getProbeType())
                .oid(awsProbeId)
                .build())
            .displayName("AWS Target")
            .oid(awsTargetId)
            .isHidden(false)
            .build();
        long appDProbeId = 5L;
        final ThinTargetCache.ThinTargetInfo appDProbeInfo = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(ProbeCategory.GUEST_OS_PROCESSES.getCategoryInUpperCase())
                .type(SDKProbeType.APPDYNAMICS.getProbeType())
                .oid(appDProbeId)
                .build())
            .displayName("AppD Target")
            .oid(appDTargetId)
            .isHidden(false)
            .build();
        when(targetCache.getTargetInfo(awsTargetId)).thenReturn(Optional.of(awsTargetInfo));
        when(targetCache.getTargetInfo(appDTargetId)).thenReturn(Optional.of(appDProbeInfo));

        when(cloudTypeMapper.fromTargetType(SDKProbeType.AWS.getProbeType())).thenReturn(Optional.of(CloudType.AWS));
        // This is the scenario that caused the NPE in OM-54171.
        when(cloudTypeMapper.fromTargetType(SDKProbeType.APPDYNAMICS.getProbeType())).thenReturn(Optional.empty());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        EntityEnvironment entityEnvironment = groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers);
        assertEquals(EnvironmentType.CLOUD, entityEnvironment.getEnvironmentType());
        assertEquals(CloudType.AWS, entityEnvironment.getCloudType());
    }


    /**
     * Test that the severity field on GroupApiDTO is populated as expected.
     */
    @Test
    public void testPopulateSeverityOnGroupApiDTO() {
        Grouping group = Grouping.newBuilder().setId(8L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                        .setDisplayName("foo"))
                .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .entities(Collections.singleton(1L))
                .members(Collections.singleton(1L))
                .build());

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(
                MinimalEntity.newBuilder().setOid(1L)
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .build()));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);
        when(severityPopulator.calculateSeverity(eq(CONTEXT_ID), eq(Collections.singleton(1L))))
                .thenReturn(Optional.of(Severity.CRITICAL));

        GroupApiDTO mappedDto = groupMapper.toGroupApiDto(group, true);
        // verify that calculateSeverity is invoked and severity is populated
        assertEquals(mappedDto.getSeverity(), Severity.CRITICAL.name());
        verify(severityPopulator).calculateSeverity(eq(CONTEXT_ID), eq(Collections.singleton(1L)));

        // verify that calculateSeverity is not invoked and severity is not populated if group is empty
        req = ApiTestUtils.mockMultiMinEntityReq(Collections.emptyList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);
        group = Grouping.newBuilder().setId(9L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                        .setDisplayName("foo"))
                .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                .group(group)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build());
        mappedDto = groupMapper.toGroupApiDto(group, true);
        assertNull(mappedDto.getSeverity());
        verifyZeroInteractions(severityPopulator);
    }

    /**
     * GroupApiDto should fill in BillingFamilyApiDTO when BillingFamily request.
     */
    @Test
    public void testToGroupApiDtoBillingFamily() {
        BusinessUnitApiDTO masterAccountDevelopment = new BusinessUnitApiDTO();
        masterAccountDevelopment.setMaster(true);
        masterAccountDevelopment.setUuid("2");
        masterAccountDevelopment.setDisplayName("Development");
        masterAccountDevelopment.setCostPrice(2.5F);
        masterAccountDevelopment.setAssociatedTargetId(123L);

        BusinessUnitApiDTO productTrustSubAccount = new BusinessUnitApiDTO();
        productTrustSubAccount.setMaster(false);
        productTrustSubAccount.setUuid("1");
        productTrustSubAccount.setDisplayName("Product Trust");
        productTrustSubAccount.setCostPrice(3.25F);

        Set<Long> oidsInBillingFamily = new HashSet<>(Arrays.asList(1L, 2L));
        when(businessAccountRetriever.getBusinessAccounts(oidsInBillingFamily))
            .thenReturn(Arrays.asList(
                masterAccountDevelopment, productTrustSubAccount));

        Grouping group = Grouping.newBuilder().setId(8L)
            .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
            .setDefinition(GroupDefinition.newBuilder().setType(GroupType.BILLING_FAMILY)
                .setDisplayName("Development"))
            .build();
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
            .group(group)
            .members(oidsInBillingFamily)
            .entities(Collections.emptyList())
            .build();
        GroupApiDTO mappedDto = groupMapper.toGroupApiDto(
            groupAndMembers,
            EnvironmentType.CLOUD,
            CloudType.AWS,
            false);

        Assert.assertTrue(mappedDto instanceof BillingFamilyApiDTO);
        BillingFamilyApiDTO billingFamilyApiDTO = (BillingFamilyApiDTO)mappedDto;

        Assert.assertEquals("2", billingFamilyApiDTO.getMasterAccountUuid());
        Assert.assertEquals("Development", billingFamilyApiDTO.getDisplayName());

        Assert.assertEquals(
            ImmutableMap.of("2", "Development", "1", "Product Trust"),
            billingFamilyApiDTO.getUuidToNameMap());

        Assert.assertEquals(3.25F + 2.5F, billingFamilyApiDTO.getCostPrice(), 0.0000001F);

        /* Member count should only consider accounts that are monitored by a probe. Accounts that
         * are only submitted as a member of a BillingFamily should not be counted.
         * - development account is discovered by a probe, indicted by associated target id being set
         * - product trust is not discovered by a probe, because the associated target id is no set
         * As a result, the final member count should be 1.
         */
        Assert.assertEquals(Integer.valueOf(1), billingFamilyApiDTO.getMembersCount());
    }

    /**
     * BillingAccountRetriever is not guarenteed to return a cost price because cost component
     * might be temporarily unavailable. When that happens, the billing family should also have
     * a null cost price.
     */
    @Test
    public void testBillingFamilyNoCostPrice() {
        BusinessUnitApiDTO masterAccountDevelopment = new BusinessUnitApiDTO();
        masterAccountDevelopment.setMaster(true);
        masterAccountDevelopment.setUuid("2");
        masterAccountDevelopment.setDisplayName("Development");
        masterAccountDevelopment.setCostPrice(null);

        BusinessUnitApiDTO productTrustSubAccount = new BusinessUnitApiDTO();
        productTrustSubAccount.setMaster(false);
        productTrustSubAccount.setUuid("1");
        productTrustSubAccount.setDisplayName("Product Trust");
        masterAccountDevelopment.setCostPrice(null);

        Set<Long> oidsInBillingFamily = new HashSet<>(Arrays.asList(1L, 2L));
        when(businessAccountRetriever.getBusinessAccounts(oidsInBillingFamily))
            .thenReturn(Arrays.asList(
                masterAccountDevelopment, productTrustSubAccount));

        Grouping group = Grouping.newBuilder().setId(8L)
            .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
            .setDefinition(GroupDefinition.newBuilder().setType(GroupType.BILLING_FAMILY)
                .setDisplayName("Development"))
            .build();
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
            .group(group)
            .members(oidsInBillingFamily)
            .entities(Collections.emptyList())
            .build();
        GroupApiDTO mappedDto = groupMapper.toGroupApiDto(
            groupAndMembers,
            EnvironmentType.CLOUD,
            CloudType.AWS,
            false);

        Assert.assertTrue(mappedDto instanceof BillingFamilyApiDTO);
        BillingFamilyApiDTO billingFamilyApiDTO = (BillingFamilyApiDTO)mappedDto;
        Assert.assertNull(billingFamilyApiDTO.getCostPrice());
    }

    /**
     * If there are no group members we don't need to get call to cost component, because if
     * entityFilter has empty collection of entities, cost component return cost stats for
     * all existed entities.
     */
    @Test
    public void testEmptyResourceGroupNoCostComponentInteraction() {
        Grouping group = Grouping.newBuilder()
                .setId(8L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("empty_rg"))
                .build();
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build();
        groupMapper.toGroupApiDto(groupAndMembers, EnvironmentType.CLOUD, CloudType.AZURE, false);
        final GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setEntityFilter(Cost.EntityFilter.newBuilder()
                                .addAllEntityId(groupAndMembers.members())
                                .build())
                        .build())
                .build();
        Mockito.verify(costServiceMole, times(0)).getCloudCostStats(cloudCostStatsRequest);
    }
}
