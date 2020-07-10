package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.utils.StringConstants.RESOURCE_GROUP;
import static com.vmturbo.common.protobuf.utils.StringConstants.WORKLOAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

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
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ObjectsPage;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Discovered;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit test for {@link GroupMapper}.
 */
public class GroupMapperTest {

    private static final SearchParameters.Builder SEARCH_PARAMETERS = SearchParameters.newBuilder()
                    .setStartingFilter(PropertyFilter.newBuilder().setPropertyName("entityType")
                                    .setStringFilter(StringFilter.newBuilder()
                                                    .setStringPropertyRegex("PhysicalMachine")));

    private static final long CONTEXT_ID = 7777777;

    private static final ThinTargetCache.ThinTargetInfo AWS_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.AWS.getProbeType())
                    .oid(111111L)
                    .build())
            .displayName("SOME CLOUD TARGET")
            .oid(11111L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo VC_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.HYPERVISOR.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.HYPERVISOR.getCategoryInUpperCase())
                    .type(SDKProbeType.VCENTER.getProbeType())
                    .oid(111112L)
                    .build())
            .displayName("VC target")
            .oid(11112L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo AZURE_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.AZURE.getProbeType())
                    .oid(111113L)
                    .build())
            .displayName("TARGET_DISPLAY_NAME")
            .oid(11113L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo APPD_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.GUEST_OS_PROCESSES.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.APPDYNAMICS.getProbeType())
                    .oid(111114L)
                    .build())
            .displayName("AppD Target")
            .oid(11114)
            .isHidden(false)
            .build();

    private static final MinimalEntity ENTITY_VM1 =  MinimalEntity.newBuilder()
            .setOid(3L)
            .setDisplayName("foo")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .addDiscoveringTargetIds(AWS_TARGET.oid())
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
            .build();

    private static final MinimalEntity VC_VM =  MinimalEntity.newBuilder()
            .setOid(4L)
            .setDisplayName("vm-1")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .addDiscoveringTargetIds(VC_TARGET.oid())
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
            .build();

    private static final MinimalEntity VC_VM2 =  MinimalEntity.newBuilder()
            .setOid(5L)
            .setDisplayName("vm-2")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .addDiscoveringTargetIds(VC_TARGET.oid())
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
            .build();

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

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private CostMoles.CostServiceMole costServiceMole;
    private EntitySeverityServiceMole severityService;

    /**
     * gRPC server to mock out inter-component dependencies.
     */
    public GrpcTestServer grpcServer;

    private ThinTargetCache targetCache;

    private EntityFilterMapper entityFilterMapper =
            new EntityFilterMapper(groupUseCaseParser, targetCache);

    private GroupFilterMapper groupFilterMapper = new GroupFilterMapper();

    private SeverityPopulator severityPopulator;

    private final CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();

    private final BusinessAccountRetriever businessAccountRetriever = mock(BusinessAccountRetriever.class);
    private GroupMapper groupMapper;
    private static final String AND = "AND";
    private static final String FOO = "foo";
    private static final String VM_TYPE = "VirtualMachine";
    private Collection<ThinTargetInfo> targets;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exception occurred.
     */
    @Before
    public void setup() throws Exception {
        targets = new ArrayList<>();
        targetCache = Mockito.mock(ThinTargetCache.class);
        Mockito.when(targetCache.getAllTargets()).thenAnswer(invocation -> targets);
        Mockito.when(targetCache.getTargetInfo(Mockito.anyLong()))
                .thenAnswer(invocation -> targets.stream()
                        .filter(target -> target.oid() == invocation.getArgumentAt(0, Long.class))
                        .findFirst());
        severityService = Mockito.spy(new EntitySeverityServiceMole());
        costServiceMole = Mockito.spy(new CostServiceMole());
        grpcServer = GrpcTestServer.newServer(costServiceMole, severityService);
        grpcServer.start();
        severityPopulator = Mockito.spy(
                new SeverityPopulator(EntitySeverityServiceGrpc.newStub(grpcServer.getChannel())));
        groupMapper = new GroupMapper(supplyChainFetcherFactory, groupExpander,
                repositoryApi, entityFilterMapper, groupFilterMapper, severityPopulator,
                businessAccountRetriever, CostServiceGrpc.newStub(grpcServer.getChannel()),
                CONTEXT_ID, targetCache, cloudTypeMapper);
        SearchRequest req = ApiTestUtils.mockSearchIdReq(Collections.emptySet());
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);
    }

    /**
     * Test static group converting GroupApiDTO to GroupInfo.
     *
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testToGroupInfoStaticGroup() throws Exception {
        final String displayName = "group-foo";
        final String groupType = ApiEntityType.VIRTUAL_MACHINE.apiStr();
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
        final String groupType = ApiEntityType.PHYSICAL_MACHINE.apiStr();
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
        assertEquals(ApiEntityType.PHYSICAL_MACHINE.typeNumber(),
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
        final String groupType = ApiEntityType.VIRTUAL_MACHINE.apiStr();
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
        groupDto.setLogicalOperator("OR");
        groupDto.setCriteriaList(criteriaList);

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(groupDto);

        assertEquals(displayName, groupDefinition.getDisplayName());

        final EntityFilter entityFilter = groupDefinition.getEntityFilters().getEntityFilter(0);

        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), entityFilter.getEntityType());
        assertEquals(GroupDefinition.SelectionCriteriaCase.ENTITY_FILTERS,
                        groupDefinition.getSelectionCriteriaCase());
        assertEquals(LogicalOperator.OR, entityFilter.getLogicalOperator());
        assertEquals(2, entityFilter.getSearchParametersCollection().getSearchParametersCount());
        SearchParameters firstSearchParameters =
                        entityFilter.getSearchParametersCollection().getSearchParameters(0);
        // Verify the first search parameters' starting filter is VM entity
        assertEquals("entityType", firstSearchParameters.getStartingFilter().getPropertyName());
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
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
        assertEquals(ApiEntityType.PHYSICAL_MACHINE.typeNumber(),
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
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.typeNumber(), secondSearchParameters
                        .getSearchFilter(2).getPropertyFilter().getNumericFilter().getValue());
    }

    /**
     * Test converting dynamic group info which only has starting filter to groupApiDTO.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testToGroupApiDTOOnlyWithStartingFilter() throws Exception {
        final String displayName = "group-foo";
        final long oid = 123L;

        final Grouping group = Grouping.newBuilder().setId(oid)
                        .addExpectedTypes(MemberType
                                        .newBuilder()
                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition
                            .newBuilder().setType(GroupType.REGULAR).setDisplayName(displayName)
                            .setEntityFilters(EntityFilters.newBuilder().addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
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

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

        assertThat(dto.getUuid(), is(Long.toString(oid)));
        assertThat(dto.getDisplayName(), is(displayName));
        assertThat(dto.getGroupType(), is(ApiEntityType.PHYSICAL_MACHINE.apiStr()));
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
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testToGroupApiDTOWithMultipleSearchParameters() throws Exception {
        final String displayName = "group-foo";
        final Boolean isStatic = false;
        final long oid = 123L;

        final SearchParameters.Builder vmParameters = getVmParameters();
        final SearchParameters.Builder pmParameters = getVmByPmParameters();

        final Grouping group = Grouping.newBuilder().setId(oid)
                        .addExpectedTypes(MemberType
                                        .newBuilder()
                                        .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition
                        .newBuilder().setType(GroupType.REGULAR).setDisplayName(displayName)
                        .setEntityFilters(EntityFilters.newBuilder().addEntityFilter(EntityFilter
                                        .newBuilder()
                                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                                        .setLogicalOperator(LogicalOperator.OR)
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

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

        assertEquals(Long.toString(oid), dto.getUuid());
        assertEquals(displayName, dto.getDisplayName());
        assertEquals("OR", dto.getLogicalOperator());
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.apiStr(), dto.getGroupType());
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

    /**
     * Tests that when converting to GroupApiDto, entities not present in the repository are
     * excluded from the entitiesCount for a group of clusters.
     *
     * @throws InterruptedException from toGroupApiDto
     * @throws ConversionException from toGroupApiDto
     * @throws InvalidOperationException from toGroupApiDto
     */
    @Test
    public void testToGroupApiDtoExcludesEntitiesNotPresentInLiveTopology()
            throws InterruptedException, ConversionException, InvalidOperationException {
        // GIVEN
        final ArrayList<Long> memberIds = new ArrayList<>();
        memberIds.add(1L);
        memberIds.add(2L);
        final ArrayList<Long> entityIds = new ArrayList<>();
        entityIds.add(11L);
        entityIds.add(12L);
        entityIds.add(21L);
        entityIds.add(22L);
        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder()
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                .setType(MemberType.newBuilder()
                                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                                        .build())
                                                .addMembers(1L)
                                                .addMembers(2L)
                                                .build())
                                        .build())
                                .build())
                        .addExpectedTypes(MemberType.newBuilder()
                                .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                .build())
                        .build())
                .members(memberIds)
                .entities(entityIds)
                .build();
        // suppose entities 21 & 22 don't exist in repository
        final ArrayList<MinimalEntity> minimalEntitiesRequest = new ArrayList<>();
        minimalEntitiesRequest.add(MinimalEntity.newBuilder()
                .setOid(11)
                .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                .build());
        minimalEntitiesRequest.add(MinimalEntity.newBuilder()
                .setOid(12)
                .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                .build());
        MultiEntityRequest multiEntityRequest =
                ApiTestUtils.mockMultiMinEntityReq(minimalEntitiesRequest);
        when(repositoryApi.entitiesRequest(new HashSet<>(entityIds)))
                .thenReturn(multiEntityRequest);

        // WHEN
        ObjectsPage<GroupApiDTO> objectsPage = groupMapper.toGroupApiDto(
                Collections.singletonList(groupAndMembers),
                false,
                null,
                null);

        //THEN
        assertEquals(1, objectsPage.getObjects().size());
        assertEquals(minimalEntitiesRequest.size(),
                objectsPage.getObjects().get(0).getEntitiesCount().longValue());
    }

    /**
     * Tests that when we have multiple targets as source for a discovered group, the correct one
     * will be returned. (This will probably be removed when proper handling for multiple targets
     * is implemented). See GroupMapper.chooseTarget() for the logic.
     *
     * @throws InterruptedException from toGroupApiDto
     * @throws ConversionException from toGroupApiDto
     * @throws InvalidOperationException from toGroupApiDto
     */
    @Test
    public void testToGroupApiDtoReturnsCorrectTarget()
            throws InterruptedException, ConversionException, InvalidOperationException {
        final ArrayList<Long> targetIds = new ArrayList();
        final ArrayList<Long> memberIds = new ArrayList();
        final ArrayList<Long> entityIds = new ArrayList();
        long probeOid = 12345678L;

        // check that when all targets are hidden, the one with the lowest uuid is being returned
        long targetOid1 = 124L;
        long targetOid2 = 123L;
        targetIds.add(targetOid1);
        targetIds.add(targetOid2);
        addTargetToTargets(targetOid1, true, probeOid);
        addTargetToTargets(targetOid2, true, probeOid);
        GroupAndMembers groupAndMembers =
                createGroupAndMembersWithDiscoveredTargets(targetIds, memberIds, entityIds);
        final boolean populateSeverity = false;
        final SearchPaginationRequest paginationRequest = null;
        final EnvironmentType requestedEnvironment = null;

        ObjectsPage<GroupApiDTO> objectsPage = groupMapper.toGroupApiDto(
                Collections.singletonList(groupAndMembers),
                populateSeverity,
                paginationRequest,
                requestedEnvironment);

        assertEquals(1, objectsPage.getTotalCount());
        assertNotNull(objectsPage.getObjects().get(0).getSource());
        assertEquals(Long.toString(targetOid2),
                objectsPage.getObjects().get(0).getSource().getUuid());
        assertEquals("target123", objectsPage.getObjects().get(0).getSource().getDisplayName());
        assertEquals("targetCategory", objectsPage.getObjects().get(0).getSource().getCategory());
        assertEquals("targetType", objectsPage.getObjects().get(0).getSource().getType());


        // check that when there are both hidden and non hidden targets, the one with the lowest
        // uuid from the non-hidden onesq is being returned
        long targetOid3 = 126L;
        long targetOid4 = 125L;
        targetIds.add(targetOid3);
        targetIds.add(targetOid4);
        addTargetToTargets(targetOid3, false, probeOid);
        addTargetToTargets(targetOid4, false, probeOid);
        groupAndMembers =
                createGroupAndMembersWithDiscoveredTargets(targetIds, memberIds, entityIds);

        objectsPage = groupMapper.toGroupApiDto(
                Collections.singletonList(groupAndMembers),
                populateSeverity,
                paginationRequest,
                requestedEnvironment);

        assertEquals(1, objectsPage.getTotalCount());
        assertNotNull(objectsPage.getObjects().get(0).getSource());
        assertEquals(Long.toString(targetOid4),
                objectsPage.getObjects().get(0).getSource().getUuid());
        assertEquals("target" + targetOid4,
                objectsPage.getObjects().get(0).getSource().getDisplayName());
        assertEquals("targetCategory", objectsPage.getObjects().get(0).getSource().getCategory());
        assertEquals("targetType", objectsPage.getObjects().get(0).getSource().getType());
    }

    /**
     * Adds a target to the targets array, with dummy display/category/type names.
     * @param targetOid the oid of the target
     * @param isHidden whether the target should be marked as hidden
     * @param probeOid the oid of the probe
     */
    private void addTargetToTargets(long targetOid, boolean isHidden, long probeOid) {
        targets.add(ImmutableThinTargetInfo.builder()
                .oid(targetOid)
                .displayName("target" + targetOid)
                .isHidden(isHidden)
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .oid(probeOid)
                        .category("targetCategory")
                        .uiCategory("targetUiCategory")
                        .type("targetType")
                        .build())
                .build());
    }

    /**
     * Utility function to create a GroupsAndMembers object with discovered origin and source
     * targets.
     * @param targetIds a list of target ids to be set as source targets. The target objects must be
     *                  added to targets array externally.
     * @param memberIds the ids of the members of the group
     * @param entityIds the ids of the entities of the group
     * @return the newly created object
     */
    private GroupAndMembers createGroupAndMembersWithDiscoveredTargets(ArrayList<Long> targetIds,
            ArrayList<Long> memberIds,
            ArrayList<Long> entityIds) {
        return ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder()
                        .setOrigin(Origin.newBuilder()
                                .setDiscovered(Discovered.newBuilder()
                                        .addAllDiscoveringTargetId(targetIds)
                                        .build())
                                .build())
                        .build())
                .members(memberIds)
                .entities(entityIds)
                .build();
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
     * @throws Exception any error happens
     */
    @Test
    public void testVmsByClusterNameToSearchParameters() throws Exception {
        GroupApiDTO groupDto = groupApiDTO(AND, VM_TYPE,
                        filterDTO(EntityFilterMapper.REGEX_MATCH, FOO, "vmsByClusterName"));
        List<SearchParameters> parameters =
                        entityFilterMapper.convertToSearchParameters(
                                        groupDto.getCriteriaList(), groupDto.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);

        // verify that the starting filter is PM
        assertEquals(SearchProtoUtil.entityTypeFilter(ApiEntityType.PHYSICAL_MACHINE), param.getStartingFilter());

        // 2 search filters after starting filter
        assertEquals(3, param.getSearchFilterCount());

        // 1. first one is Cluster Membership Filter, verify that it was created
        assertTrue(param.getSearchFilter(0).hasGroupFilter());
        final GroupFilter groupFilter =
                        param.getSearchFilter(0).getGroupFilter();
        // verify that we are looking for clusters with name FOO
        assertEquals("^" + FOO + "$", groupFilter.getGroupSpecifier()
                        .getStringFilter().getStringPropertyRegex());

        // 2. second one is traversal filter (produces) used to traverse one hop
        assertEquals(SearchProtoUtil.searchFilterTraversal(
                SearchProtoUtil.numberOfHops(TraversalDirection.PRODUCES, 1)),
                param.getSearchFilter(1));

        // 3. third one is an entity type filter to only return VMs
        assertEquals(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.entityTypeFilter(EntityType.VIRTUAL_MACHINE_VALUE)),
                param.getSearchFilter(2));

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
        assertEquals("RXEQ", vmsByClusterNameFilter.getExpType());
        assertEquals(FOO, vmsByClusterNameFilter.getExpVal());
    }

    /**
     * Test that the PM group criteria by clusters name is converted to SearchParameters correctly.
     * @throws OperationFailedException any error happens
     */
    @Test
    public void testPMsByClusterNameToSearchParameters() throws OperationFailedException {
        GroupApiDTO groupDto = groupApiDTO(AND, ApiEntityType.PHYSICAL_MACHINE.apiStr(),
                filterDTO(EntityFilterMapper.REGEX_MATCH, FOO, "pmsByClusterName"));
        List<SearchParameters> parameters =
                entityFilterMapper.convertToSearchParameters(
                        groupDto.getCriteriaList(), groupDto.getClassName(), null);
        assertEquals(1, parameters.size());
        SearchParameters param = parameters.get(0);

        // verify that the starting filter is PM
        assertEquals(SearchProtoUtil.entityTypeFilter(ApiEntityType.PHYSICAL_MACHINE),
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

    /**
     * Test that a single invalid group doesn't prevent the mapping of other valid groups.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testMapInvalidGroup() throws Exception {

        final Grouping invalidGroup = Grouping.newBuilder().setId(77777L)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("not cool boy")
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupDTO.GroupFilter.newBuilder()
                        // This filter is currently invalid because it's adding a "tags" property filter
                        // without specifying that we are looking for a specific group type.
                        .addPropertyFilters(PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
                            .setMapFilter(MapFilter.newBuilder()
                                .setKey("foo")
                                .addValues("bar"))))))
            .build();
        final Grouping validGroup = Grouping.newBuilder().setId(7L)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("cool boy")
                .setStaticGroupMembers(StaticMembers.newBuilder()))
            .build();

        when(groupExpander.getMembersForGroup(invalidGroup)).thenReturn(ImmutableGroupAndMembers
            .builder().group(invalidGroup)
            .members(Collections.emptyList())
            .entities(Collections.emptyList())
            .build());
        when(groupExpander.getMembersForGroup(validGroup)).thenReturn(ImmutableGroupAndMembers
            .builder().group(validGroup)
            .members(Collections.emptyList())
            .entities(Collections.emptyList())
            .build());

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final Map<Long, GroupApiDTO> dto =
            // The valid group is second in the order, so we make sure we continue processing
            // after the invalid group.
            groupMapper.groupsToGroupApiDto(Arrays.asList(invalidGroup, validGroup), false);

        // Only the valid group got returned.
        assertThat(dto.keySet(), containsInAnyOrder(validGroup.getId()));
        // We don't do additional checks, because the job of this test isn't to check how the group
        // is mapped, but THAT the group is mapped even if a previous group had mapping errors.
        assertEquals("7", dto.get(validGroup.getId()).getUuid());
    }

    @Test
    public void testMapComputeCluster() throws Exception {

        final Grouping computeCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                                        .setDisplayName("cool boy")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE
                                                                                                        .typeNumber()))
                                                                        .addMembers(10L))))
                        .build();

        when(groupExpander.getMembersForGroup(computeCluster)).thenReturn(ImmutableGroupAndMembers
                        .builder().group(computeCluster)
                        .members(GroupProtoUtil.getAllStaticMembers(computeCluster.getDefinition()))
                        .entities(GroupProtoUtil
                                        .getAllStaticMembers(computeCluster.getDefinition()))
                        .build());

        final MinimalEntity entPM = MinimalEntity.newBuilder()
                .setOid(10L)
                .build();
        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(entPM));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(computeCluster), false)
                        .values()
                        .iterator()
                        .next();
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool boy", dto.getDisplayName());
        assertEquals(ApiEntityType.PHYSICAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testMapComputeVirtualMachineCluster() throws Exception {

        final Grouping computeVirtualMachineCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                                        .setDisplayName("red silence")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(ApiEntityType.VIRTUAL_MACHINE
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

        final MinimalEntity ent = MinimalEntity.newBuilder()
                .setOid(10L)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO dto = groupMapper.groupsToGroupApiDto(
                Collections.singletonList(computeVirtualMachineCluster), false)
                .values()
                .iterator()
                .next();
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.VIRTUAL_MACHINE_CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("red silence", dto.getDisplayName());
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testMapStorageCluster() throws Exception {

        final Grouping storageCluster = Grouping.newBuilder().setId(7L)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.STORAGE_CLUSTER)
                                        .setDisplayName("cool girl")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(ApiEntityType.STORAGE
                                                                                                        .typeNumber()))
                                                                        .addMembers(10L))))
                        .build();

        when(groupExpander.getMembersForGroup(storageCluster)).thenReturn(ImmutableGroupAndMembers
                        .builder().group(storageCluster)
                        .members(GroupProtoUtil.getAllStaticMembers(storageCluster.getDefinition()))
                        .entities(GroupProtoUtil
                                        .getAllStaticMembers(storageCluster.getDefinition()))
                        .build());

        final MinimalEntity ent = MinimalEntity.newBuilder()
                .setOid(10L)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(storageCluster), false)
                        .values()
                        .iterator()
                        .next();
        assertEquals("7", dto.getUuid());
        assertEquals(StringConstants.STORAGE_CLUSTER, dto.getClassName());
        assertEquals(true, dto.getIsStatic());
        assertEquals(1, dto.getMembersCount().intValue());
        assertEquals(1, dto.getEntitiesCount().intValue());
        assertEquals(1, dto.getMemberUuidList().size());
        assertEquals("10", dto.getMemberUuidList().get(0));
        assertEquals("cool girl", dto.getDisplayName());
        assertEquals(ApiEntityType.STORAGE.apiStr(), dto.getGroupType());
        assertEquals(EnvironmentType.ONPREM, dto.getEnvironmentType());
    }

    @Test
    public void testToTempGroupProtoGlobalScope() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        apiDTO.setEnvironmentType(EnvironmentType.ONPREM);

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
                        is(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertTrue(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(
                        Collections.singletonList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoNotGlobalScope() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setEnvironmentType(EnvironmentType.ONPREM);
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
                        is(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testToTempGroupProtoUuidList() throws Exception {
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
                        is(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(1L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());
    }

    @Test
    public void testToTempGroupProtoUuidListInsideScope() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setDisplayName("foo");
        apiDTO.setGroupType(VM_TYPE);
        apiDTO.setScope(Lists.newArrayList("1"));
        apiDTO.setEnvironmentType(EnvironmentType.ONPREM);
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
                        is(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(memberByType.getMembersList(), containsInAnyOrder(7L));
        assertThat(groupDefinition.getDisplayName(), is("foo"));
        assertFalse(groupDefinition.getOptimizationMetadata().getIsGlobalScope());

        verify(fetcherBuilder).addSeedUuids(Collections.singletonList("1"));
        verify(fetcherBuilder).entityTypes(Collections.singletonList(VM_TYPE));
    }

    @Test
    public void testMapTempGroup() throws Exception {
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
                        .addExpectedTypes(MemberType.newBuilder().setGroup(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(Collections.singleton(1L))
                        // Temp groups will never have different entity count, but we want to check the
                        // entity count gets set from the right field in GroupAndMembers.
                        .entities(ImmutableList.of(4L, 5L)).build());

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(VC_VM, VC_VM2));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

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
    public void testMapTempGroupCloud() throws Exception {
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
        Mockito.when(targetCache.getAllTargets()).thenReturn(Collections.singletonList(AWS_TARGET));

        final MinimalEntity ent = MinimalEntity.newBuilder()
                .setOid(1L)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

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
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testMapResourceGroup() throws Exception {
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
                            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addMembers(1L)
                        .addMembers(2L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(ApiEntityType.DATABASE.typeNumber()))
                        .addMembers(3L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(ApiEntityType.DATABASE_SERVER.typeNumber()))
                        .addMembers(4L))
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(ApiEntityType.VIRTUAL_VOLUME.typeNumber()))
                        .addAllMembers(Arrays.asList(5L, 6L, 7L, 8L, 9L)))
                ))
            .build();

        List<MinimalEntity> entities = new ArrayList<>();
        for (long i = 1; i <= 9; i++) {
            entities.add(MinimalEntity.newBuilder()
                    .setOid(i)
                    .build());
        }
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(entities);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final MultiEntityRequest testRg = ApiTestUtils.mockMultiMinEntityReq(
                Collections.singletonList(MinimalEntity.newBuilder()
                        .setOid(parentId)
                        .setDisplayName(parentDisplayName)
                        .build()));
        when(repositoryApi.entitiesRequest(Collections.singleton(parentId))).thenReturn(testRg);

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
            .group(group).entities(GroupProtoUtil.getStaticMembers(group))
            .members(GroupProtoUtil.getStaticMembers(group)).build());
        Mockito.when(targetCache.getAllTargets())
                .thenReturn(Collections.singletonList(AWS_TARGET));

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

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
    public void testMapGroupActiveEntities() throws Exception {
        final Grouping group = Grouping.newBuilder().setId(8L)
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                        .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                                        .setDisplayName("foo")
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(ApiEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber()))
                                                                        .addMembers(1L).addMembers(2L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(ImmutableSet.of(1L, 2L))
                        .members(ImmutableSet.of(1L, 2L)).build());

        final MinimalEntity ent1 = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEntityState(EntityState.POWERED_ON)
                .build();

        final MinimalEntity ent2 = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEntityState(EntityState.POWERED_OFF)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent1, ent2));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final int count = 1;

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getActiveEntitiesCount(), is(count));
    }

    @Test
    public void testMapGroupActiveEntitiesGlobalTempGroup() throws Exception {
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
                                                                                        .setEntity(ApiEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber())))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(Collections.singleton(1L))
                        .members(Collections.singleton(1L)).build());

        final SearchRequest countReq = ApiTestUtils.mockSearchIdReq(Collections.singleton(1L));
        when(repositoryApi.newSearchRequest(any())).thenReturn(countReq);

        final MinimalEntity ent = MinimalEntity.newBuilder()
                .setOid(1L)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        assertThat(mappedDto.getUuid(), is("8"));
        Assert.assertEquals(Integer.valueOf(1), mappedDto.getActiveEntitiesCount());

        final ArgumentCaptor<SearchParameters> captor =
                        ArgumentCaptor.forClass(SearchParameters.class);
        verify(repositoryApi, Mockito.times(1)).newSearchRequest(captor.capture());
        final SearchParameters params = captor.getValue();
        assertThat(params.getStartingFilter(),
                        is(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_MACHINE)));
        assertThat(params.getSearchFilterList(), containsInAnyOrder(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.stateFilter(UIEntityState.ACTIVE))));
    }

    @Test
    public void testMapGroupActiveEntitiesException() throws Exception {
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
                                                                                        .setEntity(ApiEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber())))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(Collections.singleton(1L))
                        .members(Collections.singleton(1L)).build());

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        when(repositoryApi.newSearchRequest(any())).thenThrow(Status.INTERNAL.asRuntimeException());

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        assertThat(mappedDto.getUuid(), is("8"));
        // The fallback is the number of entities.
        assertThat(mappedDto.getActiveEntitiesCount(), is(1));
    }

    @Test
    public void testMapTempGroupONPREM() throws Exception {
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
                        .addExpectedTypes(MemberType.newBuilder().setGroup(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).members(Collections.singleton(1L))
                        // Return a different entity set to make sure it gets used for the entity count.
                        .entities(ImmutableSet.of(4L, 5L)).build());
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(VC_VM, VC_VM2));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

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
                                                                        .addMembers(1L)
                                                                        .addMembers(2L))))
                        .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                        .group(group).entities(GroupProtoUtil.getStaticMembers(group))
                        .members(GroupProtoUtil.getStaticMembers(group)).build());

        final MultiEntityRequest request = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(
                MinimalEntity.newBuilder()
                        .setOid(1L)
                        .addDiscoveringTargetIds(VC_TARGET.oid())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .build(), MinimalEntity.newBuilder()
                        .setOid(2L)
                        .addDiscoveringTargetIds(AWS_TARGET.oid())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build()));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(request);
        Mockito.when(targetCache.getAllTargets()).thenReturn(Arrays.asList(VC_TARGET, AWS_TARGET));
        Mockito.when(targetCache.getTargetInfo(VC_TARGET.oid())).thenReturn(Optional.of(VC_TARGET));
        Mockito.when(targetCache.getTargetInfo(AWS_TARGET.oid()))
                .thenReturn(Optional.of(AWS_TARGET));

        // if no cloud targets, it should be ONPREM
        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        // mock one cloud target and expect the environment type to be HYBRID
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.HYBRID));
    }

    @Test
    public void testStaticGroupMembersCount() throws Exception {
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

        final MinimalEntity entPM1 = MinimalEntity.newBuilder()
                .setOid(10L)
                .build();
        final MinimalEntity entPM2 = MinimalEntity.newBuilder()
                .setOid(20L)
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(entPM1, entPM2));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        assertEquals("7", dto.getUuid());
        assertEquals(true, dto.getIsStatic());
        assertThat(dto.getEntitiesCount(), is(2));
        assertThat(dto.getMemberUuidList(), containsInAnyOrder("10", "20"));
    }

    @Test
    public void testDynamicGroupMembersCount() throws Exception {
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
        final MultiEntityRequest req =
                ApiTestUtils.mockMultiMinEntityReq(Collections.emptyList());
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final GroupApiDTO dto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
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
                    throws Exception {
        final String displayName = "group-foo";
        final String groupType = ApiEntityType.VIRTUAL_MACHINE.apiStr();
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
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
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
        final String groupType = ApiEntityType.VIRTUAL_MACHINE.apiStr();
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
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
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
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEnvironmentTypeForGroup() throws Exception {
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
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        MinimalEntity entVM2 =  MinimalEntity.newBuilder()
                        .setOid(uuid2)
                        .setDisplayName("foo")
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .build();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);
        listVMs.add(entVM2);
        Mockito.when(targetCache.getAllTargets()).thenReturn(Arrays.asList(VC_TARGET, AWS_TARGET));

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(any())).thenReturn(req1);
        final SearchRequest searchRequest = ApiTestUtils.mockEmptySearchReq();
        Mockito.when(repositoryApi.newSearchRequest(Mockito.any())).thenReturn(searchRequest);

        final GroupApiDTO groupDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();
        Assert.assertEquals(groupDto.getEnvironmentType(), EnvironmentType.HYBRID);
    }

    /**
     * Test {@link GroupMapper#toGroupApiDto(List, boolean, SearchPaginationRequest, EnvironmentType)}}
     * in case when we have regular group with empty resource group members.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForRegularGroupWithCloudGroupMembers()
            throws Exception {
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
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(ENTITY_VM1));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);

        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final GroupApiDTO groupDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        Assert.assertEquals(groupDto.getEnvironmentType(), EnvironmentType.CLOUD);
    }

    /**
     * Test {@link GroupMapper#toGroupApiDto(List, boolean, SearchPaginationRequest,
     * EnvironmentType)}} in case when we have empty resource group.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForCloudResourceGroup() throws Exception {
        final Grouping rg = Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.RESOURCE).build())
                .build();
        final ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg)
                .members(Collections.emptySet())
                .entities(Collections.emptySet())
                .build();
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(ENTITY_VM1));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);

        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final GroupApiDTO groupDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        Assert.assertEquals(groupDto.getEnvironmentType(), EnvironmentType.CLOUD);
    }

    /**
     * Test {@link GroupMapper#toGroupApiDto(List, boolean, SearchPaginationRequest,
     * EnvironmentType)}} in case when we have empty billing family.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForBillingFamily() throws Exception {
        final Grouping rg = Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.BILLING_FAMILY).build())
                .build();
        final ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg)
                .members(Collections.emptySet())
                .entities(Collections.emptySet())
                .build();
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(ENTITY_VM1));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);

        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final GroupApiDTO groupDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        Assert.assertEquals(groupDto.getEnvironmentType(), EnvironmentType.CLOUD);
    }

    /**
     * Test getEnvironmentTypeForGroup returns proper type for a group (ON_PREM, CLOUD, HYBRID).
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetCloudTypeForGroup() throws Exception {
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


        final MinimalEntity entVM1 =  MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo1")
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
        final MinimalEntity entVM2 =  MinimalEntity.newBuilder()
                .setOid(uuid2)
                .setDisplayName("foo2")
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);

        final GroupAndMembers groupAws = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1))
                .entities(ImmutableSet.of(uuid1))
                .build();

        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        targets.add(AZURE_TARGET);

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);
        // test with only one type, cloudType should be AWS
        final GroupApiDTO groupApiDTO =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAws), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(groupApiDTO.getCloudType(), CloudType.AWS);

        final GroupAndMembers groupHybrid = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1, uuid2))
                .entities(ImmutableSet.of(uuid1, uuid2))
                .build();
        listVMs.add(entVM2);

        MultiEntityRequest req2 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req2);
        // test with both AWS and Azure, cloudType should be Hybrid
        final GroupApiDTO groupApiDTO2 =
                groupMapper.toGroupApiDto(Collections.singletonList(groupHybrid), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(groupApiDTO2.getCloudType(), CloudType.HYBRID);
    }

    /**
     * Test {@link GroupMapper#toGroupApiDto(List, boolean, SearchPaginationRequest,
     * EnvironmentType)} when group has members discovered from different (on-prem and cloud) targets.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEnvironmentAndCloudTypeForHybridGroup() throws Exception {
        final String displayName = "hybrid-group";
        final int groupType = EntityType.VIRTUAL_MACHINE.getNumber();
        final long oid = 123L;
        final long uuid1 = 1L;
        final long uuid2 = 2L;
        final long uuid3 = 3L;

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
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final MinimalEntity entVM2 = MinimalEntity.newBuilder()
                .setOid(uuid2)
                .setDisplayName("foo2")
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final MinimalEntity entVM3 = MinimalEntity.newBuilder()
                .setOid(uuid3)
                .setDisplayName("foo3")
                .addDiscoveringTargetIds(VC_TARGET.oid())
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                .build();

        targets.add(AWS_TARGET);
        targets.add(AZURE_TARGET);
        targets.add(VC_TARGET);

        final List<MinimalEntity> listVMs = Arrays.asList(entVM1, entVM2, entVM3);
        final MultiEntityRequest minEntityReq = ApiTestUtils.mockMultiMinEntityReq(listVMs);

        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1, uuid2, uuid3))
                .entities(ImmutableSet.of(uuid1, uuid2, uuid3))
                .build();
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(minEntityReq);

        final GroupApiDTO groupApiDTO =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), true, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(EnvironmentType.HYBRID, groupApiDTO.getEnvironmentType());
        assertEquals(CloudType.HYBRID, groupApiDTO.getCloudType());
    }

    /**
     * Test {@link GroupMapper#toGroupApiDto(List, boolean, SearchPaginationRequest,
     * EnvironmentType)} when group has ARM members discovered from Cloud and Hybrid targets.
     * In this case, the group's environment type should be set to the group's
     * {@link OptimizationMetadata} environment type.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEnvironmentTypeForCloudGroupWithHybridArmEntities() throws Exception {
        final String displayName = "cloud-group";
        final int groupType = EntityType.BUSINESS_APPLICATION.getNumber();
        final long oid = 123L;
        final long uuid1 = 1L;
        final long uuid2 = 2L;

        final Grouping group = Grouping.newBuilder()
                .setId(oid)
                .addExpectedTypes(MemberType.newBuilder().setEntity(groupType))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName(displayName)
                        .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build())
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .addAllMembers(Arrays.asList(uuid1, uuid2))
                                        .build())
                                .build()))
                .build();

        final MinimalEntity awsBA = MinimalEntity.newBuilder()
                .setOid(uuid1)
                .setDisplayName("foo1")
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .setEntityType(ApiEntityType.BUSINESS_APPLICATION.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();

        final MinimalEntity appDynamicsBA = MinimalEntity.newBuilder()
                .setOid(uuid2)
                .setDisplayName("foo2")
                .addDiscoveringTargetIds(APPD_TARGET.oid())
                .setEntityType(ApiEntityType.BUSINESS_APPLICATION.typeNumber())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.HYBRID)
                .build();

        targets.add(AWS_TARGET);
        targets.add(APPD_TARGET);

        final List<MinimalEntity> listBAs = Arrays.asList(awsBA, appDynamicsBA);
        final MultiEntityRequest minEntityReq = ApiTestUtils.mockMultiMinEntityReq(listBAs);

        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(group)
                .members(ImmutableSet.of(uuid1, uuid2))
                .entities(ImmutableSet.of(uuid1, uuid2))
                .build();
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(minEntityReq);

        final GroupApiDTO groupApiDTO =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), true,
                        null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(EnvironmentType.CLOUD, groupApiDTO.getEnvironmentType());
    }

    /**
     * Test getEnvironmentTypeForGroup returns proper type for a group of resource groups. When
     * all entities from both group have the same {@link EnvironmentType} then group also has that
     * {@link EnvironmentType}.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetEnvironmentTypeForGroupOfResourceGroups() throws Exception {
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
        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);

        final List<MinimalEntity> listVMs = Arrays.asList(entVM1, entVM2);

        final MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);
        final GroupApiDTO groupDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(groupDto.getEnvironmentType(), EnvironmentType.CLOUD);
    }

    /**
     * A cloud entity in a group that belongs to a Cloud target and a Non-cloud target should not
     * cause an exception. We should still be able to determine that the AWS entity has cloud type
     * AWS and CLOUD environment type.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testCloudEntityStitchedByNonCloudTarget() throws Exception {
        final long entityId = 1L;
        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder().setId(2L).build())
                .members(Collections.singletonList(entityId))
                .entities(Collections.singletonList(entityId))
                .build();

        final MinimalEntity entVM1 =  MinimalEntity.newBuilder()
                .setOid(entityId)
            .setDisplayName("VM in cloud stitched to AppD")
            .addDiscoveringTargetIds(APPD_TARGET.oid())
            .addDiscoveringTargetIds(AWS_TARGET.oid())
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEnvironmentType(com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.CLOUD)
            .buildPartial();
        List<MinimalEntity> listVMs = new ArrayList<>();
        listVMs.add(entVM1);

        targets.add(AWS_TARGET);
        targets.add(APPD_TARGET);

        MultiEntityRequest req1 = ApiTestUtils.mockMultiMinEntityReq(listVMs);
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req1);

        final GroupApiDTO convertedDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();
        assertEquals(EnvironmentType.CLOUD, convertedDto.getEnvironmentType());
        assertEquals(CloudType.AWS, convertedDto.getCloudType());
    }


    /**
     * Test that the severity field on GroupApiDTO is populated as expected.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testPopulateSeverityOnGroupApiDTO() throws Exception {
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
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .build()));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);
        Mockito.when(severityService.getEntitySeverities(Mockito.any()))
                .thenReturn(Collections.singletonList(EntitySeveritiesResponse.newBuilder()
                        .setEntitySeverity(EntitySeveritiesChunk.newBuilder()
                                .addEntitySeverity(EntitySeverity.newBuilder()
                                        .setEntityId(1L)
                                        .setSeverity(Severity.CRITICAL)))
                        .build()));

        GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), true)
                        .values()
                        .iterator()
                        .next();
        // verify that calculateSeverity is invoked and severity is populated
        assertEquals(mappedDto.getSeverity(), Severity.CRITICAL.name());
        verify(severityPopulator).getSeverityMap(eq(CONTEXT_ID), eq(Collections.singleton(1L)));

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
        mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), true)
                        .values()
                        .iterator()
                        .next();
        assertNull(mappedDto.getSeverity());
    }

    /**
     * GroupApiDto should fill in BillingFamilyApiDTO when BillingFamily request.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testToGroupApiDtoBillingFamily() throws Exception {
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
        GroupApiDTO mappedDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();

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
        Assert.assertEquals(Integer.valueOf(1), billingFamilyApiDTO.getEntitiesCount());
        Assert.assertTrue(billingFamilyApiDTO.getMemberUuidList().contains("2"));
    }

    /**
     * BillingAccountRetriever is not guarenteed to return a cost price because cost component
     * might be temporarily unavailable. When that happens, the billing family should also have
     * a null cost price.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testBillingFamilyNoCostPrice() throws Exception {
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
        final GroupApiDTO mappedDto =
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null)
                        .getObjects()
                        .iterator()
                        .next();

        Assert.assertTrue(mappedDto instanceof BillingFamilyApiDTO);
        BillingFamilyApiDTO billingFamilyApiDTO = (BillingFamilyApiDTO)mappedDto;
        Assert.assertNull(billingFamilyApiDTO.getCostPrice());
    }

    /**
     * If there are no group members we don't need to get call to cost component, because if
     * entityFilter has empty collection of entities, cost component return cost stats for
     * all existed entities.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEmptyResourceGroupNoCostComponentInteraction() throws Exception {
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
        groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers), false, null, null);
        final GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setEntityFilter(Cost.EntityFilter.newBuilder()
                                .addAllEntityId(groupAndMembers.members())
                                .build())
                        .build())
                .build();
        Mockito.verify(costServiceMole, times(0)).getCloudCostStats(cloudCostStatsRequest);
    }

    /**
     * Tests pagination by group cost. Test covers non-empty group, empty group and non-cloud group
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSortGroupsByCost() throws Exception {
        final Grouping group1 = Grouping.newBuilder()
                .setId(1L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("rg-1"))
                .build();
        final GroupAndMembers groupAndMembers1 = ImmutableGroupAndMembers.builder()
                .group(group1)
                .entities(Arrays.asList(10L, 11L))
                .members(Arrays.asList(10L, 11L))
                .build();
        final Grouping group2 = Grouping.newBuilder()
                .setId(2L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("empty-rg"))
                .build();
        final GroupAndMembers groupAndMembers2 = ImmutableGroupAndMembers.builder()
                .group(group2)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build();
        final Grouping group3 = Grouping.newBuilder()
                .setId(3L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("non-cloud-group-3"))
                .build();
        final GroupAndMembers groupAndMembers3 = ImmutableGroupAndMembers.builder()
                .group(group3)
                .entities(Arrays.asList(31L, 32L))
                .members(Arrays.asList(31L, 32L))
                .build();
        final MultiEntityRequest req =
                ApiTestUtils.mockMultiMinEntityReq(Collections.emptyList());
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 2, true, "COST");
        Mockito.when(costServiceMole.getCloudCostStats(Mockito.any()))
                .thenReturn(Collections.singletonList(GetCloudCostStatsResponse.newBuilder()
                        .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(10L)
                                        .setValues(StatValue.newBuilder().setTotal(100F)))
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(11L)
                                        .setValues(StatValue.newBuilder().setTotal(202F))))
                        .build()));
        final List<GroupApiDTO> resultPage1 = groupMapper.toGroupApiDto(
                Arrays.asList(groupAndMembers1, groupAndMembers2, groupAndMembers3), false,
                paginationRequest, null).getObjects();
        Assert.assertEquals(2, resultPage1.size());
        Assert.assertEquals(Sets.newHashSet(group2.getId(), group3.getId()), resultPage1.stream()
                .map(GroupApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toSet()));

        final SearchPaginationRequest paginationRequest2 =
                new SearchPaginationRequest("2", 2, true, "COST");
        final List<GroupApiDTO> resultPage2 = groupMapper.toGroupApiDto(
                Arrays.asList(groupAndMembers1, groupAndMembers2, groupAndMembers3), false,
                paginationRequest2, null).getObjects();
        Assert.assertEquals(1, resultPage2.size());
        Assert.assertEquals(group1.getId(),
                Long.parseLong(resultPage2.iterator().next().getUuid()));
    }

    /**
     * Assert that we still return partial results when individual component queries fail.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMissingDataDueToError() throws Exception {
        final Grouping group1 = Grouping.newBuilder()
            .setId(1L)
            .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.RESOURCE)
                .setOwner(77)
                .setDisplayName("rg-1"))
            .build();
        final GroupAndMembers groupAndMembers1 = ImmutableGroupAndMembers.builder()
            .group(group1)
            .entities(Arrays.asList(10L, 11L))
            .members(Arrays.asList(10L, 11L))
            .build();
        final SearchPaginationRequest paginationRequest =
            new SearchPaginationRequest("0", 3, false, "SEVERITY");
        // Severity returns error.
        doReturn(Optional.of(Status.INTERNAL.asException())).when(severityService).getEntitySeveritiesError(any());

        // Cost service returns error.
        doReturn(Optional.of(Status.INTERNAL.asException())).when(costServiceMole).getCloudCostStatsError(any());

        // Entity search throws exception.
        RepositoryApi.SearchRequest req = ApiTestUtils.mockSearchIdReq(Collections.emptySet());
        CompletableFuture<Set<Long>> oidsFuture = new CompletableFuture<>();
        oidsFuture.completeExceptionally(Status.INTERNAL.asRuntimeException());
        when(req.getOidsFuture()).thenReturn(oidsFuture);
        when(repositoryApi.newSearchRequest(any())).thenReturn(req);

        // Entities fetch throws exception.
        RepositoryApi.MultiEntityRequest entityReq = ApiTestUtils.mockMultiMinEntityReq(Collections.emptyList());
        CompletableFuture<Set<Long>> minsFuture = new CompletableFuture<>();
        minsFuture.completeExceptionally(Status.INTERNAL.asRuntimeException());
        doAnswer(invocation -> {
            StreamObserver<MinimalEntity> observer = invocation.getArgumentAt(0, StreamObserver.class);
            observer.onError(Status.INTERNAL.asRuntimeException());
            return null;
        }).when(entityReq).getMinimalEntities(any());
        when(repositoryApi.entitiesRequest(any())).thenReturn(entityReq);

        final ObjectsPage<GroupApiDTO> resultPage = groupMapper.toGroupApiDto(
            Arrays.asList(groupAndMembers1), false,
            paginationRequest, null);

        assertThat(resultPage.getObjects().size(), is(1));
    }

    /**
     * Method tests sorting groups by severities.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSortGroupsBySeverity() throws Exception {
        final Grouping group1 = Grouping.newBuilder()
                .setId(1L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("rg-1"))
                .build();
        final GroupAndMembers groupAndMembers1 = ImmutableGroupAndMembers.builder()
                .group(group1)
                .entities(Arrays.asList(10L, 11L))
                .members(Arrays.asList(10L, 11L))
                .build();
        final Grouping group2 = Grouping.newBuilder()
                .setId(2L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("empty-rg"))
                .build();
        final GroupAndMembers groupAndMembers2 = ImmutableGroupAndMembers.builder()
                .group(group2)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build();
        final Grouping group3 = Grouping.newBuilder()
                .setId(3L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("non-cloud-group-3"))
                .build();
        final GroupAndMembers groupAndMembers3 = ImmutableGroupAndMembers.builder()
                .group(group3)
                .entities(Arrays.asList(31L, 32L))
                .members(Arrays.asList(31L, 32L))
                .build();
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 3, false, "SEVERITY");
        Mockito.when(severityService.getEntitySeverities(Mockito.any()))
                .thenReturn(Arrays.asList(EntitySeveritiesResponse.newBuilder()
                        .setEntitySeverity(EntitySeveritiesChunk.newBuilder()
                                .addEntitySeverity(EntitySeverity.newBuilder()
                                        .setEntityId(10L)
                                        .setSeverity(Severity.NORMAL))
                                .addEntitySeverity(EntitySeverity.newBuilder()
                                        .setEntityId(11L)
                                        .setSeverity(Severity.MAJOR))
                                .addEntitySeverity(EntitySeverity.newBuilder()
                                        .setEntityId(31L)
                                        .setSeverity(Severity.CRITICAL))
                        )
                        .build()));
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList());
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);
        final List<GroupApiDTO> resultPage = groupMapper.toGroupApiDto(
                Arrays.asList(groupAndMembers1, groupAndMembers2, groupAndMembers3), false,
                paginationRequest, null).getObjects();
        Assert.assertEquals(3, resultPage.size());
        Assert.assertEquals(Long.toString(group3.getId()), resultPage.get(0).getUuid());
        Assert.assertEquals(Long.toString(group1.getId()), resultPage.get(1).getUuid());
        Assert.assertEquals(Long.toString(group2.getId()), resultPage.get(2).getUuid());
    }

    /**
     * Method tests sorting groups by names.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSortGroupsByName() throws Exception {
        final Grouping group1 = Grouping.newBuilder()
                .setId(1L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("rg-1"))
                .build();
        final GroupAndMembers groupAndMembers1 = ImmutableGroupAndMembers.builder()
                .group(group1)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build();
        final Grouping group2 = Grouping.newBuilder()
                .setId(2L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.RESOURCE)
                        .setDisplayName("empty-rg"))
                .build();
        final GroupAndMembers groupAndMembers2 = ImmutableGroupAndMembers.builder()
                .group(group2)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build();
        final Grouping group3 = Grouping.newBuilder()
                .setId(3L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("non-cloud-group-3"))
                .build();
        final GroupAndMembers groupAndMembers3 = ImmutableGroupAndMembers.builder()
                .group(group3)
                .entities(Collections.emptyList())
                .members(Collections.emptyList())
                .build();
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest("0", 3, true, "NAME");
        final List<GroupApiDTO> resultPage = groupMapper.toGroupApiDto(
                Arrays.asList(groupAndMembers1, groupAndMembers2, groupAndMembers3), false,
                paginationRequest, null).getObjects();
        Assert.assertEquals(3, resultPage.size());
        Assert.assertEquals(Long.toString(group2.getId()), resultPage.get(0).getUuid());
        Assert.assertEquals(Long.toString(group3.getId()), resultPage.get(1).getUuid());
        Assert.assertEquals(Long.toString(group1.getId()), resultPage.get(2).getUuid());
    }

    /**
     * Tests how the OnPrem VM group is filtered in group mapper. OnPrem group is expected to be
     * hidden only if cloud environment is requested
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFilterByEnvironmentTypeVcVms() throws Exception {
        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final Grouping grouping = Grouping.newBuilder().setId(1L).build();
        final GroupAndMembers groupAndMembers1 = createGroup(grouping, VC_VM.getOid());
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(VC_VM));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        null).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.ONPREM).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.HYBRID).getObjects().size());
        Assert.assertEquals(0,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.CLOUD).getObjects().size());
    }

    /**
     * Tests how the cloud VM group is filtered in group mapper. Cloud group should not be shown
     * only if OnPrem environment is requested.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFilterByEnvironmentTypeCloudGroup() throws Exception {
        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final Grouping grouping = Grouping.newBuilder().setId(1L).build();
        final GroupAndMembers groupAndMembers1 = createGroup(grouping, ENTITY_VM1.getOid());
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(ENTITY_VM1));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        null).getObjects().size());
        Assert.assertEquals(0,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.ONPREM).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.HYBRID).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.CLOUD).getObjects().size());
    }

    /**
     * Tests how the hybrid VM group is filtered in group mapper. Hybrid group should not be shown
     * in all the cases.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFilterByEnvironmentTypeHybridGroup() throws Exception {
        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final Grouping grouping = Grouping.newBuilder().setId(1L).build();
        final GroupAndMembers groupAndMembers1 = createGroup(grouping, ENTITY_VM1.getOid(), VC_VM.getOid());
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ENTITY_VM1, VC_VM));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        null).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.ONPREM).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.HYBRID).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.CLOUD).getObjects().size());
    }

    /**
     * Filtering by environment type in homogeneous infrastructure.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFilterByEnvironmentTypeHomogenousEnv() throws Exception {
        targets.add(VC_TARGET);
        final Grouping grouping = Grouping.newBuilder().setId(1L).build();
        final GroupAndMembers groupAndMembers1 = createGroup(grouping, VC_VM.getOid());
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ENTITY_VM1, VC_VM));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        null).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.ONPREM).getObjects().size());
        Assert.assertEquals(1,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.HYBRID).getObjects().size());
        Assert.assertEquals(0,
                groupMapper.toGroupApiDto(Collections.singletonList(groupAndMembers1), false, null,
                        EnvironmentType.CLOUD).getObjects().size());
        Mockito.verify(repositoryApi, Mockito.times(3)).entitiesRequest(Mockito.any());
    }

    /**
     * Tests how the total size is calculated when filtering is applied.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testTotalSizeWhenFiltered() throws Exception {
        targets.add(VC_TARGET);
        final Grouping grouping1 = Grouping.newBuilder().setId(1L).build();
        final Grouping grouping2 = Grouping.newBuilder().setId(2L).build();
        final Grouping grouping3 = Grouping.newBuilder().setId(3L).build();
        final Grouping grouping4 = Grouping.newBuilder().setId(4L).build();
        final GroupAndMembers groupAndMembers1 = createGroup(grouping1, VC_VM.getOid());
        final GroupAndMembers groupAndMembers2 = createGroup(grouping2, VC_VM.getOid());
        final GroupAndMembers groupAndMembers3 = createGroup(grouping3, ENTITY_VM1.getOid());
        final GroupAndMembers groupAndMembers4 = createGroup(grouping4, VC_VM.getOid(), ENTITY_VM1.getOid());
        targets.add(VC_TARGET);
        targets.add(AWS_TARGET);
        final List<GroupAndMembers> requestedGroups =
                Arrays.asList(groupAndMembers1, groupAndMembers2, groupAndMembers3,
                        groupAndMembers4);
        final MultiEntityRequest req1 =
                ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ENTITY_VM1, VC_VM));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req1);
        final SearchPaginationRequest request = new SearchPaginationRequest("0", 1, true, null);
        final ObjectsPage<GroupApiDTO> response1 =
                groupMapper.toGroupApiDto(requestedGroups, false, request, null);
        Assert.assertEquals(4, response1.getTotalCount());
        Assert.assertEquals(1, response1.getObjects().size());

        final ObjectsPage<GroupApiDTO> response2 =
                groupMapper.toGroupApiDto(requestedGroups, false, request, EnvironmentType.ONPREM);
        Assert.assertEquals(3, response2.getTotalCount());
        Assert.assertEquals(1, response2.getObjects().size());

        final ObjectsPage<GroupApiDTO> response3 =
                groupMapper.toGroupApiDto(requestedGroups, false, request, EnvironmentType.HYBRID);
        Assert.assertEquals(4, response3.getTotalCount());
        Assert.assertEquals(1, response3.getObjects().size());

        final ObjectsPage<GroupApiDTO> response4 =
                groupMapper.toGroupApiDto(requestedGroups, false, request, EnvironmentType.CLOUD);
        Assert.assertEquals(2, response4.getTotalCount());
        Assert.assertEquals(1, response4.getObjects().size());
    }

    @Test
    public void testFilteringGroupInvalidEntities() throws Exception {
        final Grouping group = Grouping.newBuilder().setId(8L)
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()))
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                        .setDisplayName("foo").setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder().setEntity(10)).addMembers(1L).addMembers(2L))))
                .build();

        final MinimalEntity ent = MinimalEntity.newBuilder()
                .setOid(1L)
                .build();

        when(groupExpander.getMembersForGroup(group)).thenReturn(ImmutableGroupAndMembers.builder()
                .group(group).members(ImmutableSet.of(1L, 2L))
                .entities(ImmutableSet.of(1L, 2L)).build());
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(ent));
        when(repositoryApi.entitiesRequest(anySet())).thenReturn(req);

        final GroupApiDTO mappedDto =
                groupMapper.groupsToGroupApiDto(Collections.singletonList(group), false)
                        .values()
                        .iterator()
                        .next();

        assertThat(mappedDto.getUuid(), is("8"));
        assertThat(mappedDto.getIsStatic(), is(true));
        assertThat(mappedDto.getEntitiesCount(), is(1));
        assertThat(mappedDto.getMembersCount(), is(1));
        assertThat(mappedDto.getMemberUuidList(), containsInAnyOrder("1"));
        assertThat(mappedDto.getGroupType(), is(VM_TYPE));
        assertThat(mappedDto.getEnvironmentType(), is(EnvironmentType.ONPREM));
        assertThat(mappedDto.getClassName(), is("Group"));
    }

    private GroupAndMembers createGroup(@Nonnull Grouping grouping, Long... members) {
        return ImmutableGroupAndMembers.builder()
                .group(grouping)
                .members(Arrays.asList(members))
                .entities(Arrays.asList(members))
                .build();
    }
}
