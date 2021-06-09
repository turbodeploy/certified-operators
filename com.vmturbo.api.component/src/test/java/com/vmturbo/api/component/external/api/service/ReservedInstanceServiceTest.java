package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.AccountFilterType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceType;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse.EntitiesCoveredByReservedInstance;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceBoughtServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceSpecServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for {@link ReservedInstancesService}.
 */
public class ReservedInstanceServiceTest {

    private final ReservedInstanceUtilizationCoverageServiceMole
            reservedInstanceUtilizationCoverageService = Mockito.spy(
            new ReservedInstanceUtilizationCoverageServiceMole());
    private final ReservedInstanceBoughtServiceMole reservedInstanceBoughtService = Mockito.spy(
            new ReservedInstanceBoughtServiceMole());
    private final ReservedInstanceSpecServiceMole reservedInstanceSpecService = Mockito.spy(
            new ReservedInstanceSpecServiceMole());
    private final GroupDTOMoles.GroupServiceMole groupService = Mockito.spy(new GroupDTOMoles.GroupServiceMole());

    /**
     * Grpc test server.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(
            reservedInstanceUtilizationCoverageService, reservedInstanceBoughtService,
            reservedInstanceSpecService, groupService);

    private ReservedInstancesService reservedInstancesService;
    private RepositoryApi repositoryApi;
    private UuidMapper uuidMapper;
    private GroupExpander groupExpander;
    private CloudTypeMapper cloudTypeMapper;

    /**
     * Setup.
     */
    @Before
    public void setUp() {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        uuidMapper = Mockito.mock(UuidMapper.class);
        groupExpander = Mockito.mock(GroupExpander.class);
        cloudTypeMapper = Mockito.mock(CloudTypeMapper.class);
        final ReservedInstanceMapper reservedInstanceMapper = new ReservedInstanceMapper(
                cloudTypeMapper);
        reservedInstancesService = new ReservedInstancesService(
                ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                ReservedInstanceSpecServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(
                        grpcServer.getChannel()), reservedInstanceMapper, repositoryApi,
                groupExpander, Mockito.mock(StatsQueryExecutor.class), uuidMapper,
                        GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Test for {@link ReservedInstancesService#getReservedInstances}.
     *
     * @throws Exception when failed
     */
    @Test
    public void testGetReservedInstances() throws Exception {
        final ApiId apiId = Mockito.mock(ApiId.class);
        Mockito.when(apiId.uuid()).thenReturn("Market");
        Mockito.when(apiId.oid()).thenReturn(123L);
        Mockito.when(apiId.isCloudPlan()).thenReturn(true);
        Mockito.when(apiId.getScopeOids()).thenReturn(Collections.singleton(1L));
        Mockito.when(uuidMapper.fromUuid("Market")).thenReturn(apiId);
        Mockito.when(groupExpander.getGroup("123")).thenReturn(
                Optional.of(Grouping.newBuilder().build()));
        Mockito.when(reservedInstanceBoughtService.getReservedInstanceBoughtForScope(
                GetReservedInstanceBoughtForScopeRequest.newBuilder().addScopeSeedOids(1).build()))
                .thenReturn(GetReservedInstanceBoughtForScopeResponse.newBuilder()
                        .addReservedInstanceBought(ReservedInstanceBought.newBuilder().setId(1))
                        .build());
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(
                GetReservedInstanceSpecByIdsRequest.newBuilder()
                        .addReservedInstanceSpecIds(0)
                        .build())).thenReturn(GetReservedInstanceSpecByIdsResponse.newBuilder()
                .addReservedInstanceSpec(ReservedInstanceSpec.newBuilder()
                        .setId(0)
                        .setReservedInstanceSpecInfo(
                                ReservedInstanceSpecInfo.newBuilder().setRegionId(100)))
                .build());

        Mockito.when(reservedInstanceUtilizationCoverageService.getReservedInstanceCoveredEntities(
                GetReservedInstanceCoveredEntitiesRequest.newBuilder()
                        .addReservedInstanceId(1)
                        .build())).thenReturn(
                GetReservedInstanceCoveredEntitiesResponse.newBuilder()
                        .putEntitiesCoveredByReservedInstances(1,
                                EntitiesCoveredByReservedInstance.newBuilder()
                                        .addCoveredEntityId(2)
                                        .addCoveredEntityId(3)
                                        .addCoveredEntityId(4)
                                        .addCoveredEntityId(5)
                                        .addCoveredUndiscoveredAccountId(6)
                                        .addCoveredUndiscoveredAccountId(7)
                                        .build())
                        .build());
        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        final ServiceEntityApiDTO region = new ServiceEntityApiDTO();
        final ServiceEntityApiDTO businessAccount = new ServiceEntityApiDTO();
        businessAccount.setUuid("200");
        businessAccount.setClassName(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        region.setClassName(ApiEntityType.REGION.apiStr());
        final Map<Long, ServiceEntityApiDTO> seMap = new HashMap<>();
        seMap.put(100L, region);
        seMap.put(200L, businessAccount);
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("Azure");
        region.setDiscoveredBy(targetApiDTO);
        Mockito.when(req.getSEMap()).thenReturn(seMap);
        Mockito.when(cloudTypeMapper.fromTargetType("Azure")).thenReturn(
                Optional.of(CloudType.AZURE));
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, 100L))).thenReturn(req);
        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder().build());
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(200L))).thenReturn(baRequest);
        final List<TopologyDTO.TopologyEntityDTO> baResponse = new ArrayList<>();
        final List<ReservedInstanceApiDTO> reservedInstanceApiDTOs =
                reservedInstancesService.getReservedInstances("Market", true, AccountFilterType.PURCHASED_BY);

        Assert.assertEquals(1, reservedInstanceApiDTOs.size());
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceApiDTOs.iterator().next();
        Assert.assertEquals(4, (int)reservedInstanceApiDTO.getCoveredEntityCount());
        Assert.assertEquals(2, (int)reservedInstanceApiDTO.getUndiscoveredAccountsCoveredCount());
    }

    /**
     * Test that RI request is rejected if the user is scoped.
     *
     * @throws Exception never
     */
    @Test
    public void testScopedUserGetReservedInstances() throws Exception {
        // Since the SecurityContextHolder is global in the JVM, we need to save the existing
        // auth and restore it after the test.
        final Authentication existingScopedUser =
                SecurityContextHolder.getContext().getAuthentication();
        final Authentication scopedUser = new UsernamePasswordAuthenticationToken(
            new AuthUserDTO(null, "user", "password", "10.10.10.10", "11111", "token",
                ImmutableList.of("ADVISOR"), Collections.singletonList(12345L)),
            "admin000",
            Collections.emptySet());

        SecurityContextHolder.getContext().setAuthentication(scopedUser);

        Assert.assertTrue(reservedInstancesService.getReservedInstances("12345", null, null).isEmpty());
        Mockito.verifyZeroInteractions(uuidMapper, repositoryApi);
        // Restore existing auth
        SecurityContextHolder.getContext().setAuthentication(existingScopedUser);
    }

    /**
     * Test for {@link ReservedInstancesService#getEntitiesCoveredByReservedInstance}.
     */
    @Test
    public void testGetEntitiesCoveredByReservedInstance() {
        Mockito.when(reservedInstanceUtilizationCoverageService.getReservedInstanceCoveredEntities(
                GetReservedInstanceCoveredEntitiesRequest.newBuilder()
                        .addReservedInstanceId(123)
                        .build())).thenReturn(
                GetReservedInstanceCoveredEntitiesResponse.newBuilder()
                        .putEntitiesCoveredByReservedInstances(123,
                                EntitiesCoveredByReservedInstance.newBuilder()
                                        .addCoveredEntityId(1)
                                        .addCoveredEntityId(2)
                                        .build())
                        .build());
        final MinimalEntity virtualMachine1 = MinimalEntity.newBuilder().setOid(1).setEntityType(
                EntityType.VIRTUAL_MACHINE_VALUE).setDisplayName("Virtual Machine 1").build();
        final MinimalEntity virtualMachine2 = MinimalEntity.newBuilder().setOid(2).setEntityType(
                EntityType.VIRTUAL_MACHINE_VALUE).setDisplayName("Virtual Machine 2").build();
        final MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiMinEntityReq(
                Arrays.asList(virtualMachine1, virtualMachine2));
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(1L, 2L))).thenReturn(
                multiEntityRequest);
        final List<BaseApiDTO> entitiesCoveredByReservedInstance =
                reservedInstancesService.getEntitiesCoveredByReservedInstance("123");
        Assert.assertEquals(2, entitiesCoveredByReservedInstance.size());
        final Map<String, BaseApiDTO> entities = entitiesCoveredByReservedInstance.stream().collect(
                Collectors.toMap(BaseApiDTO::getUuid, e -> e));
        Assert.assertTrue(
                EqualsBuilder.reflectionEquals(ServiceEntityMapper.toBaseApiDTO(virtualMachine1),
                        entities.get("1")));
        Assert.assertTrue(
                EqualsBuilder.reflectionEquals(ServiceEntityMapper.toBaseApiDTO(virtualMachine2),
                        entities.get("2")));
    }

    /**
     * Test that when reserved instances are fetched and the scope is an availability zone,
     * the reserved instance from that availability zone's parent region are fetched as well.
     *
     * @throws Exception never
     */
    @Test
    public void testGetRIsForZoneIncludesParentRegionRIs() throws Exception {
        final long scopeZone = 12345;
        final ApiId mockScope = ApiTestUtils.mockEntityId("12345", ApiEntityType.AVAILABILITY_ZONE,
            EnvironmentType.CLOUD, uuidMapper);
        Mockito.when(mockScope.getScopeTypes()).thenReturn(
            Optional.of(Collections.singleton(ApiEntityType.AVAILABILITY_ZONE)));
        Mockito.when(mockScope.getScopeEntitiesByType()).thenReturn(Collections.singletonMap(
            ApiEntityType.AVAILABILITY_ZONE, Collections.singleton(scopeZone)));
        final long parentRegion = 98765;
        final SearchRequest regionSearchRequest =
            ApiTestUtils.mockSearchIdReq(Collections.singleton(parentRegion));

        Mockito.when(repositoryApi.getRegion(Collections.singleton(scopeZone)))
            .thenReturn(regionSearchRequest);

        reservedInstancesService.getReservedInstances(Long.toString(scopeZone), null, null);

        Mockito.verify(reservedInstanceBoughtService).getReservedInstanceBoughtByFilter(
            GetReservedInstanceBoughtByFilterRequest.newBuilder()
                .setRegionFilter(RegionFilter.newBuilder().addRegionId(parentRegion))
                .setZoneFilter(AvailabilityZoneFilter.newBuilder().addAvailabilityZoneId(scopeZone))
                .build()
        );
    }

    private ReservedInstanceBought createRIBought(long businessAccountOid, long riSpecOid, long riOid) {
        ReservedInstanceBought.ReservedInstanceBoughtInfo riBoughtInfo = ReservedInstanceBought.ReservedInstanceBoughtInfo
                        .newBuilder().setBusinessAccountId(businessAccountOid).setReservedInstanceSpec(riSpecOid)
                        .build();
        ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder().setId(riOid)
                        .setReservedInstanceBoughtInfo(riBoughtInfo).build();
        return riBought;
    }

    private ReservedInstanceSpec createRISpec(long regionOid, long riSpecOid) {
        ReservedInstanceSpecInfo riSpecInfo = ReservedInstanceSpecInfo.newBuilder()
                        .setRegionId(regionOid).build();
        ReservedInstanceSpec riSpec = ReservedInstanceSpec
                        .newBuilder()
                        .setId(riSpecOid)
                        .setReservedInstanceSpecInfo(riSpecInfo).build();
        return riSpec;
    }

    private ServiceEntityApiDTO createRegion(long regionOid, TargetApiDTO targetApiDTO) {
        final ServiceEntityApiDTO region = new ServiceEntityApiDTO();
        region.setClassName(ApiEntityType.REGION.apiStr());
        region.setDiscoveredBy(targetApiDTO);
        region.setUuid(Long.toString(regionOid));
        return region;
    }

    private ServiceEntityApiDTO createBusinessAccount(long baOid) {
        final ServiceEntityApiDTO businessAccount = new ServiceEntityApiDTO();
        businessAccount.setUuid(Long.toString(baOid));
        businessAccount.setClassName(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        return businessAccount;
    }

    private Map<Long, ServiceEntityApiDTO> createSEMap(List<ServiceEntityApiDTO> regionList, List<ServiceEntityApiDTO> businessAccountList) {
        final Map<Long, ServiceEntityApiDTO> seMap = new HashMap<>();
        for (ServiceEntityApiDTO region : regionList) {
            seMap.put(Long.parseLong(region.getUuid()), region);
        }
        for (ServiceEntityApiDTO businessAccount : businessAccountList) {
            seMap.put(Long.parseLong(businessAccount.getUuid()), businessAccount);
        }
        return seMap;
    }

    private FilterApiDTO createFilterApiDTO(String scopeOid, String filterType) {
        FilterApiDTO filterApiDTO = new FilterApiDTO();
        filterApiDTO.setExpVal(scopeOid);
        filterApiDTO.setFilterType(filterType);
        return filterApiDTO;
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: Region.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesRegionScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        Mockito.when(reservedInstanceBoughtService
                        .getReservedInstanceBoughtByFilter(GetReservedInstanceBoughtByFilterRequest
                                        .newBuilder().setRegionFilter(RegionFilter.newBuilder()
                                                        .addRegionId(regionOid).build()).build()))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        FilterApiDTO regionScopeFilterDTO = createFilterApiDTO(Long.toString(regionOid), "RIByRegion");

        final List<ReservedInstanceApiDTO> reservedInstancesRegionScope = reservedInstancesService
                        .getReservedInstances(Collections.singletonList(regionScopeFilterDTO));
        Assert.assertEquals(1, reservedInstancesRegionScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesRegionScope.get(0).getUuid());
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: Account.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesBusinessAccountScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        Mockito.when(reservedInstanceBoughtService.getReservedInstanceBoughtByFilter(GetReservedInstanceBoughtByFilterRequest
                        .newBuilder().setAccountFilter(
                                        AccountReferenceFilter.newBuilder().addAccountId(baOid).setAccountFilterType(
                                                        AccountReferenceType.USED_AND_PURCHASED_BY).build()).build()))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        FilterApiDTO businessAccountScopeFilterDTO = createFilterApiDTO(Long.toString(baOid), "RIByAccountAll");

        final List<ReservedInstanceApiDTO> reservedInstancesBAScope = reservedInstancesService
                        .getReservedInstances(Collections.singletonList(businessAccountScopeFilterDTO));
        Assert.assertEquals(1, reservedInstancesBAScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesBAScope.get(0).getUuid());
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: Billing Family.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesBillingFamilyScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;
        final long bfOid = 1;

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        GroupDTO.GroupDefinition groupDefinition = GroupDTO.GroupDefinition.newBuilder().setType(CommonDTO.GroupDTO.GroupType.BILLING_FAMILY).build();
        Grouping grouping = Grouping.newBuilder().setDefinition(groupDefinition).addExpectedTypes(
                GroupDTO.MemberType.newBuilder().setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .build()).build();
        Mockito.when(groupExpander.getGroup(Long.toString(bfOid))).thenReturn(Optional.of(grouping));

        final ApiId apiId = Mockito.mock(ApiId.class);
        Mockito.when(apiId.getScopeOids()).thenReturn(Collections.singleton(baOid));
        Mockito.when(uuidMapper.fromOid(bfOid)).thenReturn(apiId);

        Mockito.when(reservedInstanceBoughtService.getReservedInstanceBoughtByFilter(GetReservedInstanceBoughtByFilterRequest
                        .newBuilder().setAccountFilter(
                                        AccountReferenceFilter.newBuilder().addAccountId(baOid).setAccountFilterType(
                                                AccountReferenceType.USED_AND_PURCHASED_BY).build()).build()))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        final FilterApiDTO billingFamilyScopeFilterDTO =
                        createFilterApiDTO(Long.toString(bfOid), "RIByBillingFamilyAll");
        final List<ReservedInstanceApiDTO> reservedInstancesBFScope = reservedInstancesService
                        .getReservedInstances(Collections.singletonList(billingFamilyScopeFilterDTO));
        Assert.assertEquals(1, reservedInstancesBFScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesBFScope.get(0).getUuid());
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: Cloud Service Provider.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesCloudServiceProviderScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;
        final long cloudServiceProviderOid = 10;

        Mockito.when(repositoryApi.expandServiceProvidersToRegions(ImmutableSet.of(cloudServiceProviderOid)))
                        .thenReturn(ImmutableSet.of(regionOid));

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        Mockito.when(reservedInstanceBoughtService
                        .getReservedInstanceBoughtByFilter(GetReservedInstanceBoughtByFilterRequest
                                        .newBuilder().setRegionFilter(RegionFilter.newBuilder()
                                                        .addRegionId(regionOid).build()).build()))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        final FilterApiDTO riByCloudProviderScopeFilterDTO =
                        createFilterApiDTO(Long.toString(cloudServiceProviderOid),
                                        "RIByCloudProvider");
        final List<ReservedInstanceApiDTO> reservedInstancesCSPScope = reservedInstancesService
                        .getReservedInstances(Collections.singletonList(riByCloudProviderScopeFilterDTO));
        Assert.assertEquals(1, reservedInstancesCSPScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesCSPScope.get(0).getUuid());
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: VM Group.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesVMGroupScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;
        final long bfOid = 1;
        final long vmOid1 = 20;
        final long vmOid2 = 21;
        final long groupOid = 30;

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        GroupDTO.StaticMembers.StaticMembersByType staticMembersByType = GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                        .setType(GroupDTO.MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build()).addMembers(vmOid1).addMembers(vmOid2).build();
        GroupDTO.StaticMembers staticGroupMembers =
                        GroupDTO.StaticMembers.newBuilder().addMembersByType(staticMembersByType).build();
        GroupDTO.GroupDefinition groupDefinition = GroupDTO.GroupDefinition.newBuilder().setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                        .setStaticGroupMembers(staticGroupMembers).build();
        Grouping grouping = Grouping.newBuilder().setDefinition(groupDefinition).addExpectedTypes(
                        GroupDTO.MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build()).build();
        Mockito.when(groupExpander.getGroup(Long.toString(groupOid))).thenReturn(Optional.of(grouping));

        SearchRequest businessAccountSearchRequest = Mockito.mock(SearchRequest.class);
        Mockito.when(repositoryApi.getOwningBusinessAccount(ImmutableSet.of(vmOid1, vmOid2)))
                        .thenReturn(businessAccountSearchRequest);
        TopologyDTO.PartialEntity.ApiPartialEntity baPartialEntity = TopologyDTO.PartialEntity.ApiPartialEntity
                        .newBuilder().setOid(baOid).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(businessAccountSearchRequest.getEntities())
                        .thenReturn(Stream.<TopologyDTO.PartialEntity.ApiPartialEntity>builder().add(baPartialEntity).build());

        GroupDTO.GetGroupsForEntitiesRequest getGroupsForEntitiesRequest =
                        GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
                                        .addAllEntityId(Collections.singleton(baOid))
                                        .addGroupType(CommonDTO.GroupDTO.GroupType.BILLING_FAMILY)
                                        .setLoadGroupObjects(true).build();

        GroupDTO.StaticMembers.StaticMembersByType staticMembersByTypeBA =
                        GroupDTO.StaticMembers.StaticMembersByType.newBuilder().setType(
                                        GroupDTO.MemberType.newBuilder()
                                                        .setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                                                        .build()).addMembers(baOid).build();
        GroupDTO.StaticMembers staticGroupMembersBA = GroupDTO.StaticMembers.newBuilder()
                        .addMembersByType(staticMembersByTypeBA).build();
        GroupDTO.GroupDefinition baGroupDefinition = GroupDTO.GroupDefinition.newBuilder()
                        .setStaticGroupMembers(staticGroupMembersBA).build();
        Grouping billingFamilyGrouping = Grouping.newBuilder().addExpectedTypes(
                        GroupDTO.MemberType.newBuilder().setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                                        .build()).setId(bfOid).setDefinition(baGroupDefinition).build();
        GroupDTO.GetGroupsForEntitiesResponse getGroupsForEntitiesResponse =
                        GroupDTO.GetGroupsForEntitiesResponse.newBuilder().addGroups(billingFamilyGrouping)
                                        .build();

        Mockito.when(groupService.getGroupsForEntities(getGroupsForEntitiesRequest)).thenReturn(getGroupsForEntitiesResponse);

        SearchRequest regionSearchRequest = Mockito.mock(SearchRequest.class);
        Mockito.when(repositoryApi.getRegion(ImmutableSet.of(vmOid1, vmOid2))).thenReturn(regionSearchRequest);
        TopologyDTO.PartialEntity.ApiPartialEntity regionPartialEntity = TopologyDTO.PartialEntity.ApiPartialEntity
                        .newBuilder().setOid(regionOid).setEntityType(EntityType.REGION_VALUE).build();
        Mockito.when(regionSearchRequest.getEntities())
                        .thenReturn(Stream.<TopologyDTO.PartialEntity.ApiPartialEntity>builder().add(regionPartialEntity).build());

        final GetReservedInstanceBoughtByFilterRequest riBoughtByFilterGroupScopeRequest =
                        GetReservedInstanceBoughtByFilterRequest.newBuilder().setRegionFilter(
                                        RegionFilter.newBuilder().addRegionId(regionOid).build())
                                        .setAccountFilter(AccountReferenceFilter.newBuilder()
                                                        .setAccountFilterType(
                                                                AccountReferenceType.USED_AND_PURCHASED_BY)
                                                        .addAccountId(baOid).build()).build();
        Mockito.when(reservedInstanceBoughtService.getReservedInstanceBoughtByFilter(riBoughtByFilterGroupScopeRequest))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        final FilterApiDTO riByGroupScopeFilterAPIDTO =
                        createFilterApiDTO(Long.toString(groupOid), "RIByGroup");
        final List<ReservedInstanceApiDTO> reservedInstancesGroupScope = reservedInstancesService
                        .getReservedInstances(
                                        Collections.singletonList(riByGroupScopeFilterAPIDTO));
        Assert.assertEquals(1, reservedInstancesGroupScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesGroupScope.get(0).getUuid());
    }

    /**
     * Test getReservedInstances POST method. Given a list of FilterApiDTO with a scope and filter
     * type, fetch a list of reserved instances. The following scopes are tested: Resource Group.
     *
     * @throws Exception Generic Exception thrown.
     */
    @Test
    public void testGetReservedInstancesResourceScope() throws Exception {
        final long regionOid = 100;
        final long baOid = 1000;
        final long riOid = 2000;
        final long riSpecOid = 3000;
        final long bfOid = 1;
        final long vmOid1 = 20;
        final long vmOid2 = 21;
        final long groupOid = 30;

        final ReservedInstanceBought riBought = createRIBought(baOid, riSpecOid, riOid);
        Cost.GetReservedInstanceBoughtByFilterResponse getReservedInstanceBoughtByFilterResponse =
                        Cost.GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                        .addReservedInstanceBoughts(riBought).build();

        GroupDTO.StaticMembers.StaticMembersByType staticMembersByType = GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                        .setType(GroupDTO.MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build()).addMembers(vmOid1).addMembers(vmOid2).build();
        GroupDTO.StaticMembers staticGroupMembers =
                        GroupDTO.StaticMembers.newBuilder().addMembersByType(staticMembersByType).build();
        GroupDTO.GroupDefinition groupDefinition = GroupDTO.GroupDefinition.newBuilder().setType(CommonDTO.GroupDTO.GroupType.RESOURCE)
                        .setStaticGroupMembers(staticGroupMembers).build();
        Grouping grouping = Grouping.newBuilder().setDefinition(groupDefinition).addExpectedTypes(
                        GroupDTO.MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build()).build();
        Mockito.when(groupExpander.getGroup(Long.toString(groupOid))).thenReturn(Optional.of(grouping));

        SearchRequest businessAccountSearchRequest = Mockito.mock(SearchRequest.class);
        Mockito.when(repositoryApi.getOwningBusinessAccount(ImmutableSet.of(vmOid1, vmOid2)))
                        .thenReturn(businessAccountSearchRequest);
        TopologyDTO.PartialEntity.ApiPartialEntity baPartialEntity = TopologyDTO.PartialEntity.ApiPartialEntity
                        .newBuilder().setOid(baOid).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(businessAccountSearchRequest.getEntities())
                        .thenReturn(Stream.<TopologyDTO.PartialEntity.ApiPartialEntity>builder().add(baPartialEntity).build());

        GroupDTO.GetGroupsForEntitiesRequest getGroupsForEntitiesRequest =
                        GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
                                        .addAllEntityId(Collections.singleton(baOid))
                                        .addGroupType(CommonDTO.GroupDTO.GroupType.BILLING_FAMILY)
                                        .setLoadGroupObjects(true).build();

        GroupDTO.StaticMembers.StaticMembersByType staticMembersByTypeBA =
                        GroupDTO.StaticMembers.StaticMembersByType.newBuilder().setType(
                                        GroupDTO.MemberType.newBuilder()
                                                        .setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                                                        .build()).addMembers(baOid).build();
        GroupDTO.StaticMembers staticGroupMembersBA = GroupDTO.StaticMembers.newBuilder()
                        .addMembersByType(staticMembersByTypeBA).build();
        GroupDTO.GroupDefinition baGroupDefinition = GroupDTO.GroupDefinition.newBuilder()
                        .setStaticGroupMembers(staticGroupMembersBA).build();
        Grouping billingFamilyGrouping = Grouping.newBuilder().addExpectedTypes(
                        GroupDTO.MemberType.newBuilder().setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                                        .build()).setId(bfOid).setDefinition(baGroupDefinition).build();
        GroupDTO.GetGroupsForEntitiesResponse getGroupsForEntitiesResponse =
                        GroupDTO.GetGroupsForEntitiesResponse.newBuilder().addGroups(billingFamilyGrouping)
                                        .build();

        Mockito.when(groupService.getGroupsForEntities(getGroupsForEntitiesRequest)).thenReturn(getGroupsForEntitiesResponse);

        SearchRequest regionSearchRequest = Mockito.mock(SearchRequest.class);
        Mockito.when(repositoryApi.getRegion(ImmutableSet.of(vmOid1, vmOid2))).thenReturn(regionSearchRequest);
        TopologyDTO.PartialEntity.ApiPartialEntity regionPartialEntity = TopologyDTO.PartialEntity.ApiPartialEntity
                        .newBuilder().setOid(regionOid).setEntityType(EntityType.REGION_VALUE).build();
        Mockito.when(regionSearchRequest.getEntities())
                        .thenReturn(Stream.<TopologyDTO.PartialEntity.ApiPartialEntity>builder().add(regionPartialEntity).build());

        final GetReservedInstanceBoughtByFilterRequest riBoughtByFilterGroupScopeRequest =
                        GetReservedInstanceBoughtByFilterRequest.newBuilder().setRegionFilter(
                                        RegionFilter.newBuilder().addRegionId(regionOid).build())
                                        .setAccountFilter(AccountReferenceFilter.newBuilder()
                                                        .setAccountFilterType(
                                                                AccountReferenceType.USED_AND_PURCHASED_BY)
                                                        .addAccountId(baOid).build()).build();
        Mockito.when(reservedInstanceBoughtService.getReservedInstanceBoughtByFilter(riBoughtByFilterGroupScopeRequest))
                        .thenReturn(getReservedInstanceBoughtByFilterResponse);

        final ReservedInstanceSpec riSpec = createRISpec(regionOid, riSpecOid);
        GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec)
                                        .build();
        Mockito.when(reservedInstanceSpecService.getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest
                        .newBuilder().addReservedInstanceSpecIds(riSpecOid).build()))
                        .thenReturn(getReservedInstanceSpecByIdsResponse);

        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("AWS");

        final ServiceEntityApiDTO region = createRegion(regionOid, targetApiDTO);
        final ServiceEntityApiDTO businessAccount = createBusinessAccount(baOid);

        final Map<Long, ServiceEntityApiDTO> seMap = createSEMap(Collections.singletonList(region),
                        Collections.singletonList(businessAccount));

        final MultiEntityRequest req = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, regionOid, baOid)))
                        .thenReturn(req);
        Mockito.when(req.getSEMap()).thenReturn(seMap);

        final MultiEntityRequest baRequest = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(baOid))).thenReturn(baRequest);

        TopologyDTO.TopologyEntityDTO baEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(baOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        Mockito.when(baRequest.getFullEntities()).thenReturn(Stream.<TopologyDTO.TopologyEntityDTO>builder()
                        .add(baEntityDTO).build());

        Mockito.when(cloudTypeMapper.fromTargetType("AWS")).thenReturn(
                        Optional.of(CloudType.AWS));

        final FilterApiDTO riByGroupScopeFilterAPIDTO =
                        createFilterApiDTO(Long.toString(groupOid), "RIByResourceGroup");
        final List<ReservedInstanceApiDTO> reservedInstancesGroupScope = reservedInstancesService
                        .getReservedInstances(
                                        Collections.singletonList(riByGroupScopeFilterAPIDTO));
        Assert.assertEquals(1, reservedInstancesGroupScope.size());
        Assert.assertEquals(Long.toString(riOid), reservedInstancesGroupScope.get(0).getUuid());
    }
}
