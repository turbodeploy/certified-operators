package com.vmturbo.api.component.external.api.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.AccountFilterType;
import com.vmturbo.api.enums.CloudType;
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
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
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

    /**
     * Grpc test server.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(
            reservedInstanceUtilizationCoverageService, reservedInstanceBoughtService,
            reservedInstanceSpecService);

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
                groupExpander, Mockito.mock(StatsQueryExecutor.class), uuidMapper);
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
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("Azure");
        region.setDiscoveredBy(targetApiDTO);
        Mockito.when(req.getSEMap()).thenReturn(Collections.singletonMap(100L, region));
        Mockito.when(cloudTypeMapper.fromTargetType("Azure")).thenReturn(
                Optional.of(CloudType.AZURE));
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(0L, 100L))).thenReturn(req);

        final List<ReservedInstanceApiDTO> reservedInstanceApiDTOs =
                reservedInstancesService.getReservedInstances("Market", true, AccountFilterType.PURCHASED_BY);

        Assert.assertEquals(1, reservedInstanceApiDTOs.size());
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceApiDTOs.iterator().next();
        Assert.assertEquals(4, (int)reservedInstanceApiDTO.getCoveredEntityCount());
        Assert.assertEquals(2, (int)reservedInstanceApiDTO.getUndiscoveredAccountsCoveredCount());
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
}
