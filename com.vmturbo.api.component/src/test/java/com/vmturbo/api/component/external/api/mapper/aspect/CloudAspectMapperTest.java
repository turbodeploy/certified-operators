package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.GraphResponse;
import com.vmturbo.common.protobuf.search.Search.OidList;
import com.vmturbo.common.protobuf.search.Search.ResponseNode;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;

/**
 * Unit tests for {@link CloudAspectMapper}.
 */
public class CloudAspectMapperTest {

    private static final long VIRTUAL_MACHINE_OID = 70000000000000L;
    private static final long COMPUTE_TIER_OID = 70000000000001L;
    private static final String TEMPLATE_NAME = "t3a.medium";
    private static final long ZONE_OID = 70000000000002L;
    private static final String ZONE_NAME = "aws-us-west-2c";
    private static final long REGION_OID = 70000000000003L;
    private static final long BUSINESS_ACCOUNT_OID = 70000000000004L;
    private static final String REGION_NAME = "aws-US West (Oregon)";
    private static final String BUSINESS_ACCOUT_NAME = "BusinessAccountName";
    private static final String ARCHITECTURE = "64-bit";
    private static final String VIRTUALIZATION_TYPE = "HVM";
    private static final String NVME = "Installed";
    private static final int VM_COUPON_CAPACITY = 10;
    private static final double DELTA = 0.001;
    private static final long RESOURCE_GROUP_ID = 1L;
    private static final long VIRTUAL_MACHINE_NO_RG_OID = 2L;
    private static final long VIRTUAL_MACHINE_NULL_RG_RESPONSE_OID = 3L;
    private static final String RESOURCE_GROUP_DISPLAYNAME = "aResourceGroup";

    private ReservedInstanceUtilizationCoverageServiceMole riCoverageServiceMole =
            spy(new ReservedInstanceUtilizationCoverageServiceMole());
    private GroupDTOMoles.GroupServiceMole groupServiceMole =
            spy(GroupDTOMoles.GroupServiceMole.class);
    private GrpcTestServer testServer = GrpcTestServer.newServer(riCoverageServiceMole, groupServiceMole);
    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private final ExecutorService executorService =  Executors.newCachedThreadPool(new ThreadFactoryBuilder().build());

    private CloudAspectMapper cloudAspectMapper;
    private TopologyEntityDTO.Builder topologyEntityBuilder;
    private ApiPartialEntity.Builder apiPartialEntityBuilder;


    /**
     * Objects initialization necessary for a unit test.
     *
     * @throws IOException if testServer::start throws IOException.
     */
    @Before
    public void setUp() throws IOException {
        testServer.start();
        final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService =
                ReservedInstanceUtilizationCoverageServiceGrpc
                        .newBlockingStub(testServer.getChannel());
        final GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(
                testServer.getChannel());
        cloudAspectMapper = new CloudAspectMapper(repositoryApi, riCoverageService, groupServiceBlockingStub, executorService);
        topologyEntityBuilder = TopologyEntityDTO.newBuilder()
                .setOid(VIRTUAL_MACHINE_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setArchitecture(Architecture.BIT_64)
                                .setVirtualizationType(VirtualizationType.HVM)
                                .setDriverInfo(DriverInfo.newBuilder()
                                        .setHasEnaDriver(true)
                                        .setHasNvmeDriver(true))
                                .setBillingType(VMBillingType.RESERVED)))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setProviderId(COMPUTE_TIER_OID));
        apiPartialEntityBuilder = ApiPartialEntity.newBuilder()
                .setOid(VIRTUAL_MACHINE_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD);
        MinimalEntity computeTier = MinimalEntity.newBuilder()
                .setOid(COMPUTE_TIER_OID)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setDisplayName(TEMPLATE_NAME)
                .build();
        final MinimalEntity region = MinimalEntity.newBuilder()
                .setOid(REGION_OID)
                .setEntityType(EntityType.REGION_VALUE)
                .setDisplayName(REGION_NAME)
                .build();
        final MinimalEntity zone = MinimalEntity.newBuilder()
                .setOid(ZONE_OID)
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setDisplayName(ZONE_NAME)
                .build();
        final MinimalEntity businessAccount = MinimalEntity.newBuilder()
                .setOid(BUSINESS_ACCOUNT_OID)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setDisplayName(BUSINESS_ACCOUT_NAME)
                .build();
        final MultiEntityRequest template =
                ApiTestUtils.mockMultiMinEntityReq(Collections.singletonList(computeTier));
        final MultiEntityRequest templateAndRegion =
                ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(computeTier, region));
        final MultiEntityRequest templateAndZone =
                ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(computeTier, zone));
        final SearchRequest regionSearchRequest =
                ApiTestUtils.mockSearchMinReq(Collections.singletonList(region));
        final SearchRequest businessAccountSearchRequest =
                ApiTestUtils.mockSearchMinReq(Collections.singletonList(businessAccount));
        final Grouping grouping1 = Grouping.newBuilder()
                .setId(RESOURCE_GROUP_ID)
                .setDefinition(GroupDTO.GroupDefinition.newBuilder()
                    .setType(CommonDTO.GroupDTO.GroupType.RESOURCE)
                    .setDisplayName(RESOURCE_GROUP_DISPLAYNAME))
                .build();
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID, ZONE_OID)))
                .thenReturn(templateAndZone);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID)))
                .thenReturn(template);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID, REGION_OID)))
                .thenReturn(templateAndRegion);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(ZONE_OID, TraversalDirection.CONNECTED_FROM,
                        ApiEntityType.REGION))).thenReturn(regionSearchRequest);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(VIRTUAL_MACHINE_OID,
                        TraversalDirection.CONNECTED_FROM, ApiEntityType.BUSINESS_ACCOUNT)))
                .thenReturn(businessAccountSearchRequest);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(VIRTUAL_MACHINE_NO_RG_OID,
                        TraversalDirection.CONNECTED_FROM, ApiEntityType.BUSINESS_ACCOUNT)))
                .thenReturn(businessAccountSearchRequest);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(VIRTUAL_MACHINE_NULL_RG_RESPONSE_OID,
                        TraversalDirection.CONNECTED_FROM, ApiEntityType.BUSINESS_ACCOUNT)))
                .thenReturn(businessAccountSearchRequest);

        GroupDTO.GetGroupsForEntitiesRequest.Builder request = GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
                .addGroupType(CommonDTO.GroupDTO.GroupType.RESOURCE)
                .setLoadGroupObjects(true);
        when(groupServiceMole.getGroupsForEntities(request.addEntityId(VIRTUAL_MACHINE_OID).build()))
            .thenReturn(GroupDTO.GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(VIRTUAL_MACHINE_OID, GroupDTO.Groupings.newBuilder().addGroupId(grouping1.getId())
                        .build())
                .addGroups(grouping1)
                .build());
        when(groupServiceMole.getGroupsForEntities(request.addEntityId(VIRTUAL_MACHINE_NO_RG_OID).build()))
                .thenReturn(GroupDTO.GetGroupsForEntitiesResponse.newBuilder()
                        .putEntityGroup(VIRTUAL_MACHINE_NO_RG_OID, GroupDTO.Groupings.newBuilder().addGroupId(grouping1.getId())
                                .build())
                        .build());
        when(groupServiceMole.getGroupsForEntities(request.addEntityId(VIRTUAL_MACHINE_NULL_RG_RESPONSE_OID).build()))
                .thenReturn(null);

        long accountOid = 123L;
        Mockito.when(repositoryApi.graphSearch(any())).thenReturn(GraphResponse.newBuilder()
                .putNodes("account", ResponseNode.newBuilder().putOidMap(VIRTUAL_MACHINE_OID, OidList.newBuilder().addOids(accountOid).build())
                        .putEntities(accountOid, PartialEntity.newBuilder().setMinimal(businessAccount).build()).build())
                .putNodes("region", ResponseNode.newBuilder().putOidMap(VIRTUAL_MACHINE_OID, OidList.newBuilder().addOids(REGION_OID).build())
                        .putEntities(REGION_OID, PartialEntity.newBuilder().setMinimal(region).build()).build())
                .putNodes("regionByZone", ResponseNode.newBuilder().build())
                .putNodes("zone", ResponseNode.newBuilder().putOidMap(VIRTUAL_MACHINE_OID, OidList.newBuilder().addOids(ZONE_OID).build())
                        .putEntities(ZONE_OID, PartialEntity.newBuilder().setMinimal(zone).build()).build()).build());
    }

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     */
    @Test
    public void testMapEntityToAspectTopologyEntity() {

        // Act
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                topologyEntityBuilder.build());
        // Assert
        Assert.assertEquals(ARCHITECTURE, aspect.getArchitecture());
        Assert.assertEquals(VirtualMachineProtoUtil.ENA_IS_ACTIVE, aspect.getEnaActive());
        Assert.assertEquals(NVME, aspect.getNvme());
        Assert.assertEquals(VIRTUALIZATION_TYPE, aspect.getVirtualizationType());
        Assert.assertEquals(String.valueOf(COMPUTE_TIER_OID), aspect.getTemplate().getUuid());
        Assert.assertEquals(TEMPLATE_NAME, aspect.getTemplate().getDisplayName());
        Assert.assertEquals(ApiEntityType.COMPUTE_TIER.apiStr(),
                aspect.getTemplate().getClassName());
        Assert.assertEquals(ApiEntityType.BUSINESS_ACCOUNT.apiStr(),
                aspect.getBusinessAccount().getClassName());
        Assert.assertEquals(String.valueOf(BUSINESS_ACCOUNT_OID),
                aspect.getBusinessAccount().getUuid());
        Assert.assertEquals(BUSINESS_ACCOUT_NAME, aspect.getBusinessAccount().getDisplayName());
        Assert.assertEquals(VMBillingType.RESERVED.name(), aspect.getBillingType());
    }

    /**
     * Test that partially covered VM has a Hybrid BillingType.
     */
    @Test
    public void testRiCoverageInfoPartiallyReserved() {
        // given
        final float couponsCovered = 3;
        stubRiCoverage(couponsCovered);

        // when
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                        topologyEntityBuilder.build());

        // then
        Assert.assertNotNull(aspect);
        Assert.assertEquals(couponsCovered * 100 / VM_COUPON_CAPACITY,
                aspect.getRiCoveragePercentage(), DELTA);
        Assert.assertEquals(couponsCovered, aspect.getRiCoverage().getValue(), DELTA);
        Assert.assertEquals(VMBillingType.HYBRID.name(), aspect.getBillingType());
    }

    /**
     * Test that a fully covered VM has a Reserved BillingType.
     */
    @Test
    public void testRiCoverageInfoFullyReserved() {
        // given
        final float couponsCovered = 9.999f;
        stubRiCoverage(couponsCovered);

        // when
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                        topologyEntityBuilder.build());

        // then
        Assert.assertNotNull(aspect);
        Assert.assertEquals(couponsCovered * 100 / VM_COUPON_CAPACITY,
                aspect.getRiCoveragePercentage(), DELTA);
        Assert.assertEquals(couponsCovered, aspect.getRiCoverage().getValue(), DELTA);
        Assert.assertEquals(VMBillingType.RESERVED.name(), aspect.getBillingType());
    }

    /**
     * Test that an uncovered VM has an On demand BillingType.
     */
    @Test
    public void testRiCoverageInfoOnDemand() {
        // given
        final float couponsCovered = 0;
        stubRiCoverage(couponsCovered);

        // when
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                        topologyEntityBuilder.build());
        // then
        Assert.assertNotNull(aspect);
        Assert.assertEquals(VMBillingType.ONDEMAND.name(), aspect.getBillingType());
    }

    /**
     * Test that for VM with billing type Bidding, the RI coverage information is not considered.
     */
    @Test
    public void testBiddingInstanceBillingType() {
        // given
        topologyEntityBuilder.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                .setBillingType(VMBillingType.BIDDING);
        // when
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                topologyEntityBuilder.build());
        // then
        Assert.assertNotNull(aspect);
        Assert.assertEquals(VMBillingType.BIDDING.name(), aspect.getBillingType());
    }

    private void stubRiCoverage(float couponsCovered) {
        when(riCoverageServiceMole.getEntityReservedInstanceCoverage(any())).thenReturn(
                GetEntityReservedInstanceCoverageResponse.newBuilder()
                        .putCoverageByEntityId(VIRTUAL_MACHINE_OID,
                                EntityReservedInstanceCoverage.newBuilder()
                                        .setEntityCouponCapacity(VM_COUPON_CAPACITY)
                                        .putCouponsCoveredByRi(1111L, couponsCovered)
                                        .build())
                        .build());
    }

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     * AWS case - when both availability zone and region are present.
     */
    @Test
    public void testAwsCaseAvailabilityZoneAndRegion() {
        // Arrange
        final TopologyEntityDTO entity = topologyEntityBuilder.addConnectedEntityList(
                ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_OID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)).build();
        // Act
        final CloudAspectApiDTO aspect =
                (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(entity);
        // Assert
        Assert.assertEquals(String.valueOf(REGION_OID), aspect.getRegion().getUuid());
        Assert.assertEquals(REGION_NAME, aspect.getRegion().getDisplayName());
        Assert.assertEquals(ApiEntityType.REGION.apiStr(), aspect.getRegion().getClassName());
        Assert.assertEquals(String.valueOf(ZONE_OID), aspect.getZone().getUuid());
        Assert.assertEquals(ZONE_NAME, aspect.getZone().getDisplayName());
        Assert.assertEquals(ApiEntityType.AVAILABILITY_ZONE.apiStr(),
                aspect.getZone().getClassName());
    }

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     * Azure case - when only region are present.
     */
    @Test
    public void testAzureCaseRegion() {
        // Arrange
        final TopologyEntityDTO entity = topologyEntityBuilder.addConnectedEntityList(
                ConnectedEntity.newBuilder()
                        .setConnectedEntityId(REGION_OID)
                        .setConnectedEntityType(EntityType.REGION_VALUE)).build();
        // Act
        final CloudAspectApiDTO aspect =
                (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(entity);
        // Assert
        Assert.assertEquals(String.valueOf(REGION_OID), aspect.getRegion().getUuid());
        Assert.assertEquals(REGION_NAME, aspect.getRegion().getDisplayName());
        Assert.assertEquals(ApiEntityType.REGION.apiStr(), aspect.getRegion().getClassName());
    }

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     * Azure case - when an associated resource group is present
     */
    @Test
    public void testAzureResourceGroup() {
        // when
        final CloudAspectApiDTO aspect = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                topologyEntityBuilder.build());

        // then
        Assert.assertNotNull(aspect);
        com.vmturbo.api.dto.BaseApiDTO resourceGroup = aspect.getResourceGroup();
        Assert.assertNotNull(resourceGroup);
        Assert.assertEquals(Long.valueOf(RESOURCE_GROUP_ID), Long.valueOf(resourceGroup.getUuid()));
        Assert.assertEquals(RESOURCE_GROUP_DISPLAYNAME, resourceGroup.getDisplayName());
        Assert.assertEquals("ResourceGroup", resourceGroup.getClassName());

        // when
        final CloudAspectApiDTO aspectNoRG = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
            TopologyEntityDTO.newBuilder()
                .setOid(VIRTUAL_MACHINE_NO_RG_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setProviderId(COMPUTE_TIER_OID))
                .build());
        // then
        Assert.assertNotNull(aspectNoRG);
        Assert.assertNull(aspectNoRG.getResourceGroup());

        // when
        final CloudAspectApiDTO aspectNullRG = (CloudAspectApiDTO)cloudAspectMapper.mapEntityToAspect(
                TopologyEntityDTO.newBuilder()
                    .setOid(VIRTUAL_MACHINE_NULL_RG_RESPONSE_OID)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                            .setProviderId(COMPUTE_TIER_OID))
                    .build());
        // then
        Assert.assertNotNull(aspectNullRG);
        Assert.assertNull(aspectNullRG.getResourceGroup());
    }

    /**
     * Clean up test resources.
     */
    @After
    public void cleanUp() {
        testServer.close();
    }
}
