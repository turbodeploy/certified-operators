package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Arrays;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;

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
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
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
import com.vmturbo.common.protobuf.topology.UIEntityType;
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

    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
    private CloudAspectMapper cloudAspectMapper;
    private TopologyEntityDTO.Builder topologyEntityBuilder;
    private ApiPartialEntity.Builder apiPartialEntityBuilder;

    /**
     * Objects initialization necessary for a unit test.
     */
    @Before
    public void setUp() {
        cloudAspectMapper = new CloudAspectMapper(repositoryApi);
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
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID, ZONE_OID)))
                .thenReturn(templateAndZone);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID)))
                .thenReturn(template);
        Mockito.when(repositoryApi.entitiesRequest(ImmutableSet.of(COMPUTE_TIER_OID, REGION_OID)))
                .thenReturn(templateAndRegion);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(ZONE_OID, TraversalDirection.CONNECTED_FROM,
                        UIEntityType.REGION))).thenReturn(regionSearchRequest);
        Mockito.when(repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(VIRTUAL_MACHINE_OID,
                        TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT)))
                .thenReturn(businessAccountSearchRequest);
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
        Assert.assertEquals(UIEntityType.COMPUTE_TIER.apiStr(),
                aspect.getTemplate().getClassName());
        Assert.assertEquals(UIEntityType.BUSINESS_ACCOUNT.apiStr(),
                aspect.getBusinessAccount().getClassName());
        Assert.assertEquals(String.valueOf(BUSINESS_ACCOUNT_OID),
                aspect.getBusinessAccount().getUuid());
        Assert.assertEquals(BUSINESS_ACCOUT_NAME, aspect.getBusinessAccount().getDisplayName());
        Assert.assertEquals(VMBillingType.RESERVED.name(), aspect.getBillingType());
    }

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity)}
     * method.
     *
     * @throws OperationFailedException in case of API mapper failure.
     */
    @Test
    public void testMapEntityToAspectApiPartialEntity() {
        final TopologyEntityDTO topologyEntityDTO = topologyEntityBuilder.build();
        final SingleEntityRequest topologyEntity =
                ApiTestUtils.mockSingleEntityRequest(topologyEntityDTO);
        Mockito.when(repositoryApi.entityRequest(VIRTUAL_MACHINE_OID)).thenReturn(topologyEntity);
        final EntityAspect entityAspect =
                cloudAspectMapper.mapEntityToAspect(apiPartialEntityBuilder.build());
        Assert.assertNotNull(entityAspect);
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
        Assert.assertEquals(UIEntityType.REGION.apiStr(), aspect.getRegion().getClassName());
        Assert.assertEquals(String.valueOf(ZONE_OID), aspect.getZone().getUuid());
        Assert.assertEquals(ZONE_NAME, aspect.getZone().getDisplayName());
        Assert.assertEquals(UIEntityType.AVAILABILITY_ZONE.apiStr(),
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
        Assert.assertEquals(UIEntityType.REGION.apiStr(), aspect.getRegion().getClassName());
        Assert.assertNull(aspect.getZone());
    }
}
