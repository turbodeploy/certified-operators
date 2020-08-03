package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.DiscountMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.BusinessUnitsService.MissingDiscountException;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Test services for the {@link BusinessUnitsService}
 */
public class BusinessUnitsServiceTest {


    public static final String UUID_STRING = "123";
    private static final long UUID = 123;
    private static final String CHILD_ACCOUNT_1_STRING = "324";
    private static final String CHILD_ACCOUNT_2_STRING = "3241";
    private static final long CHILD_ACCOUNT_1 = 324;
    private static final long CHILD_ACCOUNT_2 = 3241;
    public static final long OID_LONG= 123L;
    public static final String TEST_DISPLAY_NAME = "testDisplayName";
    public static final String CHILD_UNIT_ID1 = "123";
    public static final String SERVICE_DISCOUNT_UUID = "3";
    public static final String TIER_DISCOUNT_UUID = "4";
    public static final float TIER_DISCOUNT = 11f;
    public static final float SERVICE_DISCOUNT = 22f;

    private final DiscountMapper mapper = Mockito.mock(DiscountMapper.class);

    private final BusinessAccountRetriever accountRetriever = Mockito.mock(BusinessAccountRetriever.class);

    private final UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private CostServiceMole costServiceMole = spy(new CostServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    private EntitiesService entitiesService = Mockito.mock(EntitiesService.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);

    private ThinTargetCache thinTargetCache = Mockito.mock(ThinTargetCache.class);

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private static final long CONTEXT_ID = 777777L;

    private CostServiceBlockingStub costService;

    private IBusinessUnitsService businessUnitsService;

    private final CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();


    private static CloudServicePriceAdjustmentApiDTO populateServiceDto() {
        final CloudServicePriceAdjustmentApiDTO cloudServiceDiscountApiDTO = new CloudServicePriceAdjustmentApiDTO();
        cloudServiceDiscountApiDTO.setUuid(SERVICE_DISCOUNT_UUID);
        cloudServiceDiscountApiDTO.setDisplayName("AWS RDS");
        cloudServiceDiscountApiDTO.setPricingModel(ServicePricingModel.ON_DEMAND);
        cloudServiceDiscountApiDTO.setDiscount(SERVICE_DISCOUNT);
        TemplatePriceAdjustmentDTO templateDiscountDTO = new TemplatePriceAdjustmentDTO();
        templateDiscountDTO.setFamily("c5d");
        templateDiscountDTO.setUuid(TIER_DISCOUNT_UUID);
        templateDiscountDTO.setDisplayName("c5d.xlarge");
        templateDiscountDTO.setDiscount(TIER_DISCOUNT);
        EntityPriceDTO entityPriceDTO = new EntityPriceDTO();
        entityPriceDTO.setPrice(30f);
        entityPriceDTO.setUuid("aws::eu-west-1::DC::eu-west-1");
        entityPriceDTO.setDisplayName("aws-EU (Ireland)");
        templateDiscountDTO.setPricesPerDatacenter(ImmutableList.of(entityPriceDTO));

        cloudServiceDiscountApiDTO.setTemplatePriceAdjustments(ImmutableList.of(templateDiscountDTO));
        return cloudServiceDiscountApiDTO;
    }

    @Before
    public void setup() throws Exception {
        costService = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        businessUnitsService = new BusinessUnitsService(
            costService,
            mapper,
            thinTargetCache,
            CONTEXT_ID,
            uuidMapper,
            entitiesService,
            supplyChainFetcherFactory,
            repositoryApi,
            accountRetriever,
            cloudTypeMapper);
    }

    @Test
    public void testGetBusinessUnitsWithDiscountType() throws Exception {
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(mapper.toDiscountBusinessUnitApiDTO(any())).thenReturn(ImmutableList.of(apiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList = businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOUNT, null, null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTOList.get(0).getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTOList.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTOList.get(0).getBusinessUnitType());
    }

    /**
     * Test the case that there are some discounts for child accounts and should be filtered.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetBusinessUnitsWithDiscountTypeFilterChildren() throws Exception {
        //ARRANGE
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        BusinessUnitApiDTO childAccountApiDTO = getChildAccount1BusinessUnitApiDTO();
        Cost.Discount masterAccountDiscount = Cost.Discount.newBuilder()
            .setAssociatedAccountId(UUID)
            .setDiscountInfo(Cost.DiscountInfo.newBuilder().build())
            .build();
        Cost.Discount childAccountDiscount = Cost.Discount.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_1)
            .setDiscountInfo(Cost.DiscountInfo.newBuilder().build())
            .build();
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .build())).thenReturn(ImmutableList.of(masterAccountDiscount, childAccountDiscount));
        when(accountRetriever.getBusinessAccounts(ImmutableSet.of(UUID, CHILD_ACCOUNT_1))).thenReturn(
            ImmutableList.of(apiDTO, childAccountApiDTO));
        ArgumentCaptor<Iterator> captor =
            ArgumentCaptor.forClass(Iterator.class);
        when(mapper.toDiscountBusinessUnitApiDTO(captor.capture()))
            .thenReturn(ImmutableList.of(apiDTO));

        //ACT
        List<BusinessUnitApiDTO> businessUnitApiDTOList = businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOUNT, null, null, null);

        //ASSERT
        assertTrue(captor.getValue().hasNext());
        assertThat(captor.getValue().next(), is(masterAccountDiscount));
        assertFalse(captor.getValue().hasNext());
    }

    @Test
    public void testGetBusinessUnitsWithDiscoveredType() throws Exception {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(UUID_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        when(accountRetriever.getBusinessAccountsInScope(any(), any())).thenReturn(ImmutableList.of(apiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList = businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTOList.get(0).getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTOList.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOList.get(0).getBusinessUnitType());
    }

    /**
     * Test that the hasParent flag for getBusinessUnits works properly with discovered BUs.
     *
     * @throws Exception if getBusinessUnits throws an exception.
     */
    @Test
    public void testGetBusinessUnitsWithDiscoveredTypeAndParentFlag() throws Exception {
        final String parentUuid = "234";
        final String childUuid = "567";
        final BusinessUnitApiDTO parentApiDTO = new BusinessUnitApiDTO();
        parentApiDTO.setDisplayName(TEST_DISPLAY_NAME);
        parentApiDTO.setUuid(parentUuid);
        parentApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        parentApiDTO.setChildrenBusinessUnits(Collections.singleton(childUuid));
        final BusinessUnitApiDTO childApiDTO = new BusinessUnitApiDTO();
        childApiDTO.setDisplayName(TEST_DISPLAY_NAME);
        childApiDTO.setUuid(childUuid);
        childApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        when(accountRetriever.getBusinessAccountsInScope(any(), any()))
            .thenReturn(ImmutableList.of(parentApiDTO, childApiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, null, null);
        assertEquals(2, businessUnitApiDTOList.size());
        assertThat(businessUnitApiDTOList.stream().map(BusinessUnitApiDTO::getUuid).collect(Collectors.toList()),
            containsInAnyOrder(parentUuid, childUuid));
        // set hasParent = true.  Now we should only get the child unit.
        businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, true, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(childUuid, businessUnitApiDTOList.get(0).getUuid());
        // set hasParent = false.  Now we should only get the parent unit.
        businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, false, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(parentUuid, businessUnitApiDTOList.get(0).getUuid());
    }

    /**
     * Test that filtering by cloud type works properly for discovered BUs.
     *
     * @throws Exception if getBusinessUnits throws an exception.
     */
    @Test
    public void testGetBusinessUnitsWithDiscoveredTypeAndCloudType() throws Exception {
        final TargetApiDTO awsTarget = new TargetApiDTO();
        awsTarget.setUuid("11");
        awsTarget.setCategory(ProbeCategory.PUBLIC_CLOUD.getCategory());
        awsTarget.setType(SDKProbeType.AWS.getProbeType());
        final TargetApiDTO azureTarget = new TargetApiDTO();
        azureTarget.setUuid("22");
        azureTarget.setCategory(ProbeCategory.PUBLIC_CLOUD.getCategory());
        azureTarget.setType(SDKProbeType.AZURE.getProbeType());
        final ThinProbeInfo awsProbeInfo = Mockito.mock(ThinProbeInfo.class);
        final ThinProbeInfo azureProbeInfo = Mockito.mock(ThinProbeInfo.class);
        final ThinTargetInfo awsThinTargetInfo = Mockito.mock(ThinTargetInfo.class);
        final ThinTargetInfo azureThinTargetInfo = Mockito.mock(ThinTargetInfo.class);
        when(thinTargetCache.getTargetInfo(11L)).thenReturn(Optional.of(awsThinTargetInfo));
        when(thinTargetCache.getTargetInfo(22L))
            .thenReturn(Optional.of(azureThinTargetInfo));
        when(awsThinTargetInfo.isHidden()).thenReturn(false);
        when(awsThinTargetInfo.probeInfo()).thenReturn(awsProbeInfo);
        when(awsProbeInfo.type()).thenReturn(SDKProbeType.AWS.getProbeType());
        when(azureThinTargetInfo.isHidden()).thenReturn(false);
        when(azureThinTargetInfo.probeInfo()).thenReturn(azureProbeInfo);
        when(azureProbeInfo.type()).thenReturn(SDKProbeType.AZURE.getProbeType());
        final String awsBusinessUnitUuid = "111";
        final String azureBusinessUnitUuid = "222";
        final BusinessUnitApiDTO awsApiDTO = new BusinessUnitApiDTO();
        awsApiDTO.setDisplayName(TEST_DISPLAY_NAME);
        awsApiDTO.setUuid(awsBusinessUnitUuid);
        awsApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        awsApiDTO.setTargets(Collections.singletonList(awsTarget));
        final BusinessUnitApiDTO azureApiDTO = new BusinessUnitApiDTO();
        azureApiDTO.setDisplayName(TEST_DISPLAY_NAME);
        azureApiDTO.setUuid(azureBusinessUnitUuid);
        azureApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        azureApiDTO.setTargets(Collections.singletonList(azureTarget));
        when(accountRetriever.getBusinessAccountsInScope(any(), any()))
            .thenReturn(ImmutableList.of(awsApiDTO, azureApiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, null, null);
        assertEquals(2, businessUnitApiDTOList.size());
        assertThat(businessUnitApiDTOList.stream().map(BusinessUnitApiDTO::getUuid).collect(Collectors.toList()),
            containsInAnyOrder(awsBusinessUnitUuid, azureBusinessUnitUuid));
        // set hasParent = true.  Now we should only get the child unit.
        businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED,
                CloudType.AWS.name(), null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(awsBusinessUnitUuid, businessUnitApiDTOList.get(0).getUuid());
        // set hasParent = false.  Now we should only get the parent unit.
        businessUnitApiDTOList =
            businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED,
                CloudType.AZURE.name(), null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(azureBusinessUnitUuid, businessUnitApiDTOList.get(0).getUuid());
    }

    @Test
    public void testGetBusinessUnitByUuid() throws Exception {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(UUID_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);

        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);

        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.getBusinessUnitByUuid(UUID_STRING);

        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTO.getBusinessUnitType());
    }

    @Test
    public void testCreateBusinessUnit() throws Exception {
        //ARRANGE
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(mapper.toBusinessUnitApiDTO(any())).thenReturn(apiDTO);

        //ACT
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.createBusinessUnit(businessUnitApiInputDTO);

        //ASSERT
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
    }

    private BusinessUnitApiDTO getBusinessUnitApiDTO() {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(UUID_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOUNT);
        apiDTO.setChildrenBusinessUnits(ImmutableList.of(CHILD_ACCOUNT_1_STRING, CHILD_ACCOUNT_2_STRING));
        apiDTO.setMaster(true);
        return apiDTO;
    }

    private BusinessUnitApiDTO getChildAccount1BusinessUnitApiDTO() {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(CHILD_ACCOUNT_1_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOUNT);
        apiDTO.setChildrenBusinessUnits(Collections.emptyList());
        apiDTO.setMaster(false);
        return apiDTO;
    }

    private BusinessUnitApiInputDTO getBusinessUnitApiInputDTO() {
        BusinessUnitApiInputDTO businessUnitApiInputDTO = new BusinessUnitApiInputDTO();
        businessUnitApiInputDTO.setChildrenBusinessUnits(ImmutableSet.of(CHILD_UNIT_ID1));
        businessUnitApiInputDTO.setName(TEST_DISPLAY_NAME);
        businessUnitApiInputDTO.setDiscount(11f);
        return businessUnitApiInputDTO;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateBusinessUnitWithInvalidExeption() throws Exception {
        businessUnitsService.createBusinessUnit(new BusinessUnitApiInputDTO());
    }

    @Test
    public void testEditBusinessUnitWithUUID() throws Exception {
        // ARRANGE
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.singletonList(Cost.Discount.newBuilder().build()));
        when(mapper.toBusinessUnitApiDTO(any())).thenReturn(apiDTO);

        // ACT
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.editBusinessUnit(UUID_STRING,
            businessUnitApiInputDTO);

        //ASSERT
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEditBusinessUnitWithoutUUIDWithException() throws Exception {
        // ARRANGE
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        //ACT
        businessUnitsService.editBusinessUnit(null, businessUnitApiInputDTO);
    }

    @Test
    public void testEditBusinessUnitWithoutUUID() throws Exception {
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        businessUnitApiInputDTO.setTargets(ImmutableList.of(UUID_STRING));
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.singletonList(Cost.Discount.newBuilder().build()));
        when(mapper.toBusinessUnitApiDTO(any())).thenReturn(apiDTO);
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.editBusinessUnit(null, businessUnitApiInputDTO);
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
    }

    @Test(expected = MissingDiscountException.class)
    public void testGetBusinessUnitDiscounts() throws Exception {
        businessUnitsService.getBusinessUnitPriceAdjustments(UUID_STRING);
    }

    /**
     * Testing get count of actions stats by business unit.
     * @throws Exception If one of the necessary steps/RPCs failed.
     */
    @Test
    public void testGetActionCountStatsByUuid() throws Exception {
        final ApiId apiId = ApiTestUtils.mockGroupId(UUID_STRING, uuidMapper);
        when(uuidMapper.fromUuid(UUID_STRING)).thenReturn(apiId);

        final SingleEntityRequest mockReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
                        .setOid(OID_LONG)
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .build());
        when(repositoryApi.entityRequest(OID_LONG)).thenReturn(mockReq);

        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                        .setOid(OID_LONG)
                        .setDisplayName("foo")
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .build();
        when(mockReq.getFullEntity()).thenReturn(Optional.of(entityDTO));

        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        actionDTO.setEndTime(DateTimeUtil.getNow());
        actionDTO.setEnvironmentType(EnvironmentType.CLOUD);
        actionDTO.setRelatedEntityTypes(ImmutableList.of());
        actionDTO.setGroupBy(ImmutableList.of("riskSubCategory", "actionStates"));

        final List<StatSnapshotApiDTO> listStatDTO = new ArrayList<>();
        StatSnapshotApiDTO stat = new StatSnapshotApiDTO();
        stat.setDisplayName("foo");
        stat.setDate("2000-01-01");
        listStatDTO.add(stat);

        when(entitiesService.getActionCountStatsByUuid(UUID_STRING, actionDTO)).thenReturn(listStatDTO);

        List<StatSnapshotApiDTO> statsByUuid = businessUnitsService.getActionCountStatsByUuid(UUID_STRING, actionDTO);

        verify(entitiesService).getActionCountStatsByUuid(UUID_STRING, actionDTO);

        assertThat(statsByUuid, is(listStatDTO));
    }

    /**
     * Testing get count of actions stats by account uuid, for workload related entity type.
     * @throws Exception If any mandatory RPC call fails.
     */
    @Test
    public void testGetActionCountStatsByUuidForWorkloadType() throws Exception {
        final ApiId apiId = ApiTestUtils.mockGroupId(UUID_STRING, uuidMapper);
        when(uuidMapper.fromUuid(UUID_STRING)).thenReturn(apiId);

        final SingleEntityRequest mockReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
                        .setOid(OID_LONG)
                        .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                        .build());
        when(repositoryApi.entityRequest(OID_LONG)).thenReturn(mockReq);

        ConnectedEntity connectedEntity1 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(1)
                .setConnectedEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        ConnectedEntity connectedEntity2 = ConnectedEntity.newBuilder()
                .setConnectedEntityId(2)
                .setConnectedEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .build();
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                        .setOid(OID_LONG)
                        .setDisplayName("account")
                        .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                        .addConnectedEntityList(connectedEntity1)
                        .addConnectedEntityList(connectedEntity2)
                        .build();
        when(mockReq.getFullEntity()).thenReturn(Optional.of(entityDTO));

        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        actionDTO.setEndTime(DateTimeUtil.getNow());
        actionDTO.setEnvironmentType(EnvironmentType.CLOUD);
        actionDTO.setRelatedEntityTypes(Lists.newArrayList(StringConstants.WORKLOAD));
        actionDTO.setGroupBy(ImmutableList.of("riskSubCategory", "actionStates"));

        final List<StatSnapshotApiDTO> listStatDTO = new ArrayList<>();
        StatSnapshotApiDTO stat = new StatSnapshotApiDTO();
        stat.setDisplayName("vm-action-1");
        stat.setDate("2020-01-01");
        listStatDTO.add(stat);

        when(entitiesService.getActionCountStatsByUuid(UUID_STRING, actionDTO)).thenReturn(listStatDTO);

        List<StatSnapshotApiDTO> statsByUuid = businessUnitsService.getActionCountStatsByUuid(UUID_STRING, actionDTO);

        verify(entitiesService).getActionCountStatsByUuid(UUID_STRING, actionDTO);
        assertThat(statsByUuid, is(listStatDTO));
    }

    /**
     * Test that if certain related entity types were not set, then we use default values which
     * expand business account scope.
     *
     * @throws Exception if any mandatory RPC call fails.
     */
    @Test
    public void testDefaultRelatedEntityTypesToBusinessUnits() throws Exception {
        final ActionApiInputDTO initialActionDTO = new ActionApiInputDTO();
        initialActionDTO.setEndTime(DateTimeUtil.getNow());
        initialActionDTO.setEnvironmentType(EnvironmentType.CLOUD);

        final ActionApiInputDTO expandedActionDTO = initialActionDTO;
        expandedActionDTO.setRelatedEntityTypes(
                ApiEntityType.ENTITY_TYPES_TO_EXPAND.get(ApiEntityType.BUSINESS_ACCOUNT)
                        .stream()
                        .map(ApiEntityType::apiStr)
                        .collect(Collectors.toList()));

        businessUnitsService.getActionCountStatsByUuid(UUID_STRING, initialActionDTO);

        verify(entitiesService).getActionCountStatsByUuid(UUID_STRING, expandedActionDTO);
    }

    /**
     * Testing get entities by business unit.
     * @throws Exception If one of the necessary steps/RPCs failed.
     */
    @Test
    public void testGetEntities() throws Exception {
        final ApiId apiId = ApiTestUtils.mockGroupId(UUID_STRING, uuidMapper);
        when(uuidMapper.fromUuid(UUID_STRING)).thenReturn(apiId);

        final SingleEntityRequest mockReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
                        .setOid(OID_LONG)
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .build());
        when(repositoryApi.entityRequest(OID_LONG)).thenReturn(mockReq);

        ConnectedEntity connectedEntity = ConnectedEntity.newBuilder().setConnectedEntityId(OID_LONG).build();
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                        .setOid(OID_LONG)
                        .setDisplayName("foo")
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .addConnectedEntityList(connectedEntity)
                        .build();
        when(mockReq.getFullEntity()).thenReturn(Optional.of(entityDTO));

        final ServiceEntityApiDTO serviceEntityDTO = new ServiceEntityApiDTO();
        serviceEntityDTO.setUuid(UUID_STRING);
        serviceEntityDTO.setEnvironmentType(EnvironmentType.CLOUD);
        serviceEntityDTO.setDisplayName("VM");
        Map<Long, ServiceEntityApiDTO> entityMap = new HashMap<>();
        entityMap.put(OID_LONG, serviceEntityDTO);

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();

        when(repositoryApi.entitiesRequest(ImmutableSet.of(OID_LONG))).thenReturn(multiEntityRequest);
        when(multiEntityRequest.getSEMap()).then(invocation -> entityMap);

        EntityPaginationRequest paginationRequest = new EntityPaginationRequest(null, null, false, null);

        EntityPaginationResponse paginationResponse = businessUnitsService.getEntities(UUID_STRING, paginationRequest);

        assertThat(paginationResponse.getRestResponse().getBody().size(), is(1));
        assertThat(paginationResponse.getRestResponse().getBody().get(0), is(serviceEntityDTO));
    }

    /**
     * Testing getRelatedBusinessUnits by business unit.
     * @throws Exception If one of the necessary steps/RPCs failed.
     */
    @Test
    public void testGetRelatedBusinessUnits() throws Exception {
        BusinessUnitApiDTO baApiDTO = new BusinessUnitApiDTO();
        baApiDTO.setDisplayName(TEST_DISPLAY_NAME);
        baApiDTO.setUuid(UUID_STRING);
        baApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        baApiDTO.setChildrenBusinessUnits(ImmutableList.of(UUID_STRING));

        when(accountRetriever.getChildAccounts(UUID_STRING)).thenReturn(Collections.singletonList(baApiDTO));

        Collection<BusinessUnitApiDTO> resultDTOs =
            businessUnitsService.getRelatedBusinessUnits(UUID_STRING, HierarchicalRelationship.CHILDREN);

        assertThat(resultDTOs.size(), is(1));
        assertThat(resultDTOs.iterator().next(), is(baApiDTO));

        assertThat(resultDTOs.size(), is(1));
        assertThat(resultDTOs.iterator().next().getUuid(), is(UUID_STRING));
    }

    /**
     * Test a case of deleting a discount.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testDeleteBusinessUnit() throws Exception {
        //ARRANGE
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.singletonList(Cost.Discount.newBuilder().build()));
        Cost.DeleteDiscountResponse response =
            Cost.DeleteDiscountResponse.newBuilder().setDeleted(true).build();
        when(costServiceMole.deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(UUID)
            .build())).thenReturn(response);

        //ACT
        businessUnitsService.deleteBusinessUnit(UUID_STRING);

        //ASSERT
        verify(costServiceMole).deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_1)
            .build());
        verify(costServiceMole).deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_2)
            .build());
    }

    /**
     * Test a case of deleting a discount when a discount does not exist.
     * @throws Exception if something goes wrong.
     */
    @Test(expected = OperationFailedException.class)
    public void testDeleteBusinessUnitNoExistingDiscount() throws Exception {
        //ARRANGE
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.emptyList());

        //ACT
        businessUnitsService.deleteBusinessUnit(UUID_STRING);
    }

    /**
     * Test a case of deleting a discount when child accounts discount does not exist.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testDeleteBusinessUnitNoChildDiscount() throws Exception {
        //ARRANGE
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.singletonList(Cost.Discount.newBuilder().build()));
        Cost.DeleteDiscountResponse response =
            Cost.DeleteDiscountResponse.newBuilder().setDeleted(true).build();
        when(costServiceMole.deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(UUID)
            .build())).thenReturn(response);
        when(costServiceMole.deleteDiscountError(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_1)
            .build())).thenReturn(Optional.of(Status.NOT_FOUND.asRuntimeException()));

        //ACT
        businessUnitsService.deleteBusinessUnit(UUID_STRING);

        //ASSERT
        verify(costServiceMole).deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_2)
            .build());
    }

    /**
     * Test a case of deleting a discount an error while deleting one of the children. so we should
     * changes that we made.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testDeleteBusinessUnitInternalError() throws Exception {
        //ARRANGE
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(accountRetriever.getBusinessAccount(UUID_STRING)).thenReturn(apiDTO);
        Cost.Discount discount = Cost.Discount.newBuilder().build();
        when(costServiceMole.getDiscounts(Cost.GetDiscountRequest.newBuilder()
            .setFilter(Cost.DiscountQueryFilter.newBuilder().addAssociatedAccountId(UUID).build())
            .build())).thenReturn(Collections.singletonList(discount));
        Cost.DeleteDiscountResponse response =
            Cost.DeleteDiscountResponse.newBuilder().setDeleted(true).build();
        when(costServiceMole.deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(UUID)
            .build())).thenReturn(response);
        when(costServiceMole.deleteDiscountError(Cost.DeleteDiscountRequest.newBuilder()
            .setAssociatedAccountId(CHILD_ACCOUNT_1)
            .build())).thenReturn(Optional.of(Status.INTERNAL.asRuntimeException()));

        //ACT
        try {
            businessUnitsService.deleteBusinessUnit(UUID_STRING);
        } catch (StatusRuntimeException ex) {
            //ASSERT
            assertThat(ex.getStatus().getCode(), is(Status.Code.INTERNAL));
            verify(costServiceMole).createDiscount(Cost.CreateDiscountRequest.newBuilder()
                .setId(UUID)
                .setDiscountInfo(discount.getDiscountInfo())
                .build());
            verify(costServiceMole, times(0)).deleteDiscount(Cost.DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(CHILD_ACCOUNT_2)
                .build());
            return;
        }
        Assert.fail("The test should have thrown StatusRuntimeException");
    }

}
