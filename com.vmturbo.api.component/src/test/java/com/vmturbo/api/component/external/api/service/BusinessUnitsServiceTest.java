package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
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
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test services for the {@link BusinessUnitsService}
 */
public class BusinessUnitsServiceTest {


    public static final String UUID_STRING = "123";
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

    private ITargetsService targetsService = Mockito.mock(TargetsService.class);

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private static final long CONTEXT_ID = 777777L;

    private CostServiceBlockingStub costService;

    private IBusinessUnitsService businessUnitsService;


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
            targetsService,
            CONTEXT_ID,
            uuidMapper,
            entitiesService,
            supplyChainFetcherFactory,
            repositoryApi,
            accountRetriever);
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

    @Test
    public void testGetBusinessUnitsWithDiscoveredType() throws Exception {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(UUID_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        when(accountRetriever.getBusinessAccountsInScope(any())).thenReturn(ImmutableList.of(apiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList = businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTOList.get(0).getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTOList.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOList.get(0).getBusinessUnitType());
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
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(mapper.toBusinessUnitApiDTO(any())).thenReturn(apiDTO);
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.createBusinessUnit(businessUnitApiInputDTO);
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
    }

    private BusinessUnitApiDTO getBusinessUnitApiDTO() {
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setDisplayName(TEST_DISPLAY_NAME);
        apiDTO.setUuid(UUID_STRING);
        apiDTO.setBusinessUnitType(BusinessUnitType.DISCOUNT);
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
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
        when(mapper.toBusinessUnitApiDTO(any())).thenReturn(apiDTO);
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.editBusinessUnit(UUID_STRING, businessUnitApiInputDTO);
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTO.getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEditBusinessUnitWithoutUUIDWithException() throws Exception {
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitsService.editBusinessUnit(null, businessUnitApiInputDTO);
    }

    @Test
    public void testEditBusinessUnitWithoutUUID() throws Exception {
        BusinessUnitApiInputDTO businessUnitApiInputDTO = getBusinessUnitApiInputDTO();
        businessUnitApiInputDTO.setTargets(ImmutableList.of(UUID_STRING));
        BusinessUnitApiDTO apiDTO = getBusinessUnitApiDTO();
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
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                        .build());
        when(repositoryApi.entityRequest(OID_LONG)).thenReturn(mockReq);

        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                        .setOid(OID_LONG)
                        .setDisplayName("foo")
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
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
     * Testing get entities by business unit.
     * @throws Exception If one of the necessary steps/RPCs failed.
     */
    @Test
    public void testGetEntities() throws Exception {
        final ApiId apiId = ApiTestUtils.mockGroupId(UUID_STRING, uuidMapper);
        when(uuidMapper.fromUuid(UUID_STRING)).thenReturn(apiId);

        final SingleEntityRequest mockReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
                        .setOid(OID_LONG)
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                        .build());
        when(repositoryApi.entityRequest(OID_LONG)).thenReturn(mockReq);

        ConnectedEntity connectedEntity = ConnectedEntity.newBuilder().setConnectedEntityId(OID_LONG).build();
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                        .setOid(OID_LONG)
                        .setDisplayName("foo")
                        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
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


}
