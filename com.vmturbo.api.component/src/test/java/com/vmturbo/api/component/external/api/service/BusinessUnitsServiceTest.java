package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper;
import com.vmturbo.api.component.external.api.service.BusinessUnitsService.MissingDiscountException;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test services for the {@link BusinessUnitsService}
 */
public class BusinessUnitsServiceTest {


    public static final String UUID_STRING = "123";
    public static final String TEST_DISPLAY_NAME = "testDisplayName";
    public static final String CHILD_UNIT_ID1 = "123";
    public static final String SERVICE_DISCOUNT_UUID = "3";
    public static final String TIER_DISCOUNT_UUID = "4";
    public static final float TIER_DISCOUNT = 11f;
    public static final float SERVICE_DISCOUNT = 22f;
    private final BusinessUnitMapper mapper = Mockito.mock(BusinessUnitMapper.class);
    private final ITargetsService targetsService = Mockito.mock(TargetsService.class);
    private IBusinessUnitsService businessUnitsService;

    private CostServiceMole costServiceMole = spy(new CostServiceMole());


    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    private CostServiceBlockingStub costService;

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
        businessUnitsService = new BusinessUnitsService(costService, mapper, targetsService);
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
        when(mapper.getAndConvertDiscoveredBusinessUnits(any())).thenReturn(ImmutableList.of(apiDTO));
        List<BusinessUnitApiDTO> businessUnitApiDTOList = businessUnitsService.getBusinessUnits(BusinessUnitType.DISCOVERED, null, null, null);
        assertEquals(1, businessUnitApiDTOList.size());
        assertEquals(TEST_DISPLAY_NAME, businessUnitApiDTOList.get(0).getDisplayName());
        assertEquals(UUID_STRING, businessUnitApiDTOList.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOList.get(0).getBusinessUnitType());
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
}
