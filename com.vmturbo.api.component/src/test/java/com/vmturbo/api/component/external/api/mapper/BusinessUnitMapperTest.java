package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper.MissingTopologyEntityException;
import com.vmturbo.api.component.external.api.service.SearchService;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Unit tests for the {@link BusinessUnitMapper}.
 */
public class BusinessUnitMapperTest {

    public static final String TARGET_UUID = "2";
    public static final String SERVICE_DISCOUNT_UUID = "3";
    public static final String TIER_DISCOUNT_UUID = "4";

    public static final long ENTITY_OID = 1l;
    public static final String AWS = "AWS";
    public static final String ENGINEERING_AWS_AMAZON_COM = "engineering.aws.amazon.com";
    public static final String CHILDREN_BUSINESS_UNITS = "3";
    public static final String TEST_DSICOUNT = "testDsicount";
    public static final float TIER_DISCOUNT = 11f;
    public static final float SERVICE_DISCOUNT = 22f;
    public static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    public static final double DISCOUNT_PERCENTAGE2 = 20.0;
    public static final double DISCOUNT_PERCENTAGE1 = 10.0;
    public static final String WORKLOAD = "Workload";
    public static final String BUSINESS_ACCOUNT = "BusinessAccount";
    private static final long id = 1234L;
    private static final long id2 = 1235L;
    final DiscountInfo discountInfo1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .setDisplayName(TEST_DSICOUNT)
            .build();
    private final RepositoryClient repositoryClient = Mockito.mock(RepositoryClient.class);
    private final TargetsService targetsService = Mockito.mock(TargetsService.class);
    private final Discount discount1 = Discount.newBuilder()
            .setId(id)
            .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
            .setDiscountInfo(discountInfo1)
            .build();
    private BusinessUnitMapper businessUnitMapper = new BusinessUnitMapper(111l);
    private ISearchService searchService = mock(SearchService.class);

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

        final RetrieveTopologyEntitiesResponse response = RetrieveTopologyEntitiesResponse.newBuilder()
                .addEntities(TopologyEntityDTO.newBuilder()
                        .setOid(ENTITY_OID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2).build()).build())
                        .build())
                .build();
        when(repositoryClient.retrieveTopologyEntities(anyList(), anyLong())).thenReturn(response);
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(TARGET_UUID);
        targetApiDTO.setType(AWS);
        targetApiDTO.setDisplayName(ENGINEERING_AWS_AMAZON_COM);
        BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setChildrenBusinessUnits(ImmutableSet.of(CHILDREN_BUSINESS_UNITS));
        targetApiDTO.setCurrentBusinessAccount(businessUnitApiDTO);
        when(targetsService.getTarget(anyString())).thenReturn(targetApiDTO);
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid(String.valueOf(ENTITY_OID));
        List<BaseApiDTO> results = ImmutableList.of(groupApiDTO);
        final SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(null, null, false, null);
        final SearchPaginationResponse searchPaginationResponse = searchPaginationRequest.allResultsResponse(results);
        when(searchService.getMembersBasedOnFilter(anyString(), any(), any())).thenReturn(searchPaginationResponse);
    }

    @Test
    public void testToBusinessUnitApiDTO() {
        BusinessUnitApiDTO businessUnitApiDTO = businessUnitMapper.toBusinessUnitApiDTO(discount1, repositoryClient, targetsService);
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
        assertEquals(false, businessUnitApiDTO.isMaster());
        assertEquals(CloudType.AWS, businessUnitApiDTO.getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTO.getMemberType());
        assertEquals(ASSOCIATED_ACCOUNT_ID, Long.parseLong(businessUnitApiDTO.getUuid()));
        assertEquals(BUSINESS_ACCOUNT, businessUnitApiDTO.getClassName());
        assertEquals(TEST_DSICOUNT, businessUnitApiDTO.getDisplayName());
    }

    @Test
    public void testToTierDiscountProto() {
        BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServicePriceAdjustments(ImmutableList.of(populateServiceDto()));
        TierLevelDiscount tierLevelDiscount = businessUnitMapper.toTierDiscountProto(businessUnitDiscountApiDTO);
        assertEquals(TIER_DISCOUNT, tierLevelDiscount.getDiscountPercentageByTierIdMap().get(Long.parseLong(TIER_DISCOUNT_UUID)), 0.001);
    }

    @Test
    public void testToServiceDiscountProto() {
        BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServicePriceAdjustments(ImmutableList.of(populateServiceDto()));
        ServiceLevelDiscount serviceLevelDiscount = businessUnitMapper.toServiceDiscountProto(businessUnitDiscountApiDTO);
        assertEquals(SERVICE_DISCOUNT, serviceLevelDiscount.getDiscountPercentageByServiceIdMap().get(Long.parseLong(SERVICE_DISCOUNT_UUID)), 0.001);
    }

    @Test
    public void testToDiscountApiDTO() throws Exception {
        BusinessUnitPriceAdjustmentApiDTO businessUnitApiDTO = businessUnitMapper.toDiscountApiDTO(discount1, repositoryClient, searchService);
        assertEquals(1, businessUnitApiDTO.getServicePriceAdjustments().size());
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTO.getServicePriceAdjustments().get(0).getUuid());
        assertEquals(ServicePricingModel.ON_DEMAND, businessUnitApiDTO.getServicePriceAdjustments().get(0).getPricingModel());
    }

    @Test
    public void testToDiscoveredBusinessUnitDTO() throws Exception {
        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.toDiscoveredBusinessUnitDTO(searchService, targetsService, repositoryClient);
        assertEquals(1, businessUnitApiDTOs.size());
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTOs.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOs.get(0).getBusinessUnitType());
        assertEquals(CloudType.AWS, businessUnitApiDTOs.get(0).getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTOs.get(0).getMemberType());
    }


    @Test(expected = MissingTopologyEntityException.class)
    public void testToDiscoveredBusinessUnitDTOWithException() throws Exception {
        businessUnitMapper.toDiscoveredBusinessUnitDTO(searchService, targetsService, Mockito.mock(RepositoryClient.class));
    }

}
