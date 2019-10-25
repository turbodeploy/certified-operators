package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper.MissingTopologyEntityException;
import com.vmturbo.api.component.external.api.service.SearchService;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
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
    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private final TargetsService targetsService = Mockito.mock(TargetsService.class);
    private final Discount discount1 = Discount.newBuilder()
        .setId(id)
        .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
        .setDiscountInfo(discountInfo1)
        .build();
    private BusinessUnitMapper businessUnitMapper = new BusinessUnitMapper(111l, repositoryApi);
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
//        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
//                .setOid(ENTITY_OID)
//                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
//                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
//                        DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2)))
//                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
//                        BusinessAccountInfo.newBuilder().setHasAssociatedTarget(true)))
//                .build();
//        Stream<TopologyEntityDTO> responseStream = Stream.of(TopologyEntityDTO.newBuilder()
//                .setOid(ENTITY_OID)
//                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
//                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
//                        DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2)))
//                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
//                        BusinessAccountInfo.newBuilder().setHasAssociatedTarget(true)))
//                .build());
//
//        when(repositoryClient.retrieveTopologyEntities(anyList(), anyLong()))
//                .thenAnswer(i -> Stream.of(entity));
//        final TargetApiDTO targetApiDTO = new TargetApiDTO();
//        targetApiDTO.setUuid(TARGET_UUID);
//        targetApiDTO.setType(AWS);
//        targetApiDTO.setDisplayName(ENGINEERING_AWS_AMAZON_COM);
//        BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
//        businessUnitApiDTO.setChildrenBusinessUnits(ImmutableSet.of(CHILDREN_BUSINESS_UNITS));
//        targetApiDTO.setCurrentBusinessAccount(businessUnitApiDTO);
//        when(targetsService.getTarget(anyString())).thenReturn(targetApiDTO);
//        GroupApiDTO groupApiDTO = new GroupApiDTO();
//        groupApiDTO.setUuid(String.valueOf(ENTITY_OID));
//        List<BaseApiDTO> results = ImmutableList.of(groupApiDTO);
//        final SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(null, null, false, null);
//        final SearchPaginationResponse searchPaginationResponse = searchPaginationRequest.allResultsResponse(results);
//        when(searchService.getMembersBasedOnFilter(anyString(), any(), any())).thenReturn(searchPaginationResponse);
    }

    @Test
    public void testToBusinessUnitApiDTO() {
        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ASSOCIATED_ACCOUNT_ID)).thenReturn(req);

        BusinessUnitApiDTO businessUnitApiDTO = businessUnitMapper.toBusinessUnitApiDTO(discount1);
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
        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);
        SearchRequest req = ApiTestUtils.mockSearchReq(Collections.singletonList(
            ApiPartialEntity.newBuilder()
                .setEntityType(UIEntityType.CLOUD_SERVICE.typeNumber())
                .setOid(1000L)
                .setDisplayName("Cloud Service")
                .build()
        ));
        when(repositoryApi.newSearchRequest(any())).thenReturn(req);

        BusinessUnitPriceAdjustmentApiDTO businessUnitApiDTO = businessUnitMapper.toDiscountApiDTO(discount1);
        assertEquals(1, businessUnitApiDTO.getServicePriceAdjustments().size());
        assertEquals(String.valueOf(1000), businessUnitApiDTO.getServicePriceAdjustments().get(0).getUuid());
        assertEquals(ServicePricingModel.ON_DEMAND, businessUnitApiDTO.getServicePriceAdjustments().get(0).getPricingModel());
    }

    @Test
    public void testToDiscoveredBusinessUnitDTO() throws Exception {
        final String accountId = "accountId";
        final String offerId = "offerId";
        final String enrollmentNum = "enrollment";
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_OID)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2)))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder()
                    .setAssociatedTargetId(id2)
                    .setAccountId(accountId)
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                        .setIdentifierValue(enrollmentNum).build())
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.OFFER_ID)
                        .setIdentifierValue(offerId).build())))
            .build();
        final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTOs.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOs.get(0).getBusinessUnitType());
        assertEquals(CloudType.AWS, businessUnitApiDTOs.get(0).getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTOs.get(0).getMemberType());
        assertEquals((Long)id2, businessUnitApiDTOs.get(0).getAssociatedTargetId());
        assertEquals(2, businessUnitApiDTOs.get(0).getPricingIdentifiers().size());
        assertEquals(offerId, businessUnitApiDTOs.get(0).getPricingIdentifiers()
            .get(PricingIdentifierName.OFFER_ID.name()));
        assertEquals(enrollmentNum, businessUnitApiDTOs.get(0).getPricingIdentifiers()
            .get(PricingIdentifierName.ENROLLMENT_NUMBER.name()));
        assertEquals(accountId, businessUnitApiDTOs.get(0).getAccountId());
        assertTrue(businessUnitApiDTOs.get(0).isHasRelatedTarget());
    }

    @Test
    public void testToDiscoveredBusinessUnitDTOWithBillingProbe() throws Exception {
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_OID)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2)))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder().setAssociatedTargetId(id2)))
            .build();

        final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(TARGET_UUID);
        targetApiDTO.setType("AWS Billing");
        targetApiDTO.setDisplayName("engineering.billing.aws.amazon.com");
        when(targetsService.getTarget(anyString())).thenReturn(targetApiDTO);
        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTOs.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOs.get(0).getBusinessUnitType());
        // still AWS instead of billing probe
        assertEquals(CloudType.AWS, businessUnitApiDTOs.get(0).getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTOs.get(0).getMemberType());
        assertTrue(businessUnitApiDTOs.get(0).getPricingIdentifiers().isEmpty());
    }

    /**
     * Test that the empty business account creates the correct BusinessUnitApiDTO.
     *
     * @throws Exception when businessUnitMapper.getAndConvertDiscoveredBusinessUnits throws one.
     */
    @Test
    public void testEmptyBusinessAccountToDiscoveredBusinessUnitDTO() throws Exception {
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_OID)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id2)))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder()))
            .build();
        final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTOs.get(0).getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTOs.get(0).getBusinessUnitType());
        assertEquals(CloudType.AWS, businessUnitApiDTOs.get(0).getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTOs.get(0).getMemberType());
        assertNull(businessUnitApiDTOs.get(0).getAssociatedTargetId());
        assertTrue(businessUnitApiDTOs.get(0).getPricingIdentifiers().isEmpty());
        assertNull(businessUnitApiDTOs.get(0).getAccountId());
        assertFalse(businessUnitApiDTOs.get(0).isHasRelatedTarget());
    }
}
