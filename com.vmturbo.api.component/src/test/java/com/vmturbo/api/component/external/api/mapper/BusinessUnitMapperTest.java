package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.service.SearchService;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.ServicePricingModel;
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
    private static final long target_id1 = 11L;
    private static final long target_id2 = 22L;
    private static final long target_id3 = 33L;
    private static final TargetApiDTO target1 = new TargetApiDTO();
    private static final TargetApiDTO target2 = new TargetApiDTO();
    private static final TargetApiDTO target3 = new TargetApiDTO();

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
        target1.setUuid(String.valueOf(target_id1));
        target2.setUuid(String.valueOf(target_id2));
        target3.setUuid(String.valueOf(target_id3));
        when(targetsService.getTargets(EnvironmentType.CLOUD))
            .thenReturn(ImmutableList.of(target1, target2, target3));
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
                DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(target_id2)
                    .addDiscoveringTargetIds(target_id1)))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder()
                    .setAssociatedTargetId(target_id2)
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
        associatedEntity.setDiscoveredBy(target2);
        associatedEntity.getDiscoveredBy().setType(AWS);

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        final BusinessUnitApiDTO result = businessUnitApiDTOs.iterator().next();
        assertEquals(String.valueOf(ENTITY_OID), result.getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, result.getBusinessUnitType());
        assertEquals(CloudType.AWS, result.getCloudType());
        assertEquals(WORKLOAD, result.getMemberType());
        assertEquals((Long)target_id2, result.getAssociatedTargetId());
        assertEquals(2, result.getPricingIdentifiers().size());
        assertEquals(offerId, result.getPricingIdentifiers()
            .get(PricingIdentifierName.OFFER_ID.name()));
        assertEquals(enrollmentNum, result.getPricingIdentifiers()
            .get(PricingIdentifierName.ENROLLMENT_NUMBER.name()));
        assertEquals(accountId, result.getAccountId());
        assertTrue(result.isHasRelatedTarget());
        assertEquals(2, result.getTargets().size());
        assertThat(result.getTargets(), containsInAnyOrder(target1, target2));
    }

    @Test
    public void testToDiscoveredBusinessUnitDTOWithBillingProbe() throws Exception {
        TopologyEntityDTO entity = createBusinessAccount(ENTITY_OID,
            Collections.singletonList(target_id2))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder().setAssociatedTargetId(target_id2)))
            .build();

        final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(target2);
        associatedEntity.getDiscoveredBy().setType(AWS);

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        target1.setType("AWS Billing");
        target1.setDisplayName("engineering.billing.aws.amazon.com");

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        final BusinessUnitApiDTO result = businessUnitApiDTOs.iterator().next();
        assertEquals(String.valueOf(ENTITY_OID), result.getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, result.getBusinessUnitType());
        // still AWS instead of billing probe
        assertEquals(CloudType.AWS, result.getCloudType());
        assertEquals(WORKLOAD, result.getMemberType());
        assertTrue(result.getPricingIdentifiers().isEmpty());
    }

    /**
     * Test that the empty business account creates the correct BusinessUnitApiDTO.
     *
     * @throws Exception when businessUnitMapper.getAndConvertDiscoveredBusinessUnits throws one.
     */
    @Test
    public void testEmptyBusinessAccountToDiscoveredBusinessUnitDTO() throws Exception {
        TopologyEntityDTO entity = createBusinessAccount(ENTITY_OID,
            Collections.singletonList(target_id2)).build();
       final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(target2);
        associatedEntity.getDiscoveredBy().setType(AWS);

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        assertEquals(1, businessUnitApiDTOs.size());
        final BusinessUnitApiDTO result = businessUnitApiDTOs.iterator().next();
        assertEquals(String.valueOf(ENTITY_OID), result.getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, result.getBusinessUnitType());
        assertEquals(CloudType.AWS, result.getCloudType());
        assertEquals(WORKLOAD, result.getMemberType());
        assertNull(result.getAssociatedTargetId());
        assertTrue(result.getPricingIdentifiers().isEmpty());
        assertNull(result.getAccountId());
        assertFalse(result.isHasRelatedTarget());
        assertEquals(1, result.getTargets().size());
        assertEquals(target2, result.getTargets().get(0));
    }

    /**
     * Test that when we get all business accounts scoped to a set of targets, we get the right
     * set of business accounts.
     *
     * @throws Exception when businessUnitMapper.getAndConvertDiscoveredBusinessUnits throws one.
     */
    @Test
    public void testGetBusinessAccountByTargetScope() throws Exception {
        final long entity1_id = 111L;
        final long entity2_id = 222L;
        final long entity3_id = 333L;

        final TopologyEntityDTO businessAccount1 = createBusinessAccount(entity1_id,
            ImmutableList.of(target_id1, target_id3)).build();
        final TopologyEntityDTO businessAccount2 = createBusinessAccount(entity2_id,
            ImmutableList.of(target_id2)).build();
        final TopologyEntityDTO businessAccount3 = createBusinessAccount(entity3_id,
            ImmutableList.of(target_id3)).build();
        final SearchRequest request = ApiTestUtils.mockSearchFullReq(ImmutableList.of(businessAccount1,
            businessAccount2, businessAccount3));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);
        final ServiceEntityApiDTO associatedEntity1 = new ServiceEntityApiDTO();
        associatedEntity1.setDiscoveredBy(target1);
        associatedEntity1.getDiscoveredBy().setType(AWS);
        final ServiceEntityApiDTO associatedEntity2 = new ServiceEntityApiDTO();
        associatedEntity2.setDiscoveredBy(target2);
        associatedEntity2.getDiscoveredBy().setType(AWS);
        final ServiceEntityApiDTO associatedEntity3 = new ServiceEntityApiDTO();
        associatedEntity3.setDiscoveredBy(target3);
        associatedEntity3.getDiscoveredBy().setType(AWS);
        final SingleEntityRequest req1 = ApiTestUtils.mockSingleEntityRequest(associatedEntity1);
        final SingleEntityRequest req2 = ApiTestUtils.mockSingleEntityRequest(associatedEntity2);
        final SingleEntityRequest req3 = ApiTestUtils.mockSingleEntityRequest(associatedEntity3);
        when(repositoryApi.entityRequest(entity1_id)).thenReturn(req1);
        when(repositoryApi.entityRequest(entity2_id)).thenReturn(req2);
        when(repositoryApi.entityRequest(entity3_id)).thenReturn(req3);

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        List<BusinessUnitApiDTO> businessUnitApiDTOs = businessUnitMapper
            .getAndConvertDiscoveredBusinessUnits(targetsService,
                Collections.singletonList(String.valueOf(target_id3)));
        assertEquals(2, businessUnitApiDTOs.size());
        assertThat(businessUnitApiDTOs.stream().map(BusinessUnitApiDTO::getUuid)
            .collect(Collectors.toList()), containsInAnyOrder(String.valueOf(entity1_id),
            String.valueOf(entity3_id)));

        businessUnitApiDTOs = businessUnitMapper
            .getAndConvertDiscoveredBusinessUnits(targetsService,
                ImmutableList.of(String.valueOf(target_id1), String.valueOf(target_id2)));
        assertEquals(2, businessUnitApiDTOs.size());
        assertThat(businessUnitApiDTOs.stream().map(BusinessUnitApiDTO::getUuid)
            .collect(Collectors.toList()), containsInAnyOrder(String.valueOf(entity1_id),
            String.valueOf(entity2_id)));
    }

    private TopologyEntityDTO.Builder createBusinessAccount(long oid,
                                                         @Nonnull List<Long> discoveringTargets) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder().addAllDiscoveringTargetIds(discoveringTargets)))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder()));
    }

    @Test
    public void testGetBusinessUnitByOID() throws Exception {
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(ENTITY_OID)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                    DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(id)))
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                    BusinessAccountInfo.newBuilder()))
                .build();

        final SearchRequest request = ApiTestUtils.mockSearchFullReq(Collections.singletonList(entity));
        when(repositoryApi.newSearchRequest(any())).thenReturn(request);


        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(TARGET_UUID);
        targetApiDTO.setType("AWS Billing");
        targetApiDTO.setDisplayName("engineering.billing.aws.amazon.com");
        when(targetsService.getTargets(EnvironmentType.CLOUD))
            .thenReturn(ImmutableList.of(target1, target2, targetApiDTO));

        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(targetApiDTO);
        associatedEntity.getDiscoveredBy().setType(AWS);

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ENTITY_OID)).thenReturn(req);

        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        BusinessUnitApiDTO businessUnitApiDTO = businessUnitMapper.getBusinessUnitByOID(targetsService, String.valueOf(ENTITY_OID));
        assertEquals(String.valueOf(ENTITY_OID), businessUnitApiDTO.getUuid());
        assertEquals(BusinessUnitType.DISCOVERED, businessUnitApiDTO.getBusinessUnitType());
        assertEquals(CloudType.AWS, businessUnitApiDTO.getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTO.getMemberType());
    }
}
