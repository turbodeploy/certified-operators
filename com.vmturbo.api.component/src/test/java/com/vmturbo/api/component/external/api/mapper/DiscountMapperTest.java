package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
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
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;

/**
 * Unit tests for the {@link DiscountMapper}.
 */
public class DiscountMapperTest {

    private static final String TARGET_UUID = "2";
    private static final String SERVICE_DISCOUNT_UUID = "3";
    private static final String TIER_DISCOUNT_UUID = "4";

    private static final long ENTITY_OID = 1L;
    private static final String AWS = "AWS";
    private static final String AWS_BILLING = "AWS Billing";
    private static final String ENGINEERING_AWS_AMAZON_COM = "engineering.aws.amazon.com";
    private static final String CHILDREN_BUSINESS_UNITS = "3";
    private static final String TEST_DSICOUNT = "testDsicount";
    private static final float TIER_DISCOUNT = 11f;
    private static final float SERVICE_DISCOUNT = 22f;
    private static final long ASSOCIATED_ACCOUNT_ID = 1111L;
    private static final double DISCOUNT_PERCENTAGE2 = 20.0;
    private static final double DISCOUNT_PERCENTAGE1 = 10.0;
    private static final String WORKLOAD = "Workload";
    private static final String BUSINESS_ACCOUNT = "BusinessAccount";
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

    private final Discount discount2 = Discount.newBuilder()
            .setId(id)
            .setDiscountInfo(discountInfo1)
            .build();

    private DiscountMapper discountMapper = new DiscountMapper(repositoryApi);

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

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        target1.setUuid(String.valueOf(target_id1));
        target2.setUuid(String.valueOf(target_id2));
        target3.setUuid(String.valueOf(target_id3));
        when(targetsService.getTargets(EnvironmentType.CLOUD))
            .thenReturn(ImmutableList.of(target1, target2, target3));
    }

    /**
     * Test straightforward discount to {@link BusinessUnitApiDTO} conversion.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testToBusinessUnitApiDTO() throws Exception {
        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ASSOCIATED_ACCOUNT_ID)).thenReturn(req);

        BusinessUnitApiDTO businessUnitApiDTO = discountMapper.toBusinessUnitApiDTO(discount1);
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
        assertEquals(false, businessUnitApiDTO.isMaster());
        assertEquals(CloudType.AWS, businessUnitApiDTO.getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTO.getMemberType());
        assertEquals(ASSOCIATED_ACCOUNT_ID, Long.parseLong(businessUnitApiDTO.getUuid()));
        assertEquals(BUSINESS_ACCOUNT, businessUnitApiDTO.getClassName());
        assertEquals(TEST_DSICOUNT, businessUnitApiDTO.getDisplayName());
    }

    /**
     * Test straightforward discount to {@link BusinessUnitApiDTO} conversion.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testToBusinessUnitApiFromAWSBilling() throws Exception {
        final ServiceEntityApiDTO associatedEntity = new ServiceEntityApiDTO();
        associatedEntity.setDiscoveredBy(new TargetApiDTO());
        associatedEntity.getDiscoveredBy().setType(AWS_BILLING);
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(associatedEntity);
        when(repositoryApi.entityRequest(ASSOCIATED_ACCOUNT_ID)).thenReturn(req);

        BusinessUnitApiDTO businessUnitApiDTO = discountMapper.toBusinessUnitApiDTO(discount1);
        assertEquals(BusinessUnitType.DISCOUNT, businessUnitApiDTO.getBusinessUnitType());
        assertEquals(false, businessUnitApiDTO.isMaster());
        assertEquals(CloudType.AWS, businessUnitApiDTO.getCloudType());
        assertEquals(WORKLOAD, businessUnitApiDTO.getMemberType());
        assertEquals(ASSOCIATED_ACCOUNT_ID, Long.parseLong(businessUnitApiDTO.getUuid()));
        assertEquals(BUSINESS_ACCOUNT, businessUnitApiDTO.getClassName());
        assertEquals(TEST_DSICOUNT, businessUnitApiDTO.getDisplayName());
    }

    /**
     * Test mapping API tier discount to its protobuf representation.
     */
    @Test
    public void testToTierDiscountProto() {
        BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServicePriceAdjustments(ImmutableList.of(populateServiceDto()));
        TierLevelDiscount tierLevelDiscount = discountMapper.toTierDiscountProto(businessUnitDiscountApiDTO);
        assertEquals(TIER_DISCOUNT, tierLevelDiscount.getDiscountPercentageByTierIdMap().get(Long.parseLong(TIER_DISCOUNT_UUID)), 0.001);
    }

    /**
     * Test mapping API service discount to its protobuf representation.
     */
    @Test
    public void testToServiceDiscountProto() {
        BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServicePriceAdjustments(ImmutableList.of(populateServiceDto()));
        ServiceLevelDiscount serviceLevelDiscount = discountMapper.toServiceDiscountProto(businessUnitDiscountApiDTO);
        assertEquals(SERVICE_DISCOUNT, serviceLevelDiscount.getDiscountPercentageByServiceIdMap().get(Long.parseLong(SERVICE_DISCOUNT_UUID)), 0.001);
    }

    /**
     * Test mapping {@link Discount} to the associated API {@link BusinessUnitPriceAdjustmentApiDTO}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testToDiscountApiDTO() throws Exception {
        MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);
        SearchRequest req = ApiTestUtils.mockSearchReq(Collections.singletonList(
            ApiPartialEntity.newBuilder()
                .setEntityType(ApiEntityType.CLOUD_SERVICE.typeNumber())
                .setOid(1000L)
                .setDisplayName("Cloud Service")
                .build()
        ));
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(req);

        BusinessUnitPriceAdjustmentApiDTO businessUnitApiDTO = discountMapper.toDiscountApiDTO(discount1);
        assertEquals(1, businessUnitApiDTO.getServicePriceAdjustments().size());
        assertEquals(String.valueOf(1000), businessUnitApiDTO.getServicePriceAdjustments().get(0).getUuid());
        assertEquals(ServicePricingModel.ON_DEMAND, businessUnitApiDTO.getServicePriceAdjustments().get(0).getPricingModel());

        BusinessUnitPriceAdjustmentApiDTO businessUnitApiDTO2 = discountMapper.toDiscountApiDTO(discount2);
        assertEquals(1, businessUnitApiDTO2.getServicePriceAdjustments().size());
        assertEquals(String.valueOf(1000), businessUnitApiDTO2.getServicePriceAdjustments().get(0).getUuid());
        assertEquals(ServicePricingModel.ON_DEMAND, businessUnitApiDTO2.getServicePriceAdjustments().get(0).getPricingModel());
    }
}
