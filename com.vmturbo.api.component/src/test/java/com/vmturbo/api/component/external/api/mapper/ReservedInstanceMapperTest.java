package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.AzureRIScopeType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.enums.ReservedInstanceType;
import com.vmturbo.api.enums.Tenancy;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for {@link ReservedInstanceMapper} class.
 */
public class ReservedInstanceMapperTest {

    private static final long ACCOUNT_1_ID = 111L;
    private static final long ACCOUNT_2_ID = 222L;
    private static final long NOT_EXISTING_ACCOUNT_ID = 333L;

    private ReservedInstanceMapper reservedInstanceMapper =
        new ReservedInstanceMapper(new CloudTypeMapper());

    private ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
            .setId(123L)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setProbeReservedInstanceId("RI_ID")
                    .setDisplayName("RI display name")
                    .setBusinessAccountId(ACCOUNT_1_ID)
                    .setNumBought(10)
                    .setAvailabilityZoneId(22L)
                    .setReservedInstanceSpec(99L)
                    .setReservationOrderId("ResOrder-1")
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder()
                                    .setAmount(100.0))
                            .setUsageCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(200.0))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(300.0)))
                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCouponsUsed(10)
                            .setNumberOfCoupons(100))
                    .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                            .setShared(false)
                            .addApplicableBusinessAccountId(ACCOUNT_1_ID)
                            .addApplicableBusinessAccountId(ACCOUNT_2_ID)
                            .addApplicableBusinessAccountId(NOT_EXISTING_ACCOUNT_ID)
                            .build()))
            .build();

    private ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
            .setId(99L)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setType(CloudCostDTO.ReservedInstanceType.newBuilder()
                            .setOfferingClass(CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD)
                            .setPaymentOption(CloudCostDTO.ReservedInstanceType.PaymentOption.NO_UPFRONT)
                            .setTermYears(1))
                    .setTenancy(CloudCostDTO.Tenancy.DEFAULT)
                    .setOs(CloudCostDTO.OSType.LINUX)
                    .setTierId(33L)
                    .setRegionId(44L))
            .build();

    @Test
    public void testMapToReservedInstanceApiDTO() throws Exception {
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = new HashMap<>();
        final float delta = 0.000001f;
        final TargetApiDTO target = new TargetApiDTO();
        target.setType(CloudType.AWS.name());
        final ServiceEntityApiDTO mainAccount = new ServiceEntityApiDTO();
        mainAccount.setDiscoveredBy(target);
        mainAccount.setDisplayName("Account");
        final ServiceEntityApiDTO siblingAccount = new ServiceEntityApiDTO();
        siblingAccount.setDiscoveredBy(target);
        siblingAccount.setDisplayName("Sibling Account");
        ServiceEntityApiDTO availabilityZoneEntity = new ServiceEntityApiDTO();
        availabilityZoneEntity.setUuid("22");
        availabilityZoneEntity.setDisplayName("us-east-1a");
        ServiceEntityApiDTO regionEntity = new ServiceEntityApiDTO();
        regionEntity.setUuid("44");
        regionEntity.setDisplayName("us-east-1");
        ServiceEntityApiDTO templateEntity = new ServiceEntityApiDTO();
        templateEntity.setUuid("33");
        templateEntity.setDisplayName("c3.xlarge");
        serviceEntityApiDTOMap.put(ACCOUNT_1_ID, mainAccount);
        serviceEntityApiDTOMap.put(ACCOUNT_2_ID, siblingAccount);
        serviceEntityApiDTOMap.put(22L, availabilityZoneEntity);
        serviceEntityApiDTOMap.put(33L, templateEntity);
        serviceEntityApiDTOMap.put(44L, regionEntity);
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought, riSpec, serviceEntityApiDTOMap);
        assertEquals("RI_ID", reservedInstanceApiDTO.getTrueID());
        assertEquals(String.valueOf(ACCOUNT_1_ID), reservedInstanceApiDTO.getAccountId());
        assertEquals("Account", reservedInstanceApiDTO.getAccountDisplayName());
        assertEquals("ResOrder-1", reservedInstanceApiDTO.getOrderID());
        assertEquals(AzureRIScopeType.SINGLE, reservedInstanceApiDTO.getScopeType());
        assertEquals(Platform.LINUX, reservedInstanceApiDTO.getPlatform());
        assertEquals(ReservedInstanceType.STANDARD, reservedInstanceApiDTO.getType());
        assertEquals("us-east-1a", reservedInstanceApiDTO.getLocation().getDisplayName());
        assertEquals(1.0f, reservedInstanceApiDTO.getTerm().getValue(), delta);
        assertEquals(PaymentOption.NO_UPFRONT, reservedInstanceApiDTO.getPayment());
        assertEquals(10.0f, reservedInstanceApiDTO.getCoupons().getValue(), delta);
        assertEquals(100.0f, reservedInstanceApiDTO.getCoupons().getCapacity().getAvg(), delta);
        assertEquals(10, reservedInstanceApiDTO.getInstanceCount().intValue());
        assertEquals(Tenancy.DEFAULT, reservedInstanceApiDTO.getTenancy());
        assertEquals("RI display name", reservedInstanceApiDTO.getDisplayName());
        assertEquals("ReservedInstance", reservedInstanceApiDTO.getClassName());
        assertEquals(100.0, reservedInstanceApiDTO.getUpFrontCost(), delta);
        assertEquals(300.0, reservedInstanceApiDTO.getActualHourlyCost(), delta);
        assertEquals(CloudType.AWS, reservedInstanceApiDTO.getCloudType());
        assertEquals(Arrays.asList("Account", "Sibling Account"), reservedInstanceApiDTO.getAppliedScopes());
    }

    /**
     * Test mapping for Azure RI.
     *
     * @throws Exception in case of any error.
     */
    @Test
    public void testMapToReservedInstanceApiDTOForAzure() throws Exception {
        testMapToReservedInstanceApiDTOForAzureProbeType(SDKProbeType.AZURE);
    }

    /**
     * Test mapping for Azure EA RI.
     *
     * @throws Exception in case of any error.
     */
    @Test
    public void testMapToReservedInstanceApiDTOForAzureEA() throws Exception {
        testMapToReservedInstanceApiDTOForAzureProbeType(SDKProbeType.AZURE_EA);
    }

    private void testMapToReservedInstanceApiDTOForAzureProbeType(SDKProbeType probeType)
            throws Exception {
        // Arrange
        final ServiceEntityApiDTO businessAccount = new ServiceEntityApiDTO();
        final TargetApiDTO target = new TargetApiDTO();
        target.setType(probeType.getProbeType());
        businessAccount.setDiscoveredBy(target);
        businessAccount.setDisplayName("Main Account");
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = ImmutableMap
            .of(ACCOUNT_1_ID, businessAccount);

        // Act
        final ReservedInstanceApiDTO reservedInstanceApiDTO = reservedInstanceMapper
            .mapToReservedInstanceApiDTO(riBought, riSpec, serviceEntityApiDTOMap);

        // Assert
        assertEquals(CloudType.AZURE, reservedInstanceApiDTO.getCloudType());
    }
}
