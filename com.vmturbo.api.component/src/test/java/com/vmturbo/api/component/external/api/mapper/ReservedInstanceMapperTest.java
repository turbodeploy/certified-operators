package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for {@link ReservedInstanceMapper} class.
 */
public class ReservedInstanceMapperTest {

    private static final long ACCOUNT_1_ID = 111L;
    private static final long ACCOUNT_2_ID = 222L;
    private static final long NOT_EXISTING_ACCOUNT_ID = 333L;
    private static final long REGION_1_ID = 44L;

    private ReservedInstanceMapper reservedInstanceMapper =
        new ReservedInstanceMapper(new CloudTypeMapper());

    private ReservedInstanceBought.Builder riBought = ReservedInstanceBought.newBuilder()
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
                            .setNumberOfCoupons(100)));

    private ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
            .setId(99L)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setType(CloudCostDTO.ReservedInstanceType.newBuilder()
                            .setOfferingClass(CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD)
                            .setPaymentOption(CommonCost.PaymentOption.NO_UPFRONT)
                            .setTermYears(1))
                    .setTenancy(CloudCostDTO.Tenancy.DEFAULT)
                    .setOs(CloudCostDTO.OSType.LINUX)
                    .setTierId(33L)
                    .setRegionId(REGION_1_ID))
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
        regionEntity.setDiscoveredBy(target);
        ServiceEntityApiDTO templateEntity = new ServiceEntityApiDTO();
        templateEntity.setUuid("33");
        templateEntity.setDisplayName("c3.xlarge");
        serviceEntityApiDTOMap.put(ACCOUNT_1_ID, mainAccount);
        serviceEntityApiDTOMap.put(ACCOUNT_2_ID, siblingAccount);
        serviceEntityApiDTOMap.put(22L, availabilityZoneEntity);
        serviceEntityApiDTOMap.put(33L, templateEntity);
        serviceEntityApiDTOMap.put(44L, regionEntity);
        riBought.getReservedInstanceBoughtInfoBuilder().setReservedInstanceScopeInfo(
                ReservedInstanceScopeInfo.newBuilder()
                        .setShared(false)
                        .addApplicableBusinessAccountId(ACCOUNT_1_ID)
                        .addApplicableBusinessAccountId(ACCOUNT_2_ID)
                        .addApplicableBusinessAccountId(NOT_EXISTING_ACCOUNT_ID)
                        .build());
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought.build(), riSpec,
                        serviceEntityApiDTOMap, 50, null, null);
        assertEquals("RI_ID", reservedInstanceApiDTO.getTrueID());
        assertEquals(String.valueOf(ACCOUNT_1_ID), reservedInstanceApiDTO.getAccountId());
        assertEquals("Account", reservedInstanceApiDTO.getAccountDisplayName());
        assertEquals("ResOrder-1", reservedInstanceApiDTO.getOrderID());
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
        assertEquals(AzureRIScopeType.SINGLE, reservedInstanceApiDTO.getScopeType());
        Assert.assertEquals(50, (int)reservedInstanceApiDTO.getCoveredEntityCount());
        Assert.assertNull(reservedInstanceApiDTO.getUndiscoveredAccountsCoveredCount());
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

    @Test
    public void testMapToReservedInstanceWithExplicitEndDate() throws Exception {
        // Arrange
        final ServiceEntityApiDTO region = new ServiceEntityApiDTO();
        final TargetApiDTO target = new TargetApiDTO();
        target.setType(SDKProbeType.AWS.getProbeType());
        region.setDiscoveredBy(target);
        region.setDisplayName("Main Account");
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = ImmutableMap
                .of(REGION_1_ID, region);

        Instant endTime = Instant.ofEpochMilli(1L);
        riBought.getReservedInstanceBoughtInfoBuilder()
                .setEndTime(endTime.toEpochMilli())
                .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                        .setShared(true)
                        .build());
        // Act
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought.build(), riSpec,
                        serviceEntityApiDTOMap, null, 100, null);
        assertEquals(reservedInstanceApiDTO.getExpDateEpochTime(), new Long(1L));
        Assert.assertEquals(100, (int)reservedInstanceApiDTO.getUndiscoveredAccountsCoveredCount());
        Assert.assertNull(reservedInstanceApiDTO.getCoveredEntityCount());
        Assert.assertNull(reservedInstanceApiDTO.getAppliedScopes());
        Assert.assertEquals(AzureRIScopeType.SHARED, reservedInstanceApiDTO.getScopeType());
    }

    private void testMapToReservedInstanceApiDTOForAzureProbeType(SDKProbeType probeType)
            throws Exception {
        // Arrange
        final ServiceEntityApiDTO region = new ServiceEntityApiDTO();
        final TargetApiDTO target = new TargetApiDTO();
        target.setType(probeType.getProbeType());
        region.setDiscoveredBy(target);
        region.setDisplayName("Main Account");
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = ImmutableMap
                .of(REGION_1_ID, region);
        riBought.getReservedInstanceBoughtInfoBuilder()
                .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                        .build());
        // Act
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought.build(), riSpec,
                        serviceEntityApiDTOMap, 100, 50, null);

        // Assert
        assertEquals(CloudType.AZURE, reservedInstanceApiDTO.getCloudType());
        Assert.assertEquals(100, (int)reservedInstanceApiDTO.getCoveredEntityCount());
        Assert.assertEquals(50, (int)reservedInstanceApiDTO.getUndiscoveredAccountsCoveredCount());
        Assert.assertNull(reservedInstanceApiDTO.getAppliedScopes());
        Assert.assertEquals(AzureRIScopeType.UNKNOWN, reservedInstanceApiDTO.getScopeType());
    }

    @Test
    public void testMapToReservedInstanceApiDTOwithTargetId() throws Exception {
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = new HashMap<>();
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
        regionEntity.setDiscoveredBy(target);
        ServiceEntityApiDTO templateEntity = new ServiceEntityApiDTO();
        templateEntity.setUuid("33");
        templateEntity.setDisplayName("c3.xlarge");
        serviceEntityApiDTOMap.put(ACCOUNT_1_ID, mainAccount);
        serviceEntityApiDTOMap.put(ACCOUNT_2_ID, siblingAccount);
        serviceEntityApiDTOMap.put(22L, availabilityZoneEntity);
        serviceEntityApiDTOMap.put(33L, templateEntity);
        serviceEntityApiDTOMap.put(44L, regionEntity);
        riBought.getReservedInstanceBoughtInfoBuilder().setReservedInstanceScopeInfo(
                ReservedInstanceScopeInfo.newBuilder()
                        .setShared(false)
                        .addApplicableBusinessAccountId(ACCOUNT_1_ID)
                        .addApplicableBusinessAccountId(ACCOUNT_2_ID)
                        .build());
        long targetId = 123L;
        List<TopologyEntityDTO> accountList = Lists.newArrayList();
        TypeSpecificInfo accountInfo = TypeSpecificInfo.newBuilder()
                .setBusinessAccount(TypeSpecificInfo.BusinessAccountInfo.newBuilder()
                        .setAssociatedTargetId(targetId)
                        .build())
                .build();
        accountList.add(TopologyEntityDTO.newBuilder()
                .setOid(ACCOUNT_2_ID)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setTypeSpecificInfo(accountInfo)
                .build());

        // ACCOUNT_1_ID is not in the 'accountList', so we will not be able to populate the target id.
        ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought.build(), riSpec,
                        serviceEntityApiDTOMap, 50, null, accountList);
        assertEquals("RI_ID", reservedInstanceApiDTO.getTrueID());
        assertNull(reservedInstanceApiDTO.getTargetId());

        accountList.add(TopologyEntityDTO.newBuilder()
                .setOid(ACCOUNT_1_ID)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setTypeSpecificInfo(accountInfo)
                .build());

        // ACCOUNT_1_ID added to the 'accountList', so target id can be set.
        reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought.build(), riSpec,
                        serviceEntityApiDTOMap, 50, null, accountList);
        assertEquals("RI_ID", reservedInstanceApiDTO.getTrueID());
        assertEquals(String.valueOf(targetId), reservedInstanceApiDTO.getTargetId());
    }
}
