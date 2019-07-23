package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.enums.ReservedInstanceType;
import com.vmturbo.api.enums.Tenancy;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

public class ReservedInstanceMapperTest {

    private ReservedInstanceMapper reservedInstanceMapper = new ReservedInstanceMapper();

    private ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
            .setId(123L)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(11L)
                    .setNumBought(10)
                    .setAvailabilityZoneId(22L)
                    .setReservedInstanceSpec(99L)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder()
                                    .setAmount(100.0))
                            .setUsageCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(200.0))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(300.0)))
                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCouponsUsed(10)
                            .setNumberOfCoupons(100)))
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

    private ReservedInstanceStatsRecord riStatsrecord = ReservedInstanceStatsRecord.newBuilder()
            .setCapacity(StatValue.newBuilder()
                    .setMin(10)
                    .setMax(20)
                    .setAvg(15)
                    .setTotal(30))
            .setValues(StatValue.newBuilder()
                    .setMax(5)
                    .setMin(1)
                    .setAvg(3)
                    .setTotal(6))
            .build();


    private static float DELTA = 0.000001f;

    @Test
    public void testMapToReservedInstanceApiDTO() throws Exception {
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = new HashMap<>();
        final float delta = 0.000001f;
        ServiceEntityApiDTO availabilityZoneEntity = new ServiceEntityApiDTO();
        availabilityZoneEntity.setUuid("22");
        availabilityZoneEntity.setDisplayName("us-east-1a");
        ServiceEntityApiDTO regionEntity = new ServiceEntityApiDTO();
        regionEntity.setUuid("44");
        regionEntity.setDisplayName("us-east-1");
        ServiceEntityApiDTO templateEntity = new ServiceEntityApiDTO();
        templateEntity.setUuid("33");
        templateEntity.setDisplayName("c3.xlarge");
        serviceEntityApiDTOMap.put(22L, availabilityZoneEntity);
        serviceEntityApiDTOMap.put(33L, templateEntity);
        serviceEntityApiDTOMap.put(44L, regionEntity);
        final ReservedInstanceApiDTO reservedInstanceApiDTO =
                reservedInstanceMapper.mapToReservedInstanceApiDTO(riBought, riSpec, serviceEntityApiDTOMap);
        assertEquals("11", reservedInstanceApiDTO.getAccountId());
        assertEquals(Platform.LINUX, reservedInstanceApiDTO.getPlatform());
        assertEquals(ReservedInstanceType.STANDARD, reservedInstanceApiDTO.getType());
        assertEquals("us-east-1a", reservedInstanceApiDTO.getLocation().getDisplayName());
        assertEquals(1.0f, reservedInstanceApiDTO.getTerm().getValue(), delta);
        assertEquals(PaymentOption.NO_UPFRONT, reservedInstanceApiDTO.getPayment());
        assertEquals(10.0f, reservedInstanceApiDTO.getCoupons().getValue(), delta);
        assertEquals(100.0f, reservedInstanceApiDTO.getCoupons().getCapacity().getAvg(), delta);
        assertEquals(10, reservedInstanceApiDTO.getInstanceCount().intValue());
        assertEquals(Tenancy.DEFAULT, reservedInstanceApiDTO.getTenancy());
        assertEquals("c3.xlarge", reservedInstanceApiDTO.getDisplayName());
        assertEquals("ReservedInstance", reservedInstanceApiDTO.getClassName());
        assertEquals(100.0, reservedInstanceApiDTO.getUpFrontCost(), delta);
        assertEquals(300.0, reservedInstanceApiDTO.getActualHourlyCost(), delta);
    }

}
