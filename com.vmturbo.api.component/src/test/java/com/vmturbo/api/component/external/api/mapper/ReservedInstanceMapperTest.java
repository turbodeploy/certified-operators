package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.UuidMapper.UI_REAL_TIME_MARKET_STR;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

public class ReservedInstanceMapperTest {

    final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private ReservedInstanceMapper reservedInstanceMapper = new ReservedInstanceMapper(repositoryApi);

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

    @Test
    public void testRiCountToEntityStatsApiDTO() {
        final float delta = 0.000001f;
        final Map<Long, Long> riCountMap = ImmutableMap.of(11L, 5L, 12L, 10L,
                13L, 15L);
        final Map<Long, MinimalEntity> serviceEntityApiDTOMap = new HashMap<>();
        MinimalEntity templateEntityOne = MinimalEntity.newBuilder()
            .setOid(11)
            .setDisplayName("c3.xlarge")
            .build();
        MinimalEntity templateEntityTwo = MinimalEntity.newBuilder()
            .setOid(12)
            .setDisplayName("m3.xlarge")
            .build();
        MinimalEntity templateEntityThree = MinimalEntity.newBuilder()
            .setOid(13)
            .setDisplayName("r3.xlarge")
            .build();
        serviceEntityApiDTOMap.put(11L, templateEntityOne);
        serviceEntityApiDTOMap.put(12L, templateEntityTwo);
        serviceEntityApiDTOMap.put(13L, templateEntityThree);
        final EntityStatsApiDTO entityStatsApiDTO =
                reservedInstanceMapper.riCountMapToEntityStatsApiDTO(riCountMap, serviceEntityApiDTOMap);
        assertEquals(1L, entityStatsApiDTO.getStats().size());
        assertEquals(3L, entityStatsApiDTO.getStats().get(0).getStatistics().size());
        final List<StatApiDTO> statApiDTOList = entityStatsApiDTO.getStats().get(0).getStatistics();
        assertEquals(5L, statApiDTOList.stream()
                .filter(stat -> stat.getFilters().get(0).getValue().equals("c3.xlarge"))
                .mapToDouble(StatApiDTO::getValue)
                .sum(), delta);
        assertEquals(5L, statApiDTOList.stream()
                .filter(stat -> stat.getFilters().size() == 1
                        && stat.getFilters().get(0).getValue().equals("c3.xlarge"))
                .mapToDouble(StatApiDTO::getValue)
                .sum(), delta);
        assertEquals(10L, statApiDTOList.stream()
                .filter(stat -> stat.getFilters().size() == 1
                        && stat.getFilters().get(0).getValue().equals("m3.xlarge"))
                .mapToDouble(StatApiDTO::getValue)
                .sum(), delta);
        assertEquals(15L, statApiDTOList.stream()
                .filter(stat -> stat.getFilters().size() == 1
                        && stat.getFilters().get(0).getValue().equals("r3.xlarge"))
                .mapToDouble(StatApiDTO::getValue)
                .sum(), delta);
    }

    @Test
    public void testRiUtilizationStatsRecordsToEntityStatsApiDTO() throws Exception {
        final EntityStatsApiDTO entityStatsApiDTO =
                reservedInstanceMapper.convertRIStatsRecordsToEntityStatsApiDTO(
                        Lists.newArrayList(riStatsrecord), UI_REAL_TIME_MARKET_STR,
                        Optional.empty(), Optional.empty(), false);
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = entityStatsApiDTO.getStats();
        assertEquals(1L, statSnapshotApiDTOS.size());

        assertEquals(1L, statSnapshotApiDTOS.get(0).getStatistics().size());
        final StatApiDTO statApiDTO = statSnapshotApiDTOS.get(0).getStatistics().get(0);
        assertEquals(10, statApiDTO.getCapacity().getMin(), DELTA);
        assertEquals(20, statApiDTO.getCapacity().getMax(), DELTA);
        assertEquals(15, statApiDTO.getCapacity().getAvg(), DELTA);
        assertEquals(30, statApiDTO.getCapacity().getTotal(), DELTA);
        assertEquals(5, statApiDTO.getValues().getMax(), DELTA);
        assertEquals(1, statApiDTO.getValues().getMin(), DELTA);
        assertEquals(3, statApiDTO.getValues().getAvg(), DELTA);
        assertEquals(6, statApiDTO.getValues().getTotal(), DELTA);
    }

    @Test
    public void testRiCoverageStatsRecordsToStatSnapshotApiDTO() {
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS =
                reservedInstanceMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                        Lists.newArrayList(riStatsrecord), true);
        assertEquals(1L, statSnapshotApiDTOS.size());

        assertEquals(1L, statSnapshotApiDTOS.get(0).getStatistics().size());
        final StatApiDTO statApiDTO = statSnapshotApiDTOS.get(0).getStatistics().get(0);
        assertEquals(10, statApiDTO.getCapacity().getMin(), DELTA);
        assertEquals(20, statApiDTO.getCapacity().getMax(), DELTA);
        assertEquals(15, statApiDTO.getCapacity().getAvg(), DELTA);
        assertEquals(30, statApiDTO.getCapacity().getTotal(), DELTA);
        assertEquals(5, statApiDTO.getValues().getMax(), DELTA);
        assertEquals(1, statApiDTO.getValues().getMin(), DELTA);
        assertEquals(3, statApiDTO.getValues().getAvg(), DELTA);
        assertEquals(6, statApiDTO.getValues().getTotal(), DELTA);
    }

}
