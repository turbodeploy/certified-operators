package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.SQLReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.sql.utils.DbException;

public class RIBuyDemandCalculatorTest {

    //TODO
    @Test
    public void testCalculateUncoveredDemand() {

    }

    /**
     * Tests the adjustment of coupons for newly purchased or discovered RIs.
     * @throws DbException throws a DB exception.
     */
    @Test
    public void testSubtractCouponsFromDemand() throws DbException {
        ReservedInstanceHistoricalDemandDataType reservedInstanceHistoricalDemandDataType =
                ReservedInstanceHistoricalDemandDataType.CONSUMPTION;
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                                                        .setId(5L)
                                                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().build())
                                                        .build();
        Calendar purchaseDate = Calendar.getInstance();
        purchaseDate.add(Calendar.YEAR, -1);
        ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                                            .setId(1L)
                                            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                                                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                                            .setNumberOfCouponsUsed(5)
                                                            .setNumberOfCoupons(8).build())
                                                    .setBusinessAccountId(2L)
                                                    .setNumBought(8)
                                                    .setStartTime(purchaseDate.getTimeInMillis())
                                                    .setReservedInstanceSpec(riSpec.getId())
                                                    .setAvailabilityZoneId(4L)
                                                    .setProbeReservedInstanceId("RI1").build())
                                            .build();
        ReservedInstanceBoughtStore riBoughtStore = Mockito.mock(SQLReservedInstanceBoughtStore.class);
        Calendar yesterday = Calendar.getInstance();
        float[] demand = new float[RIBuyDemandCalculator.WEEKLY_DEMAND_DATA_SIZE];
        for (int i = 0; i < demand.length; i++) {
            demand[i] = 12.0f;
        }


        RIBuyDemandCalculator demandCalculator =
                new RIBuyDemandCalculator(Mockito.mock(RIBuyHistoricalDemandProvider.class),
                        reservedInstanceHistoricalDemandDataType,
                        Mockito.mock(RegionalRIMatcherCache.class), 0.6f,
                        riBoughtStore);

        Map<Long, ReservedInstanceAdjustmentTracker> riAdjustmentsById = new HashMap<>();
        // Test for a newly discovered RI with a past purchase date
        yesterday.add(Calendar.DATE, -1);
        demandCalculator.subtractCouponsFromDemand(demand, riAdjustmentsById, riBought, "UnitTest", yesterday.getTimeInMillis());
        //adjustment calculated for RI discovered 0 weeks ago
        double expectedLargeAdjustment =
                Math.pow(0.4d, 0) * riBought.getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        double expectedSmallAdjustment =
                Math.pow(0.4d, 1) * riBought.getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        Assert.assertEquals(expectedLargeAdjustment, riAdjustmentsById.get(riBought.getId()).getLargeAdjustment(), 0.0001);
        Assert.assertEquals(expectedSmallAdjustment, riAdjustmentsById.get(riBought.getId()).getSmallAdjustment(), 0.0001);

        // Test for a newly discovered RI with a past discovery date = purchase date
        riAdjustmentsById.clear();
        for (int i = 0; i < demand.length; i++) {
            demand[i] = 12.0f;
        }
        //adjustment calculated for RI purchased and discovered 52 weeks ago
        demandCalculator.subtractCouponsFromDemand(demand, riAdjustmentsById, riBought, "UnitTest", purchaseDate.getTimeInMillis());
        expectedLargeAdjustment =
                Math.pow(0.4d, 52) * riBought.getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        expectedSmallAdjustment =
                Math.pow(0.4d, 53) * riBought.getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        Assert.assertEquals(expectedLargeAdjustment, riAdjustmentsById.get(riBought.getId()).getLargeAdjustment(), 0.0001);
        Assert.assertEquals(expectedSmallAdjustment, riAdjustmentsById.get(riBought.getId()).getSmallAdjustment(), 0.0001);
    }


}
