package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AwsReservedInstanceCouponTest {

    @Test
    public void testConvertInstanceTypeToCoupons() {
        assertEquals(0, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("unknown"));
        assertEquals(0, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons(null));
        assertEquals(32, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("r3.xlarge"));
        assertEquals(0, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("r3.xmedium"));
        assertEquals(1, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("t2.nano"));
        assertEquals(2, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("t2.micro"));
        assertEquals(8, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("m4.medium"));
        assertEquals(64, AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("m4.2xlarge"));
        assertEquals(117 * AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("m4.xlarge"),
                AwsReservedInstanceCoupon.convertInstanceTypeToCoupons("m4.117xlarge"));
    }
}
