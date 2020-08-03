package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * JUnit test to test ReservedInstancePurchaseConstraint methods.
 */
public class ReservedInstancePurchaseConstraintsTest {

    @Test
    public void test() {
        ReservedInstancePurchaseConstraints constraint1 = new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD,
            1, PaymentOption.NO_UPFRONT);
        ReservedInstancePurchaseConstraints constraint2 = new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD,
            1, PaymentOption.NO_UPFRONT);
        int hashCode1 = constraint1.hashCode();
        int hashCode2 = constraint2.hashCode();
        Assert.assertTrue( hashCode1 == hashCode2);
        Assert.assertTrue(constraint1.equals(constraint2));

        ReservedInstancePurchaseConstraints constraint3 = new ReservedInstancePurchaseConstraints(OfferingClass.CONVERTIBLE,
            1, PaymentOption.NO_UPFRONT);
        int hashCode3 = constraint3.hashCode();
        Assert.assertFalse( hashCode1 == hashCode3);
        Assert.assertFalse(constraint1.equals(constraint3));

        ReservedInstancePurchaseConstraints constraint4 = new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD,
            3, PaymentOption.NO_UPFRONT);
        int hashCode4 = constraint4.hashCode();
        Assert.assertFalse( hashCode1 == hashCode4);
        Assert.assertFalse(constraint1.equals(constraint4));

        ReservedInstancePurchaseConstraints constraint5 = new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD,
            1, PaymentOption.PARTIAL_UPFRONT);
        int hashCode5 = constraint5.hashCode();
        Assert.assertFalse( hashCode1 == hashCode5);
        Assert.assertFalse(constraint1.equals(constraint5));

        ReservedInstancePurchaseConstraints constraint6 = new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD,
            1, PaymentOption.ALL_UPFRONT);
        int hashCode6 = constraint6.hashCode();
        Assert.assertFalse( hashCode1 == hashCode6);
        Assert.assertFalse(constraint1.equals(constraint6));
    }
}
