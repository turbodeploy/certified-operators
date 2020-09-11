package com.vmturbo.platform.analysis.economy;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;

/**
 * The unit test class for Context.
 *
 */
public class ContextTest {

    /**
     * Test the overridden equals() for {@link Context}.
     */
    @Test
    public void testContextEquals() {
        long region1 = 1L;
        long region2 = 2L;
        long baId1 = 100L;
        Context context1 = new Context(region1, region1, new BalanceAccount(baId1));
        Context context2 = new Context(region2, region2, new BalanceAccount(baId1));
        Context context3 = new Context(region1, region1, new BalanceAccount(baId1));

        Assert.assertTrue(context1.equals(context3));
        Assert.assertFalse(context1.equals(context2));
    }

    /**
     * Test the overridden equals() for {@link BalanceAccount}.
     */
    @Test
    public void testBalanceAccountEquals() {
        BalanceAccount ba1 = new BalanceAccount(1);
        BalanceAccount ba2 = new BalanceAccount(1);
        Assert.assertTrue(ba1.equals(ba2));

        ba1.setBudget(10);
        Assert.assertFalse(ba1.equals(ba2));

        BalanceAccount ba3 = new BalanceAccount(100, 200, 2, 3);
        BalanceAccount ba4 = new BalanceAccount(100, 200, 2, 3);
        BalanceAccount ba5 = new BalanceAccount(100, 200, 2, 4);
        Assert.assertTrue(ba3.equals(ba4));
        Assert.assertFalse(ba3.equals(ba5));
    }

    /**
     * Test comparison of BalanceAccount instances while ignoring the parentId.
     */
    @Test
    public void testBalanceAccountComparison() {
        BalanceAccount ba1 = new BalanceAccount(100, 200, 2, 3, 2116L);
        // Same as BalanceAccount 1, but with no parentId
        BalanceAccount ba2 = new BalanceAccount(100, 200, 2, 3);
        // Lower spent amount
        BalanceAccount ba3 = new BalanceAccount(99, 200, 2, 3);

        /*
         * ba1 and ba2 are equivalent, because the parentId is ignored in the comparison.
         * However, the normal equals should show that ba1 and ba2 are not equal.
         * ba3 should compare less than ba1 and ba2
         */
        Assert.assertTrue(ba1.compareWithoutParentId(ba2) == 0);
        Assert.assertFalse(ba1.equals(ba2));
        Assert.assertTrue(ba3.compareWithoutParentId(ba1) < 0);
        Assert.assertTrue(ba3.compareWithoutParentId(ba2) < 0);
    }
}
