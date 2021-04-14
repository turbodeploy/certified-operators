package com.vmturbo.cost.component.savings;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.cost.component.savings.Algorithm.SavingsInvestments;

/**
 * Tests for Algorithm-2.
 */
public class Algorithm2Test {

    /**
     * Low level test of Algorithm-2.
     */
    @Test
    public void testAddAction() {
        List<Double> deltaList = ImmutableList.of(2.0, 2.0, -2.0, 6.0, -4.0, -6.0, 4.0, -8.0, 6.0,
                -4.0, 10.0, -8.0);
        Algorithm alg = new Algorithm2(0L, 0L);
        deltaList.stream().forEach(delta -> alg.addAction(delta, 10L));
        alg.endPeriod(0L, 1L);
        SavingsInvestments results = alg.getRealized();
        Assert.assertEquals(10.0, results.getSavings(), 0.0001);
        Assert.assertEquals(4.0, results.getInvestments(), 0.0001);
    }

    /**
     * Verify that we handle an older entity state instance that has no action expiration
     * information.
     */
    @Test
    public void testExpiredActionNoExpirationList() {
        final long periodEnd = 1L;
        EntityState entityState = EntityState.fromJson("{\"entityId\":74612826803208,\"powerFactor\":1,\"actionList\":[2.0, 2.0, -2.0, 6.0, -4.0, -6.0, 4.0, -8.0, 6.0, -4.0, 10.0, -8.0],\"realizedSavings\":10.0,\"realizedInvestments\":4.0,\"missedSavings\":0.0,\"missedInvestments\":0.0}");
        Algorithm alg = new Algorithm2(0L, 0L);
        alg.initState(periodEnd, entityState);
        Assert.assertTrue(alg.getActionList().isEmpty());
        Assert.assertTrue(alg.getExpirationList().isEmpty());
        SavingsInvestments results = alg.getRealized();
        Assert.assertEquals(0.0, results.getSavings(), 0.0001);
        Assert.assertEquals(0.0, results.getInvestments(), 0.0001);
    }
}