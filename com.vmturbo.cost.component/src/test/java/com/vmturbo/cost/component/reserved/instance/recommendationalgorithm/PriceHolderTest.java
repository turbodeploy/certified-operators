package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link PriceHolder} tests.
 */
public class PriceHolderTest {

    /**
     * Test {@link PriceHolder} creation and access to price tables.
     */
    @Test
    public void test() {
        // priceId->Price table mapping
        Map<Long, String> priceTables = ImmutableMap.<Long, String>builder().put(1L, "101,First")
                .put(2L, "201,Second")
                .build();

        //BA to priceId mapping
        Map<Long, Long> idMapping =
                ImmutableMap.<Long, Long>builder().put(11L, 1L).put(12L, 2L).put(13L, 1L).build();

        PriceHolder<String, String> ph =
                new PriceHolder<>(priceTables, idMapping, PriceHolderTest::transform);

        Assert.assertEquals(ph.get(11, 101), "First");
        Assert.assertEquals(ph.get(12, 201), "Second");
        Assert.assertEquals(ph.get(13, 101), "First");
        Assert.assertNull(ph.get(12, 101));
        Assert.assertNull(ph.get(1222, 101));
    }

    private static Map<Long, String> transform(String e) {
        String[] parts = e.split(",");
        return ImmutableMap.of(Long.parseLong(parts[0]), parts[1]);
    }
}

