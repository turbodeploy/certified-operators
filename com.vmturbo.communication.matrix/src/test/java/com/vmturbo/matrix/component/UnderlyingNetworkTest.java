package com.vmturbo.matrix.component;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.matrix.component.external.WeightClass;

/**
 * Tests underlying network.
 */
public class UnderlyingNetworkTest {
    /**
     * Tests adding physical hosts.
     */
    @Test
    public void testAddPMs() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(101L, 1000L);
        Assert.assertEquals(Arrays.asList(100L, 101L),
                            matrix.underlayNetwork_.entities_.keySet().stream().map(
                                VolatileLong::getValue).collect(Collectors.toList()));
    }

    /**
     * Tests blade distance.
     */
    @Test
    public void testDistancesBlade() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(200L, 1000L);
        Assert.assertEquals(Arrays.asList(100L, 200L),
                            matrix.underlayNetwork_.entities_.keySet().stream()
                                                             .map(VolatileLong::getValue)
                                                             .collect(Collectors.toList()));
        Assert.assertEquals(WeightClass.BLADE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(100L)));
        Assert.assertEquals(WeightClass.BLADE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(200L), new VolatileLong(200L)));
    }

    /**
     * Tests unknown distance.
     */
    @Test
    public void testDistancesNonKnown() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(200L, 1000L);
        Assert.assertEquals(Arrays.asList(100L, 200L),
                            matrix.underlayNetwork_.entities_.keySet().stream()
                                                             .map(VolatileLong::getValue)
                                                             .collect(Collectors.toList()));
        Assert.assertEquals(WeightClass.SITE, matrix.underlayNetwork_.computeDistance(null, null));
    }

    /**
     * Tests site distance.
     */
    @Test
    public void testDistancesSite() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(200L, 1000L);
        Assert.assertEquals(Arrays.asList(100L, 200L),
                            matrix.underlayNetwork_.entities_.keySet().stream()
                                                             .map(VolatileLong::getValue)
                                                             .collect(Collectors.toList()));
        Assert.assertEquals(WeightClass.SITE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(200L)));
        // And repeat.
        Assert.assertEquals(WeightClass.SITE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(200L)));
    }

    /**
     * Tests distance with no partner.
     */
    @Test
    public void testDistancesNoPartner() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(200L, 1000L);
        Assert.assertNull(matrix.underlayNetwork_
                              .computeDistance(new VolatileLong(100L), new VolatileLong(300L)));
        Assert.assertNull(matrix.underlayNetwork_
                              .computeDistance(new VolatileLong(400L), new VolatileLong(300L)));
        Assert.assertNull(matrix.underlayNetwork_
                              .computeDistance(new VolatileLong(400L), new VolatileLong(200L)));
    }

    /**
     * Tests DPoDs.
     */
    @Test
    public void testDpod() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.populateUnderlay(100L, 1000L);
        matrix.populateUnderlay(200L, 1000L);
        matrix.populateUnderlay(300L, 1000L);
        matrix.populateUnderlay(400L, 1000L);
        matrix.populateUnderlay(500L, 1000L);
        matrix.populateUnderlay(600L, 2000L);
        matrix.populateDpod(new HashSet<>(Arrays.asList(100L, 300L)));
        Assert.assertEquals(WeightClass.SITE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(200L)));
        Assert.assertEquals(WeightClass.SWITCH,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(300L)));
        Assert.assertEquals(WeightClass.SWITCH,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(300L), new VolatileLong(100L)));
        Assert.assertEquals(WeightClass.BLADE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(300L), new VolatileLong(300L)));
        Assert.assertEquals(WeightClass.CROSS_SITE,
                            matrix.underlayNetwork_
                                .computeDistance(new VolatileLong(100L), new VolatileLong(600L)));
        Set<Long> dpod = new HashSet<>();
        dpod.add(100L);
        dpod.add(300L);
        Assert.assertEquals(
            matrix.underlayNetwork_.entities_.get(new VolatileLong(100L)).dpod_,
            dpod);
        Assert.assertEquals(
            matrix.underlayNetwork_.entities_.get(new VolatileLong(200L)).dpod_,
            Collections.emptySet());
        Assert.assertEquals(
            matrix.underlayNetwork_.entities_.get(new VolatileLong(400L)).dpod_,
            Collections.emptySet());
        Assert.assertEquals(
            matrix.underlayNetwork_.entities_.get(new VolatileLong(500L)).dpod_,
            Collections.emptySet());
    }
}
