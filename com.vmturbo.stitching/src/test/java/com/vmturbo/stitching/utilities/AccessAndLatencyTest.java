package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAccessIOPS;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageLatencyMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

public class AccessAndLatencyTest {

    @Test
    public void testHasLatency() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(1.0).build().toBuilder()));

        assertTrue(accessAndLatency.hasLatency());

        final AccessAndLatency missingLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().build().toBuilder()),
            Optional.of(storageAccessIOPS().used(1.0).build().toBuilder()));

        assertFalse(missingLatency.hasLatency());
    }

    @Test
    public void testIopsWeight() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()));

        assertEquals(2.0, accessAndLatency.iopsWeight(), 0.0);

        final AccessAndLatency missingAccess = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().build().toBuilder()));

        // When IOPS is not present, return 1.0 so that we can calculate a uniform average.
        assertEquals(1.0, missingAccess.iopsWeight(), 0.0);
    }

    @Test
    public void testWeightedLatencyPresent() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()));

        assertEquals(1.0 * 2.0, accessAndLatency.weightedLatency().get(), 0.0);

        final AccessAndLatency otherAccessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(10.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()));

        assertEquals(10.0 * 0.5, otherAccessAndLatency.weightedLatency().get(), 0.0);
    }

    @Test
    public void testWeightedLatencyAbsent() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.empty(),
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()));

        assertFalse(accessAndLatency.weightedLatency().isPresent());
    }

    @Test
    public void testLatencyWeightedAveraged() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()));

        final AccessAndLatency otherAccessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(10.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()));
        
        assertEquals(
            (1.0 * 2.0 + 10.0 * 0.5) / (2.0 + 0.5),
            AccessAndLatency.latencyWeightedAveraged(Arrays.asList(accessAndLatency, otherAccessAndLatency)),
            0.0);
    }

    @Test
    public void testLatencyWeightedAveragedWithMissingLatencyNoValues() {
        final AccessAndLatency accessAndLatency = new AccessAndLatency(
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder()),
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()));

        // This value should be skipped in the calculation.
        final AccessAndLatency missingLatency = new AccessAndLatency(
            Optional.empty(),
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()));

        assertEquals(
            (1.0 * 2.0) / (2.0),
            AccessAndLatency.latencyWeightedAveraged(Arrays.asList(accessAndLatency, missingLatency)),
            0.0);
    }

    @Test
    public void testLatencyWeightedAveragedWithMissingLatency() {
        // This value should be skipped in the calculation.
        final AccessAndLatency missingLatency = new AccessAndLatency(
            Optional.empty(),
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()));

        assertEquals(
            0.0,
            AccessAndLatency.latencyWeightedAveraged(Collections.singletonList(missingLatency)),
            0.0);

    }
}