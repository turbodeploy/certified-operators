package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAccessIOPS;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageLatencyMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

public class AccessAndLatencyTest {

    @Test
    public void testHasLatency() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(1.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        assertTrue(accessAndLatency.hasLatency());

        final AccessAndLatency missingLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(1.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().build().toBuilder())
        );

        assertFalse(missingLatency.hasLatency());
    }

    @Test
    public void testIopsWeight() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        assertEquals(2.0, accessAndLatency.iopsWeight(), 0.0);

        final AccessAndLatency missingAccess = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        // When IOPS is not present, return 1.0 so that we can calculate a uniform average.
        assertEquals(1.0, missingAccess.iopsWeight(), 0.0);
    }

    @Test
    public void testWeightedLatencyPresent() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        assertEquals(1.0 * 2.0, accessAndLatency.weightedLatency().get(), 0.0);

        final AccessAndLatency otherAccessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()), 
            Optional.of(storageLatencyMillis().used(10.0).build().toBuilder())
        );

        assertEquals(10.0 * 0.5, otherAccessAndLatency.weightedLatency().get(), 0.0);
    }

    @Test
    public void testWeightedLatencyAbsent() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()), Optional.empty()
        );

        assertFalse(accessAndLatency.weightedLatency().isPresent());
    }

    @Test
    public void testLatencyWeightedAveraged() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        final AccessAndLatency otherAccessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(10.0).build().toBuilder())
        );
        
        assertEquals(
            (1.0 * 2.0 + 10.0 * 0.5) / (2.0 + 0.5),
            AccessAndLatency.latencyWeightedAveraged(Stream.of(accessAndLatency, otherAccessAndLatency)),
            0.0);
    }

    @Test
    public void testLatencyWeightedAveragedWithMissingLatencyNoValues() {
        final AccessAndLatency accessAndLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(2.0).build().toBuilder()),
            Optional.of(storageLatencyMillis().used(1.0).build().toBuilder())
        );

        // This value should be skipped in the calculation.
        final AccessAndLatency missingLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()), Optional.empty()
        );

        assertEquals(
            (1.0 * 2.0) / (2.0),
            AccessAndLatency.latencyWeightedAveraged(Stream.of(accessAndLatency, missingLatency)),
            0.0);
    }

    @Test
    public void testLatencyWeightedAveragedWithMissingLatency() {
        // This value should be skipped in the calculation.
        final AccessAndLatency missingLatency = AccessAndLatency.accessAndLatencyFromCommodityDtos(
            Optional.of(storageAccessIOPS().used(0.5).build().toBuilder()), Optional.empty()
        );

        assertEquals(
            0.0,
            AccessAndLatency.latencyWeightedAveraged(Stream.of(missingLatency)),
            0.0);

    }
}