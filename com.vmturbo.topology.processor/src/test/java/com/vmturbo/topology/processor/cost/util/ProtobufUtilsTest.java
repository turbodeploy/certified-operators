package com.vmturbo.topology.processor.cost.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import com.google.protobuf.Descriptors.Descriptor;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket.BilledCostBucketKey;

/**
 * Test class for {@link ProtobufUtils}.
 */
public class ProtobufUtilsTest {

    /**
     * Test for {@link ProtobufUtils#getMaximumMessageSizeExcludingRepeatedFields(Descriptor)}.
     */
    @Test
    public void testMaxSizeOfMessageWithVarints() {
        // Bigger values take up more space
        final BilledCostBucket largeBucketWithNoRepeatedFields = BilledCostBucket.newBuilder()
                .setSampleTsUtc(Long.MAX_VALUE)
                .setBucketKey(BilledCostBucketKey.newBuilder()
                        .setCloudServiceOid(Long.MAX_VALUE)
                        .setRegionOid(Long.MAX_VALUE)
                        .setAccountOid(Long.MAX_VALUE)
                        .build())
                .build();

        final int maxComputedBucketSize =
                ProtobufUtils.getMaximumMessageSizeExcludingRepeatedFields(
                        BilledCostBucket.getDescriptor());

        assertThat(maxComputedBucketSize,
                greaterThanOrEqualTo(largeBucketWithNoRepeatedFields.getSerializedSize()));
    }
}