package com.vmturbo.topology.processor.history.percentile;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for UtilizationCountArray.
 */
public class UtilizationCountArrayTest {
    private static final double delta = 0.001;
    private static final EntityCommodityFieldReference REF =
            new EntityCommodityFieldReference(134L,
                    CommodityType.newBuilder().setKey("efds").setType(12).build(), 4857L,
                    CommodityField.USED);
    private static final EntityCommodityFieldReference COMMODITY_WITHOUT_KEY_REF =
            new EntityCommodityFieldReference(1L, CommodityType.newBuilder().setType(2).build(), 3L,
                    CommodityField.USED);

    /**
     * Expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private long timestamp = System.currentTimeMillis();

    /**
     * Test the rank retrieval for the empty data.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testEmptyArray() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        Assert.assertEquals(0, counts.getPercentile(90));
    }

    /**
     * Test the calculation with negative rank requested.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testNegativeRank() throws HistoryCalculationException {
        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("invalid percentile rank");
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        counts.getPercentile(-50);
    }

    /**
     * Test the percentile rank 0 request.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testRankZero() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        for (int i = 0; i < 100; ++i) {
            counts.addPoint(i, 100, "", true, timestamp);
        }
        Assert.assertEquals(0, counts.getPercentile(0));
    }

    /**
     * Test the case of single usage point repeated multiple times.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSingleCount() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 1, 5);
        Assert.assertEquals(1, counts.getPercentile(20));
        Assert.assertEquals(1, counts.getPercentile(30));
        Assert.assertThat(counts.serialize(REF).getStartTimestamp(), Matchers.is(timestamp));
    }

    /**
     * Test the multiple usage points added.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testFiveCounts() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 0, 5);
        addCount(counts, 1, 4);
        addCount(counts, 2, 6);
        addCount(counts, 3, 3);
        addCount(counts, 4, 2);
        Assert.assertEquals(1, counts.getPercentile(30));
        Assert.assertEquals(2, counts.getPercentile(50));
        Assert.assertEquals(4, counts.getPercentile(95));
    }

    /**
     * Test that counts can be subtracted.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSubtractPoints() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 1, 5);
        addCount(counts, 2, 5);
        Assert.assertEquals(2, counts.getPercentile(80));
        counts.addPoint(2, 100, "", false, timestamp);
        Assert.assertEquals(2, counts.getPercentile(80));
        counts.addPoint(2, 100, "", false, timestamp);
        counts.addPoint(2, 100, "", false, timestamp);
        Assert.assertEquals(1, counts.getPercentile(80));
    }

    /**
     * Test that usage is rescaled to current capacity when point is subtracted.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSubtractPointsChangeCapacity() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        counts.addPoint(10, 200, "", true, timestamp);
        counts.addPoint(20, 100, "", true, timestamp);
        // now after rescaling we should have a point at 10 and a point at 20
        Assert.assertEquals(10, counts.getPercentile(50));
        Assert.assertEquals(20, counts.getPercentile(100));
        // removing 1st point
        counts.addPoint(10, 200, "", false, timestamp);
        // now only one point at 20 should remain
        Assert.assertEquals(0, counts.getPercentile(80));
        Assert.assertEquals(20, counts.getPercentile(100));
    }

    /**
     * Test that counts are proportionally rescaled when capacity changes.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testRescaleCapacity() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 10, 5);
        addCount(counts, 20, 5);
        Assert.assertEquals(10, counts.getPercentile(40));
        Assert.assertEquals(20, counts.getPercentile(80));
        counts.addPoint(10, 200, "", true, timestamp);
        Assert.assertEquals(5, counts.getPercentile(40));
        Assert.assertEquals(10, counts.getPercentile(80));
    }

    /**
     * Test the serialization of array in protobuf.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSerialize() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        float capacity = 72631;
        counts.addPoint(2, capacity, "", true, timestamp);
        PercentileRecord.Builder builder = counts.serialize(REF);
        Assert.assertNotNull(builder);
        PercentileRecord record = builder.setPeriod(30).build();

        Assert.assertTrue(record.hasEntityOid());
        Assert.assertTrue(record.hasCapacity());
        Assert.assertTrue(record.hasCommodityType());
        Assert.assertTrue(record.hasKey());
        Assert.assertTrue(record.hasProviderOid());

        Assert.assertEquals(REF.getEntityOid(), record.getEntityOid());
        Assert.assertEquals(capacity, record.getCapacity(), delta);
        Assert.assertEquals(REF.getCommodityType().getType(), record.getCommodityType());
        Assert.assertEquals(REF.getCommodityType().getKey(), record.getKey());
        Assert.assertEquals(REF.getProviderOid().longValue(), record.getProviderOid());
    }

    /**
     * Test the serialization of array in protobuf. Case when the commodity without key.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSerializeCommodityWithoutKey() {
        final UtilizationCountArray utilizationCountArray =
                new UtilizationCountArray(new PercentileBuckets());
        Assert.assertFalse(utilizationCountArray.serialize(COMMODITY_WITHOUT_KEY_REF).hasKey());
    }

    /**
     * Test that counts are deserialized from protobuf structure.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testDeserialize() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        addCount(builder, 5, 10);
        addCount(builder, 10, 10);
        counts.deserialize(builder.build(), "");
        Assert.assertEquals(5, counts.getPercentile(50));

        addCount(builder, 15, 10);
        addCount(builder, 20, 10);
        counts.deserialize(builder.build(), "");
        Assert.assertEquals(5, counts.getPercentile(25));
        Assert.assertEquals(10, counts.getPercentile(50));
    }

    /**
     * Test that counts array of wrong size is not deserialized.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testDeserializeIncorrectArray() throws HistoryCalculationException {
        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("serialized percentile counts array is not valid");
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).addUtilization(20).setPeriod(30);
        counts.deserialize(builder.build(), "");
    }

    /**
     * Test that counts are rescaled when deserialization is invoked with
     * a different from existing capacity.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testDeserializeCapacityChange() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        PercentileRecord.Builder builder1 = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        addCount(builder1, 5, 10);
        addCount(builder1, 10, 10);
        counts.deserialize(builder1.build(), "");
        Assert.assertEquals(5, counts.getPercentile(50));

        PercentileRecord.Builder builder2 = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(50f).setPeriod(30);
        addCount(builder2, 0, 0);
        counts.deserialize(builder2.build(), "");
        Assert.assertEquals(10, counts.getPercentile(25));
        Assert.assertEquals(10, counts.getPercentile(50));
        Assert.assertEquals(20, counts.getPercentile(75));
    }

    /**
     * Test for {@link UtilizationCountArray#copyCountsFrom(UtilizationCountArray)}
     * When the lengths of the counts arrays do match.
     *
     * @throws HistoryCalculationException when the lengths of the counts arrays do not match
     */
    @Test
    public void testCopyCountsFromArrayLengthsMath() throws HistoryCalculationException {
        final PercentileBuckets buckets = new PercentileBuckets("0,1,5,99,100");

        final UtilizationCountArray utilizationCountArray1 = new UtilizationCountArray(buckets);
        final UtilizationCountArray utilizationCountArray2 = new UtilizationCountArray(buckets);
        final List<Integer> utilization2 = Arrays.asList(1, 2, 3, 4, 5);
        utilizationCountArray2.deserialize(PercentileRecord.newBuilder()
                .setEntityOid(REF.getEntityOid())
                .setCommodityType(REF.getCommodityType().getType())
                .setKey(REF.getCommodityType().getKey())
                .setCapacity(1000F)
                .addAllUtilization(utilization2)
                .setPeriod(30)
                .build(), "");

        utilizationCountArray1.copyCountsFrom(utilizationCountArray2);
        Assert.assertEquals(utilization2,
                utilizationCountArray1.serialize(REF).getUtilizationList());
    }

    /**
     * Test for {@link UtilizationCountArray#copyCountsFrom(UtilizationCountArray)}
     * When the lengths of the counts arrays do not match.
     *
     * @throws HistoryCalculationException when the lengths of the counts arrays do not match
     */
    @Test
    public void testCopyCountsFromArrayLengthsNotMath() throws HistoryCalculationException {
        final UtilizationCountArray utilizationCountArray1 =
                new UtilizationCountArray(new PercentileBuckets("0,1,5,99,100"));
        final UtilizationCountArray utilizationCountArray2 =
                new UtilizationCountArray(new PercentileBuckets("0,1,5,95,99,100"));
        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage(
                "The internal 5 and external 6 the lengths of the counts arrays do not match");
        utilizationCountArray1.copyCountsFrom(utilizationCountArray2);
    }

    /**
     * Checks that {@link UtilizationCountArray#toString()} and {@link
     * UtilizationCountArray#toDebugString()} methods are creating expected strings in predefined
     * states:
     * <ul>
     *     <li>No points were added to {@link UtilizationCountArray} or
     *     {@link UtilizationCountArray#clear()} was called means that instance has not
     *     been initialized, so {@link UtilizationCountArray#EMPTY} string will be returned in
     *     any string representations;</li>
     *     <li>Points were added to {@link UtilizationCountArray} instance, means that instance
     *     was initialized and capacity ant counts will be displayed.</li>
     * </ul>
     *
     * @throws HistoryCalculationException in case adding of the point failed.
     */
    @Test
    public void toStringTests() throws HistoryCalculationException {
        final UtilizationCountArray utilizationCountArray = new UtilizationCountArray(new PercentileBuckets());
        checkToString(utilizationCountArray::toDebugString, UtilizationCountArray.EMPTY);
        checkToString(utilizationCountArray::toString, UtilizationCountArray.EMPTY);
        utilizationCountArray.addPoint(35, 100, null, true, timestamp);
        utilizationCountArray.addPoint(40, 100, null, true, timestamp);
        utilizationCountArray.addPoint(40, 100, null, true, timestamp);
        utilizationCountArray.addPoint(40, 100, null, true, timestamp);
        checkToString(utilizationCountArray::toString, "{capacity=100.0}");
        checkToString(utilizationCountArray::toDebugString,
                        "{capacity=100.0; counts=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}");
        utilizationCountArray.clear();
        checkToString(utilizationCountArray::toString, UtilizationCountArray.EMPTY);
        checkToString(utilizationCountArray::toDebugString, UtilizationCountArray.EMPTY);
    }

    /**
     * Ensure that incorrect capacity entries are not deserialized.
     *
     * @throws HistoryCalculationException never
     */
    @Test
    public void toDeserializeZeroCapacity() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        final int validPercent = 20;
        addCount(counts, validPercent, 10);
        Assert.assertEquals(validPercent, counts.getPercentile(100));

        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(0f).setPeriod(30);
        addCount(builder, 50, 10);
        counts.deserialize(builder.build(), "");

        Assert.assertEquals(validPercent, counts.getPercentile(100));
    }

    private void checkToString(Supplier<String> toStringSupplier,
                    final String expectedFieldsToString) {
        Assert.assertThat(toStringSupplier.get(), CoreMatchers.is(String
                        .format("%s#%s", UtilizationCountArray.class.getSimpleName(),
                                        expectedFieldsToString)));
    }

    private void addCount(UtilizationCountArray counts, int count, int quantity)
                    throws HistoryCalculationException {
        for (int i = 0; i < quantity; ++i) {
            counts.addPoint(count, 100, "", true, timestamp);
        }
    }

    private static void addCount(PercentileRecord.Builder builder, int count, int quantity) {
        if (builder.getUtilizationCount() == 0) {
            for (int i = 0; i <= 100; ++i) {
                builder.addUtilization(0);
            }
        }
        builder.setUtilization(count, builder.getUtilization(count) + quantity);
    }
}
