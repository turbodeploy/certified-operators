package com.vmturbo.topology.processor.history.percentile;

import java.util.function.Supplier;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
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
    private UtilizationCountArray countsArray;

    /**
     * Initializes all resources required for tests.
     */
    @Before
    public void before() {
        countsArray = new UtilizationCountArray(new PercentileBuckets());
    }

    /**
     * Test the rank retrieval for the empty data.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testEmptyArray() throws HistoryCalculationException {
        final Integer percentile = countsArray.getPercentile(90, REF);
        Assert.assertNull(percentile);
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
        countsArray.getPercentile(-50, REF);
    }

    /**
     * Test the percentile rank 0 request.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testRankZero() throws HistoryCalculationException {
        for (int i = 0; i < 100; ++i) {
            countsArray.addPoint(i, 100, "", timestamp);
        }
        verifyPercentile(0, countsArray.getPercentile(0, REF));
    }

    /**
     * Test that percentile value is correct when a single count is recorded.
     *
     * @throws HistoryCalculationException if getPercentile throws HistoryCalculationException.
     */
    @Test
    public void testCountSinglePoint() throws HistoryCalculationException {
        countsArray.addPoint(3, 4, "", timestamp);
        verifyPercentile(75, countsArray.getPercentile(95, REF));
    }

    private void verifyPercentile(int expected, Integer actual) {
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, (int)actual);
    }

    /**
     * Test the case of single usage point repeated multiple times.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSingleCount() throws HistoryCalculationException {
        addCount(countsArray, 1, 5);
        verifyPercentile(1, countsArray.getPercentile(20, REF));
        verifyPercentile(1, countsArray.getPercentile(30, REF));
        Assert.assertThat(countsArray.serialize(REF).getStartTimestamp(), Matchers.is(timestamp));
    }

    /**
     * Test the multiple usage points added.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testFiveCounts() throws HistoryCalculationException {
        addCount(countsArray, 0, 5);
        addCount(countsArray, 1, 4);
        addCount(countsArray, 2, 6);
        addCount(countsArray, 3, 3);
        addCount(countsArray, 4, 2);
        verifyPercentile(1, countsArray.getPercentile(30, REF));
        verifyPercentile(2, countsArray.getPercentile(50, REF));
        verifyPercentile(4, countsArray.getPercentile(95, REF));
    }

    /**
     * Test that counts can be subtracted.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSubtractPoints() throws HistoryCalculationException {
        addCount(countsArray, 1, 5);
        addCount(countsArray, 2, 5);
        verifyPercentile(2, countsArray.getPercentile(80, REF));
        countsArray.removePoint(2, 1, 100, timestamp, "");
        verifyPercentile(2, countsArray.getPercentile(80, REF));
        countsArray.removePoint(2, 2, 100, timestamp, "");
        verifyPercentile(2, countsArray.getPercentile(80, REF));
    }

    /**
     * Test capacity is empty after subtraction.
     * @throws HistoryCalculationException when exception
     */
    @Test
    public void testEmptyCapacity() throws HistoryCalculationException {
        addCount(countsArray, 1, 2);
        countsArray.addPoint(1, 110, "", timestamp - 2);
        countsArray.addPoint(1, 120, "", timestamp - 1);
        countsArray.removePoint(1, 2, 50, timestamp + 1, "");
        Assert.assertFalse(countsArray.isEmpty());
        addCount(countsArray, 1, 3);
        Assert.assertTrue(countsArray.isEmptyOrOutdated(timestamp + 7776000001L)); //90 days in milliseconds
    }

    /**
     * Test that usage is rescaled to current capacity when point is subtracted.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSubtractPointsChangeCapacity() throws HistoryCalculationException {
        countsArray.addPoint(10, 200, "", timestamp);
        countsArray.addPoint(20, 100, "", timestamp);
        // now after rescaling we should have a point at 10 and a point at 20
        verifyPercentile(10, countsArray.getPercentile(50, REF));
        verifyPercentile(20, countsArray.getPercentile(100, REF));
        // removing 1st point
        countsArray.removePoint(5, 1, 200, timestamp, "");
        // now only one point at 20 should remain
        verifyPercentile(20, countsArray.getPercentile(80, REF));
        verifyPercentile(20, countsArray.getPercentile(100, REF));
    }

    /**
     * Test that counts are proportionally rescaled when capacity changes.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testRescaleCapacity() throws HistoryCalculationException {
        addCount(countsArray, 10, 5);
        addCount(countsArray, 20, 5);
        verifyPercentile(10, countsArray.getPercentile(40, REF));
        verifyPercentile(20, countsArray.getPercentile(80, REF));
        countsArray.addPoint(10, 200, "", timestamp);
        verifyPercentile(5, countsArray.getPercentile(40, REF));
        verifyPercentile(10, countsArray.getPercentile(80, REF));
    }

    /**
     * Test the serialization of array in protobuf.
     */
    @Test
    public void testSerialize() {
        PercentileRecord.Builder empty = countsArray.serialize(REF);
        Assert.assertNull(empty);

        float capacity = 72631;
        countsArray.addPoint(2, capacity, "", timestamp);
        PercentileRecord.Builder builder = countsArray.serialize(REF);
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
     */
    @Test
    public void testSerializeCommodityWithoutKey() {
        final UtilizationCountArray utilizationCountArray = countsArray;
        utilizationCountArray.addPoint(1.0F, 1.0F, "", timestamp);
        Assert.assertFalse(utilizationCountArray.serialize(COMMODITY_WITHOUT_KEY_REF).hasKey());
    }

    /**
     * Test that counts are deserialized from protobuf structure.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testDeserialize() throws HistoryCalculationException {
        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        addCount(builder, 5, 10);
        addCount(builder, 10, 10);
        countsArray.deserialize(builder.build(), "");
        verifyPercentile(5, countsArray.getPercentile(50, REF));

        addCount(builder, 15, 10);
        addCount(builder, 20, 10);
        countsArray.deserialize(builder.build(), "");
        verifyPercentile(5, countsArray.getPercentile(25, REF));
        verifyPercentile(10, countsArray.getPercentile(50, REF));
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
        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).addUtilization(20).setPeriod(30);
        countsArray.deserialize(builder.build(), "");
    }

    /**
     * Test that counts are rescaled when deserialization is invoked with
     * a different from existing capacity.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testDeserializeCapacityChange() throws HistoryCalculationException {
        PercentileRecord.Builder builder1 = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        addCount(builder1, 5, 10);
        addCount(builder1, 10, 10);
        countsArray.deserialize(builder1.build(), "");
        verifyPercentile(5, countsArray.getPercentile(50, REF));

        PercentileRecord.Builder builder2 = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(50f).setPeriod(30);
        addCount(builder2, 0, 0);
        countsArray.deserialize(builder2.build(), "");
        verifyPercentile(10, countsArray.getPercentile(25, REF));
        verifyPercentile(10, countsArray.getPercentile(50, REF));
        verifyPercentile(20, countsArray.getPercentile(75, REF));
    }

    /**
     * Test isMinHistoryDataAvailable.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testIsMinHistoryDataAvailable() throws HistoryCalculationException {
        // UThu Jul 16 2020 04:00:00
        long startTime = 1594872000000L;
        countsArray.addPoint(10, 10, "", startTime);

        // UTC Wed Jul 17 2020 00:00:00
        long newStartTime = 1594944000000L;
        PercentileRecord.Builder builder1 = PercentileRecord.newBuilder().setEntityOid(12)
            .setCommodityType(53).setCapacity(10).setPeriod(30).setStartTimestamp(newStartTime);
        addCount(builder1, 5, 10);
        countsArray.deserialize(builder1.build(), "");

        // UTC Fri Jul 17 2020 15:00:00
        long currentTimestamp1 = 1594998000000L;
        Assert.assertTrue(countsArray.isMinHistoryDataAvailable(currentTimestamp1, "", 1));

        // UTC Fri Jul 17 2020 03:00:00
        long currentTimestamp2 = 1594954800000L;
        Assert.assertFalse(countsArray.isMinHistoryDataAvailable(currentTimestamp2, "", 1));
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
     */
    @Test
    public void toStringTests() {
        final UtilizationCountArray utilizationCountArray = countsArray;
        checkToString(utilizationCountArray::toDebugString, UtilizationCountArray.EMPTY);
        checkToString(utilizationCountArray::toString, UtilizationCountArray.EMPTY);
        utilizationCountArray.addPoint(35, 100, null, timestamp);
        utilizationCountArray.addPoint(40, 100, null, timestamp);
        utilizationCountArray.addPoint(40, 100, null, timestamp);
        utilizationCountArray.addPoint(40, 100, null, timestamp);
        checkToString(utilizationCountArray::toString, "{capacity=100.0}");
        checkToString(utilizationCountArray::toDebugString,
                        "{capacity=[{\"timestamp\": " + timestamp + ", \"newCapacity\": "
                            + "\"100.0\"}]; counts=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}");
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
        final int validPercent = 20;
        addCount(countsArray, validPercent, 10);
        verifyPercentile(validPercent, countsArray.getPercentile(100, REF));

        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(0f).setPeriod(30);
        addCount(builder, 50, 10);
        countsArray.deserialize(builder.build(), "");

        verifyPercentile(validPercent, countsArray.getPercentile(100, REF));
    }

    /**
     * Simulates the case rank index should exceed {@link Integer#MAX_VALUE}.
     *
     * @throws HistoryCalculationException in case calculation failed
     */
    @Test
    public void checkGetPercentileOverflowRankIndex() throws HistoryCalculationException {
        countsArray.counts[100] = Integer.MAX_VALUE / 94;
        countsArray.counts[1] = 0;
        countsArray.counts[2] = 15;
        final Integer result = countsArray.getPercentile(95F, REF);
        Assert.assertThat(result, CoreMatchers.is(100));
    }

    /**
     * Simulates the case that leads to throwing of {@link ArrayIndexOutOfBoundsException}.
     *
     * @throws HistoryCalculationException in case calculation failed
     */
    @Test
    public void checkGetPercentileIndexOutOfBoundException() throws HistoryCalculationException {
        countsArray.counts[0] = Integer.MAX_VALUE + 94;
        countsArray.counts[1] = 0;
        countsArray.counts[2] = 15;
        final Integer result = countsArray.getPercentile(95F, REF);
        Assert.assertThat(result, CoreMatchers.nullValue());
    }

    /**
     * Check calculation of 100 for counts that has counts[100] > 0 will lead to 100 percentile
     * usage value.
     *
     * @throws HistoryCalculationException in case calculation failed
     */
    @Test
    public void checkGetPercentileCauseIndexOutOfBoundWithoutOverflow() throws HistoryCalculationException {
        countsArray.counts = new int[] {26, 1, 6, 6, 4, 41, 2275, 8175, 17475, 27506, 35187, 11948, 77253, 43033, 43014, 68719, 14318, 15352, 64299, 36205, 33744, 31459, 29129, 27150, 25004, 9960, 34436, 19782, 18419, 16814, 15391, 14616, 24342, 6661, 6430, 14221, 5780, 5456, 5318, 15285, 6689, 6197, 5727, 5373, 4875, 4509, 4245, 3885, 3616, 3276, 2946, 2859, 2593, 4068, 1773, 1577, 1479, 1455, 1299, 1889, 1438, 1330, 1291, 1156, 1041, 1031, 983, 921, 898, 841, 754, 729, 675, 824, 596, 526, 465, 507, 542, 445, 504, 442, 381, 384, 371, 343, 317, 638, 275, 297, 232, 251, 260, 250, 222, 228, 204, 186, 164, 177, 354};
        final Integer result = countsArray.getPercentile(100F, REF);
        Assert.assertThat(result, CoreMatchers.is(100));
    }

    /**
     * Check calculation of 100 percentile which has 0 values in the counts after some utilization
     * value. In reality this use-case means that e.g. VM CPU utilization has not exceed 6%
     * utilization for the whole observation period. In this case 100 percentile should show 6%.
     *
     * @throws HistoryCalculationException in case calculation failed
     */
    @Test
    public void checkGetPercentileMessage() throws HistoryCalculationException {
        countsArray.counts[1] = 51710;
        countsArray.counts[2] = 680879;
        countsArray.counts[3] = 185429;
        countsArray.counts[4] = 2696;
        countsArray.counts[5] = 328;
        countsArray.counts[6] = 1;
        Assert.assertThat(countsArray.getPercentile(100F, REF), CoreMatchers.is(6));
        Assert.assertThat(countsArray.getPercentile(99.999999F, REF), CoreMatchers.is(6));
        Assert.assertThat(countsArray.getPercentile(99.999F, REF), CoreMatchers.is(5));
        Assert.assertThat(countsArray.getPercentile(75F, REF), CoreMatchers.is(2));
    }

    private static void checkToString(Supplier<String> toStringSupplier,
                    final String expectedFieldsToString) {
        Assert.assertThat(toStringSupplier.get(), CoreMatchers.is(String
                        .format("%s#%s", UtilizationCountArray.class.getSimpleName(),
                                        expectedFieldsToString)));
    }

    private void addCount(UtilizationCountArray counts, int count, int quantity) {
        for (int i = 0; i < quantity; ++i) {
            counts.addPoint(count, 100, "", timestamp);
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
