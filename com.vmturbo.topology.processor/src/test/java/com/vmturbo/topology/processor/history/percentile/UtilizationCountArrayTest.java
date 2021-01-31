package com.vmturbo.topology.processor.history.percentile;

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
        final Integer percentile = counts.getPercentile(90);
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
            counts.addPoint(i, 100, "", timestamp);
        }
        verifyPercentile(0, counts.getPercentile(0));
    }

    /**
     * Test that percentile value is correct when a single count is recorded.
     *
     * @throws HistoryCalculationException if getPercentile throws HistoryCalculationException.
     */
    @Test
    public void testCountSinglePoint() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        counts.addPoint(3, 4, "", timestamp);
        verifyPercentile(75, counts.getPercentile(95));
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
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 1, 5);
        verifyPercentile(1, counts.getPercentile(20));
        verifyPercentile(1, counts.getPercentile(30));
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
        verifyPercentile(1, counts.getPercentile(30));
        verifyPercentile(2, counts.getPercentile(50));
        verifyPercentile(4, counts.getPercentile(95));
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
        verifyPercentile(2, counts.getPercentile(80));
        counts.removePoint(2, 1, 100, timestamp, "");
        verifyPercentile(2, counts.getPercentile(80));
        counts.removePoint(2, 2, 100, timestamp, "");
        verifyPercentile(2, counts.getPercentile(80));
    }

    /**
     * Test capacity is empty after subtraction.
     * @throws HistoryCalculationException when exception
     */
    @Test
    public void testEmptyCapacity() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        addCount(counts, 1, 2);
        counts.addPoint(1, 110, "", timestamp - 2);
        counts.addPoint(1, 120, "", timestamp - 1);
        counts.removePoint(1, 2, 50, timestamp + 1, "");
        Assert.assertFalse(counts.isEmpty());
        addCount(counts, 1, 3);
        Assert.assertTrue(counts.isEmptyOrOutdated(timestamp + 7776000001L)); //90 days in milliseconds
    }

    /**
     * Test that usage is rescaled to current capacity when point is subtracted.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSubtractPointsChangeCapacity() throws HistoryCalculationException {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        counts.addPoint(10, 200, "", timestamp);
        counts.addPoint(20, 100, "", timestamp);
        // now after rescaling we should have a point at 10 and a point at 20
        verifyPercentile(10, counts.getPercentile(50));
        verifyPercentile(20, counts.getPercentile(100));
        // removing 1st point
        counts.removePoint(5, 1, 200, timestamp, "");
        // now only one point at 20 should remain
        verifyPercentile(20, counts.getPercentile(80));
        verifyPercentile(20, counts.getPercentile(100));
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
        verifyPercentile(10, counts.getPercentile(40));
        verifyPercentile(20, counts.getPercentile(80));
        counts.addPoint(10, 200, "", timestamp);
        verifyPercentile(5, counts.getPercentile(40));
        verifyPercentile(10, counts.getPercentile(80));
    }

    /**
     * Test the serialization of array in protobuf.
     */
    @Test
    public void testSerialize() {
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());

        PercentileRecord.Builder empty = counts.serialize(REF);
        Assert.assertNull(empty);

        float capacity = 72631;
        counts.addPoint(2, capacity, "", timestamp);
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
     */
    @Test
    public void testSerializeCommodityWithoutKey() {
        final UtilizationCountArray utilizationCountArray =
                new UtilizationCountArray(new PercentileBuckets());
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
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f).setPeriod(30);
        addCount(builder, 5, 10);
        addCount(builder, 10, 10);
        counts.deserialize(builder.build(), "");
        verifyPercentile(5, counts.getPercentile(50));

        addCount(builder, 15, 10);
        addCount(builder, 20, 10);
        counts.deserialize(builder.build(), "");
        verifyPercentile(5, counts.getPercentile(25));
        verifyPercentile(10, counts.getPercentile(50));
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
        verifyPercentile(5, counts.getPercentile(50));

        PercentileRecord.Builder builder2 = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(50f).setPeriod(30);
        addCount(builder2, 0, 0);
        counts.deserialize(builder2.build(), "");
        verifyPercentile(10, counts.getPercentile(25));
        verifyPercentile(10, counts.getPercentile(50));
        verifyPercentile(20, counts.getPercentile(75));
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
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        counts.addPoint(10, 10, "", startTime);

        // UTC Wed Jul 17 2020 00:00:00
        long newStartTime = 1594944000000L;
        PercentileRecord.Builder builder1 = PercentileRecord.newBuilder().setEntityOid(12)
            .setCommodityType(53).setCapacity(10).setPeriod(30).setStartTimestamp(newStartTime);
        addCount(builder1, 5, 10);
        counts.deserialize(builder1.build(), "");

        // UTC Fri Jul 17 2020 15:00:00
        long currentTimestamp1 = 1594998000000L;
        Assert.assertTrue(counts.isMinHistoryDataAvailable(currentTimestamp1, "", 1));

        // UTC Fri Jul 17 2020 03:00:00
        long currentTimestamp2 = 1594954800000L;
        Assert.assertFalse(counts.isMinHistoryDataAvailable(currentTimestamp2, "", 1));
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
        final UtilizationCountArray utilizationCountArray = new UtilizationCountArray(new PercentileBuckets());
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
        UtilizationCountArray counts = new UtilizationCountArray(new PercentileBuckets());
        final int validPercent = 20;
        addCount(counts, validPercent, 10);
        verifyPercentile(validPercent, counts.getPercentile(100));

        PercentileRecord.Builder builder = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(0f).setPeriod(30);
        addCount(builder, 50, 10);
        counts.deserialize(builder.build(), "");

        verifyPercentile(validPercent, counts.getPercentile(100));
    }

    private void checkToString(Supplier<String> toStringSupplier,
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
