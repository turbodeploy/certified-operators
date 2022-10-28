package com.vmturbo.topology.processor.cost.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket.Builder;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint;
import com.vmturbo.topology.processor.cost.util.MessageChunker.OversizedElementException;

/**
 * Test class for {@link MessageChunker}.
 */
public class MessageChunkerTest {

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test
    public void testMultipleItemsPerChunk() throws OversizedElementException {

        // Typically 4 bytes in size
        final BilledCostItem item = BilledCostItem.newBuilder().setEntityId(4444L).build();

        final int numberOfItems = 64;

        // So this should be 64 * 4 = 256 bytes
        final Iterable<BilledCostItem> items =
                Iterables.limit(Iterables.cycle(item), numberOfItems);

        // Non-packed fields have up to 10 bytes of padding per entry, so a chunk should fit 2 items
        final int chunkSize = 32;

        final List<BilledCostBucket> chunks =
                MessageChunker.chunkMessages(items, BilledCostBucket::newBuilder,
                                Builder::addCostItems, false, chunkSize)
                        .stream()
                        .map(Builder::build)
                        .collect(Collectors.toList());

        assertEquals(32, chunks.size());

        for (final BilledCostBucket chunk : chunks) {
            assertTrue(chunk.getSerializedSize() < chunkSize);
        }

        assertEquals(numberOfItems,
                chunks.stream().map(BilledCostBucket::getCostItemsList).mapToInt(List::size).sum());
    }

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test(expected = OversizedElementException.class)
    public void testOversizedElement() throws OversizedElementException {

        // Typically 4 bytes in size
        final BilledCostItem item = BilledCostItem.newBuilder().setEntityId(4444L).build();
        // Typically 36 bytes in size
        final BilledCostItem largerItem = BilledCostItem.newBuilder()
                .setEntityId(4444L)
                .setEntityType(3)
                .setCloudServiceId(3822345473L)
                .setRegionId(23234487L)
                .setCostTagGroupId(92323484848L)
                .setProviderId(99234999999L)
                .build();

        final int numberOfItems = 64;

        final Iterable<BilledCostItem> items =
                Iterables.limit(Iterables.cycle(item, largerItem), numberOfItems);

        // Set the chunk size 1 byte too low
        final int chunkSize = largerItem.getSerializedSize() + ProtobufUtils.MAX_VARINT_SIZE - 1;

        MessageChunker.chunkMessages(items, BilledCostBucket::newBuilder, Builder::addCostItems,
                false, chunkSize);
    }

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test
    public void testNoInputs() throws OversizedElementException {
        final List<BilledCostBucket> chunks =
                MessageChunker.chunkMessages(ImmutableList.<BilledCostItem>of(),
                                BilledCostBucket::newBuilder, Builder::addCostItems, false, 512)
                        .stream()
                        .map(Builder::build)
                        .collect(Collectors.toList());

        assertEquals(0, chunks.size());
    }

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test
    public void testOneItemPerChunk() throws OversizedElementException {

        // Typically 4 bytes in size
        final BilledCostItem item = BilledCostItem.newBuilder().setEntityId(4444L).build();
        final int numberOfItems = 64;

        // So this should be 64 * 4 = 256 bytes
        final Iterable<BilledCostItem> items =
                Iterables.limit(Iterables.cycle(item), numberOfItems);

        // Non-packed fields have up to 10 bytes of padding per entry, so a chunk should fit 1 item
        final int chunkSize = 14;

        final List<BilledCostBucket> chunks =
                MessageChunker.chunkMessages(items, BilledCostBucket::newBuilder,
                                Builder::addCostItems, false, chunkSize)
                        .stream()
                        .map(Builder::build)
                        .collect(Collectors.toList());

        assertEquals(numberOfItems, chunks.size());

        for (final BilledCostBucket chunk : chunks) {
            assertTrue(chunk.getSerializedSize() < chunkSize);
        }

        assertEquals(numberOfItems,
                chunks.stream().map(BilledCostBucket::getCostItemsList).mapToInt(List::size).sum());
    }

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test
    public void testMultipleStringsPerChunk() throws OversizedElementException {

        // Typically 4 bytes in size
        final CloudBillingDataPoint item =
                CloudBillingDataPoint.newBuilder().setServiceProviderId("A").build();

        final int numberOfItems = 64;

        // So this should be 64 * 4 = 256 bytes
        final Iterable<CloudBillingDataPoint> items =
                Iterables.limit(Iterables.cycle(item), numberOfItems);

        // Non-packed fields have up to 10 bytes of padding per entry, so a chunk should fit 2 items
        final int chunkSize = 32;

        final List<CloudBillingBucket> chunks =
                MessageChunker.chunkMessages(items, CloudBillingBucket::newBuilder,
                                CloudBillingBucket.Builder::addSamples, false, chunkSize)
                        .stream()
                        .map(CloudBillingBucket.Builder::build)
                        .collect(Collectors.toList());

        assertEquals(32, chunks.size());

        for (final CloudBillingBucket chunk : chunks) {
            assertTrue(chunk.getSerializedSize() < chunkSize);
        }

        assertEquals(numberOfItems,
                chunks.stream().map(CloudBillingBucket::getSamplesList).mapToInt(List::size).sum());
    }

    /**
     * Test for {@link MessageChunker#chunkMessages(Iterable, Supplier, BiFunction, boolean, long)}.
     *
     * @throws OversizedElementException on error
     */
    @Test
    public void testNonProtobufOutput() throws OversizedElementException {

        // Typically 4 bytes in size
        final BilledCostItem item = BilledCostItem.newBuilder().setEntityId(4444L).build();

        final int numberOfItems = 64;

        // So this should be 64 * 4 = 256 bytes
        final Iterable<BilledCostItem> items =
                Iterables.limit(Iterables.cycle(item), numberOfItems);

        // Non-packed fields have up to 10 bytes of padding per entry, so a chunk should fit 3 items
        final int chunkSize = 48;

        final List<ArrayList<BilledCostItem>> chunks =
                MessageChunker.chunkMessages(items, ArrayList::new, (list, message) -> {
                    list.add(message);
                    return list;
                }, false, chunkSize);

        assertEquals(22, chunks.size());

        assertEquals(numberOfItems, chunks.stream().mapToInt(List::size).sum());
    }
}