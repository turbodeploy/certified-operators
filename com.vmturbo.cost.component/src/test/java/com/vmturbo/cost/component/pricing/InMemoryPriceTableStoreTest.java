package com.vmturbo.cost.component.pricing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.is;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;

/**
 * Unit tests for the {@link InMemoryPriceTableStore}.
 */
public class InMemoryPriceTableStoreTest {

    private PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);

    private InMemoryPriceTableStore store = new InMemoryPriceTableStore(mergeFactory);

    @Captor
    private ArgumentCaptor<Collection<PriceTable>> mergeArgCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPutAndGet() {
        final String probeType1 = "boo";
        final String probeType2 = "hoo";
        final PriceTable priceTable1 = mockPriceTable(1);
        final PriceTable priceTable2 = mockPriceTable(2);
        store.putPriceTable(probeType1, priceTable1);
        store.putPriceTable(probeType2, priceTable2);

        final PriceTable mergedPriceTable = mockPriceTable(3);
        final PriceTableMerge merge = mock(PriceTableMerge.class);
        when(merge.merge(any())).thenReturn(mergedPriceTable);
        when(mergeFactory.newMerge()).thenReturn(merge);
        assertThat(store.getMergedPriceTable(), is(mergedPriceTable));

        verify(merge).merge(mergeArgCaptor.capture());
        assertThat(mergeArgCaptor.getValue(), containsInAnyOrder(priceTable1, priceTable2));
    }

    @Test
    public void testOverwriteOldPriceTable() {
        final PriceTable priceTable1 = mockPriceTable(1);
        final PriceTable priceTable2 = mockPriceTable(2);
        assertThat(store.putPriceTable("foo", priceTable1), is(Optional.empty()));
        assertThat(store.putPriceTable("foo", priceTable2), is(Optional.of(priceTable1)));

        final PriceTable mergedPriceTable = mockPriceTable(3);
        final PriceTableMerge merge = mock(PriceTableMerge.class);
        when(merge.merge(any())).thenReturn(mergedPriceTable);
        when(mergeFactory.newMerge()).thenReturn(merge);
        assertThat(store.getMergedPriceTable(), is(mergedPriceTable));

        verify(merge).merge(mergeArgCaptor.capture());
        assertThat(mergeArgCaptor.getValue(), containsInAnyOrder(priceTable2));
    }

    @Nonnull
    private PriceTable mockPriceTable(final long key) {
        return PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(key, OnDemandPriceTable.getDefaultInstance())
                .build();
    }
}
