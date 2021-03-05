package com.vmturbo.extractor.topology.fetcher;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;

/**
 * Unit tests for {@link TopDownCostFetcher}.
 */
public class TopDownCostFetcherTest {

    /**
     * Test the fetch method.
     */
    @Test
    public void testFetch() {
        final AtomicReference<TopDownCostData> ref = new AtomicReference<>();
        final TopDownCostFetcherFactory fetcherFactory = mock(TopDownCostFetcherFactory.class);
        final TopDownCostData data = mock(TopDownCostData.class);
        when(fetcherFactory.getMostRecentData()).thenReturn(data);

        final TopDownCostFetcher fetcher = new TopDownCostFetcher(new MultiStageTimer(null), ref::set, fetcherFactory);
        final TopDownCostData fetchedData = fetcher.fetch();
        assertThat(fetchedData, is(data));
    }

}