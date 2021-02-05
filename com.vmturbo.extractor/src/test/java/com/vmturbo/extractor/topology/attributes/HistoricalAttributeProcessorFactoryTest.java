package com.vmturbo.extractor.topology.attributes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for {@link HistoricalAttributeProcessorFactory}.
 */
public class HistoricalAttributeProcessorFactoryTest {

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final long forceUpdateIntervalMs = 10;

    private final DbEndpoint endpoint = mock(DbEndpoint.class);

    /**
     * Test creating a processor, and the force update interval.
     */
    @Test
    public void testNewProcessor() {
        HistoricalAttributeProcessorFactory<Integer> factory = spy(new HistoricalAttributeProcessorFactory<Integer>(clock, forceUpdateIntervalMs,
                TimeUnit.MILLISECONDS) {
            @Override
            HistoricalAttributeProcessor<Integer> newProcessorInternal(DbEndpoint dbEndpoint,
                    Long2LongMap hashByEntityIdMap, Consumer<Long2LongMap> newHashesConsumer) {
                // Add an element to the map.
                Long2LongMap newMap = new Long2LongOpenHashMap();
                newMap.put(hashByEntityIdMap.size() + 1, 1L);
                newHashesConsumer.accept(newMap);
                return null;
            }
        });

        ArgumentCaptor<Long2LongMap> hashMapCaptor = ArgumentCaptor.forClass(Long2LongMap.class);

        factory.newProcessor(endpoint);
        verify(factory).newProcessorInternal(eq(endpoint), hashMapCaptor.capture(), any());
        Long2LongMap map = hashMapCaptor.getValue();
        // Map has one element, put there by the first invocation
        assertThat(map.size(), is(1));


        factory.newProcessor(endpoint);
        // Map has two elements, put there by the first and second invocation.
        assertThat(map.size(), is(2));
        clock.addTime(forceUpdateIntervalMs, ChronoUnit.MILLIS);

        factory.newProcessor(endpoint);
        // Map got cleared during the first invocation, and then populated with another element
        assertThat(map.size(), is(1));
    }
}