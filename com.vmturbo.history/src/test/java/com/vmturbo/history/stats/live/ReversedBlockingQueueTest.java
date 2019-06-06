package com.vmturbo.history.stats.live;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.history.stats.ReversedBlockingQueue;

/**
 * Checks that  {@link ReversedBlockingQueue} is working as expected.
 */
public class ReversedBlockingQueueTest {

    /**
     * Checks that {@link ReversedBlockingQueue} properly adds and removes element from it.
     *
     * @throws InterruptedException in thread will be interrupted during tests.
     */
    @Test
    public void checkReversedBlockingQueue() throws InterruptedException {
        final ReversedBlockingQueue<Integer> objects = new ReversedBlockingQueue<>();
        objects.addAll(ImmutableList.of(1, 2, 3, 4, 5, 6));
        Assert.assertThat(objects.poll(), CoreMatchers.is(6));
        objects.add(6);
        Assert.assertThat(objects.poll(30, TimeUnit.SECONDS), CoreMatchers.is(6));
        objects.offer(6);
        Assert.assertThat(objects.take(), CoreMatchers.is(6));
        objects.offer(6, 30, TimeUnit.SECONDS);
        Assert.assertThat(objects.take(), CoreMatchers.is(6));
        objects.put(6);
        Assert.assertThat(objects.peek(), CoreMatchers.is(6));
        Assert.assertThat(objects.peek(), CoreMatchers.is(6));
    }

}
