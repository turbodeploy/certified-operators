package com.vmturbo.components.api.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.AbstractMessage;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for MulticastNotificationReceiverTest.
 */
public class MulticastNotificationReceiverTest {

    private MulticastNotificationReceiver<AbstractMessage, TestListener> receiver;
    private ExecutorService executorService;

    /**
     * Verify that multiple listeners will all process messages in the proper order.
     *
     * @throws InterruptedException if the thread was interrupted.
     */
    @Test
    public void testMultipleListeners() throws InterruptedException {
        executorService = Executors.newFixedThreadPool(10,
                new ThreadFactoryBuilder().setNameFormat("pooled-thread-%d").build());
        receiver = new MulticastNotificationReceiver(executorService, 0);

        // we'll start off by submitting a blocking task to the thread pool.
        CountDownLatch startingGun = new CountDownLatch(1);

        Runnable waitForStart = () -> {
            System.out.println(Thread.currentThread().getName() + ": Waiting.");
            try {
                startingGun.await();
            } catch (InterruptedException ie) {
            }
            System.out.println(Thread.currentThread().getName() + ": Ready to go.");
        };
        executorService.submit(waitForStart);
        executorService.submit(waitForStart);

        // add two listeners
        TestListener listenerA = new TestListener(0);
        TestListener listenerB = new TestListener(1);
        receiver.addListener(listenerA);
        receiver.addListener(listenerB);

        // queue up some messages on a separate thread.
        int numMessages = 10;
        CountDownLatch waitTillDone = new CountDownLatch(2 * numMessages);
        Thread generateMessages = new Thread(() -> {
            for (int x = 1; x <= numMessages; x++) {
                final int msgNumber = x;
                receiver.invokeListeners( listener -> listener.invoke(msgNumber, waitTillDone));
            }
        });
        generateMessages.start();

        // unblock the executor threads
        System.out.println("Go!");
        startingGun.countDown();

        // wait until all messages have been processed.
        waitTillDone.await();

        // verify that all messages were processed in order.
        Assert.assertEquals(0, listenerA.errorCount + listenerB.errorCount);
    }

    /**
     * A test listener class.
     */
    private static class TestListener {
        private final int id;

        // used to detect out-of-order processing
        private int lastProcessedMessage = 0;
        private int errorCount = 0;

        TestListener(int id) {
            this.id = id;
        }

        public synchronized void invoke(int msgNumber, CountDownLatch waitTillDone) {
            // if we detect a case of processing out of order, log it and throw an error
            if (msgNumber < lastProcessedMessage) {
                System.out.println("ERROR -- processing " + msgNumber + " after " + lastProcessedMessage);
                errorCount += 1;
            }
            lastProcessedMessage = msgNumber;
            System.out.println("Listener " + id + " on thread " + Thread.currentThread().getName() + " processing msg " + msgNumber);
            waitTillDone.countDown();
        }
    }
}
