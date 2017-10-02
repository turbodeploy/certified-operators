package com.vmturbo.proactivesupport.communications;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;

import com.vmturbo.proactivesupport.DataMetric;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricLOB;
import com.vmturbo.proactivesupport.bridge.TCPAggregatorBridge;
import static com.vmturbo.proactivesupport.communications.AbstractChannel.SO_TIMEOUT_PROPERTY;

/**
 * The {@link EndToEndTestsIT} tests end-to-end scenarios.
 */
public class EndToEndTestsIT {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private File path;

    private final int KB = 1024;
    private final int MB = 1024 * 1024;
    private final int GB = 1024 * 1024 * 1024;

    @Before
    public void setup() throws IOException {
        // Use 3 seconds for tests here.
        System.setProperty(SO_TIMEOUT_PROPERTY, "3000");
        path = tempFolder.newFolder();
    }

    @Test
    public void testSimple() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        Thread serverThread;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                counter.getAndIncrement();
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                counter.getAndIncrement();
                Assert.assertEquals("data", new String(data));
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
                counter.getAndIncrement();
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendId("start");
            channel.sendNext("data".getBytes(), false);
            channel.sendNext("data".getBytes(), true);
            channel.close();
            while (counter.get() != 3) {
                Thread.sleep(100L);
            }
            Thread.sleep(3100L);
            Assert.assertEquals(3, counter.get());
        }
        serverThread.join();
    }

    @Test
    public void testPerformance() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        Thread serverThread;
        byte[] data = new byte[GB];
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                counter.getAndIncrement();
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                counter.getAndIncrement();
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            long start = System.currentTimeMillis();
            try (ClientChannel channel = new ClientChannel("localhost", 60000)) {
                channel.send("start", new ByteArrayInputStream(data));
            }
            int loops = (int)Math.ceil(data.length / (double)MessageFrame.MAX_MSG_LENGTH);
            long diff = System.currentTimeMillis() - start;
            System.out.println("Elapsed time: " + diff + "ms.");
            double rate = (double)data.length / (double)diff * 1000.;
            rate /= MB;
            System.out.printf("Rate: %.2f MB/s\n", rate);
            while (counter.get() < loops) {
                Thread.sleep(100L);
            }
            Assert.assertEquals(loops + 1, counter.get());
        }
        serverThread.join();
    }

    @Test
    public void testPerformanceFileIO() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        Thread serverThread;
        File f = new File(path.getCanonicalPath() + "/test");
        byte[] data = new byte[KB];
        Random rnd = new Random();
        FileOutputStream out = new FileOutputStream(f, false);
        final String HASH_ALGORITHM = "SHA-256";
        MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
        for (int i = 0; i < MB; i++) {
            rnd.nextBytes(data);
            md.update(data);
            out.write(data, 0, data.length);
        }
        byte[] digest = md.digest();
        out.close();

        final MessageDigest mdReceive = MessageDigest.getInstance(HASH_ALGORITHM);
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                counter.getAndIncrement();
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                mdReceive.update(data);
                counter.getAndIncrement();
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            long start = System.currentTimeMillis();
            try (ClientChannel channel = new ClientChannel("localhost", 60000);
                 FileInputStream in = new FileInputStream(f)) {
                channel.send("start", in);
            }
            long diff = System.currentTimeMillis() - start;
            System.out.println("Elapsed time: " + diff + "ms.");
            double rate = (double)(GB) / (double)diff * 1000.;
            rate /= MB;
            System.out.printf("Rate: %.2f MB/s\n", rate);
            int loops = (int)Math.ceil(GB / (double)MessageFrame.MAX_MSG_LENGTH);
            while (counter.get() < loops) {
                Thread.sleep(100L);
            }
            Assert.assertEquals(loops + 1, counter.get());
            Assert.assertArrayEquals(digest, mdReceive.digest());
        }
        serverThread.join();
    }

    /**
     * The test will fail when trying to read the ack.
     * THe ack will never arrive since the socket is closed on the server side due to an error.
     */
    @Test(expected = EOFException.class)
    public void testOutOfOrderId() throws Exception {
        Thread serverThread = null;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();

            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendNext("data".getBytes(), true);
            channel.close();
        }
        serverThread.join();
    }

    @Test(expected = EOFException.class)
    public void testOutOfOrderNext() throws Exception {
        Thread serverThread = null;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                Assert.assertEquals("data", new String(data));
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendId("start");
            channel.sendNext("data".getBytes(), false);
            channel.sendId("start");
            channel.sendNext("data".getBytes(), true);
            channel.close();
        }
        serverThread.join();
    }

    @Test
    public void testBadFrameLength() throws Exception {
        Thread serverThread = null;
        final AtomicBoolean aborted = new AtomicBoolean(false);
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
                aborted.set(true);
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            try (Socket socket = new Socket("localhost", 60000)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeInt(100000);

                // Wait
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 600000) {
                    if (aborted.get()) {
                        break;
                    }
                    Thread.sleep(100L);
                }

                DataInputStream in = new DataInputStream(socket.getInputStream());
            }
        }
        serverThread.join();
        Assert.assertTrue(aborted.get());
    }

    /**
     * The test checks whether the sender will deal well with the timeouts.
     * The server channel sends an ACK only for the last frame.
     * Since we can't introduce the timeout into the actual code to simulate the failure,
     * we add in intermediate frame where we put the timeout instead.
     */
    @Test(expected = SocketTimeoutException.class)
    public void testTimeout() throws Exception {
        Thread serverThread = null;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                try {
                    Thread.sleep(5100L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendId("start");
            channel.sendNext("data".getBytes(), false);
            channel.sendNext("EOF_Test".getBytes(), true); // Make sure we send the final.
            channel.close();
        }
        serverThread.join();
    }

    /**
     * The test checks whether the sender will deal well with the last packet being lost.
     * The situation may arise from the sender sending the last packet, which fits in the
     * MTU, and gets lost in transmission. The transmission must be aborted at that point.
     */
    @Test(expected = SocketTimeoutException.class)
    public void testLostLastPacket() throws Exception {
        Thread serverThread = null;
        final AtomicBoolean aborted = new AtomicBoolean(false);
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                Assert.assertEquals("start", id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
                aborted.set(true);
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendId("start");
            channel.sendNext("data".getBytes(), false);
            try {
                channel.receiveAck();
            } catch (SocketTimeoutException e) {
                // Since this is a test, we need to give the server the opportunity to execute
                // all its code.
                while (!aborted.get()) {
                    Thread.sleep(100L);
                }
                throw e;
            }
            channel.close();
        }
        serverThread.join();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeClientPort() throws IllegalArgumentException {
        new ClientChannel("localhost", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLargeInvalidClientPort() throws IllegalArgumentException {
        new ClientChannel("localhost", 100000);
    }

    @Test(expected = IOException.class)
    public void testWrongAck() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(8);
        dos.writeLong(100L);
        dos.flush();
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ClientChannel channel = new ClientChannel("localhost", 100);
        channel.in_ = new DataInputStream(in);
        channel.receiveAck();
    }

    @Test(expected = IOException.class)
    public void testWrongAckMoreData() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(9);
        dos.writeLong(100L);
        dos.writeByte(1);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ClientChannel channel = new ClientChannel("localhost", 100);
        channel.in_ = new DataInputStream(in);
        channel.receiveAck();
    }

    @Test(expected = EOFException.class)
    public void testWrongAckNotEnoughData() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(3);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ClientChannel channel = new ClientChannel("localhost", 100);
        channel.in_ = new DataInputStream(in);
        channel.receiveAck();
    }

    /**
     * Tests the effects of error() throwing an exception.
     * The effects are mostly in the logging, which are a bit hard to test automatically,
     * but worth having a separate test case for. The results are observed in the console.
     */
    @Test(expected = SocketTimeoutException.class)
    public void testBadAbort() throws Exception {
        Thread serverThread = null;
        final AtomicBoolean aborted = new AtomicBoolean(false);
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
                aborted.set(true);
                throw new IOException("Abort failed");
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            ClientChannel channel = new ClientChannel("localhost", 60000);
            channel.connect();
            channel.sendId("start");
            channel.sendNext("data".getBytes(), false);
            try {
                channel.receiveAck();
            } catch (SocketTimeoutException e) {
                // Since this is a test, we need to give the server the opportunity to execute
                // all its code.
                while (!aborted.get()) {
                    Thread.sleep(100L);
                }
                throw e;
            }
            channel.close();
        }
        serverThread.join();
        Assert.assertTrue(aborted.get());
    }

    /**
     * Simulates the accept failure other than timeout.
     * The effects are mostly observed in the console.
     */
    @Test
    public void testFailingAccept() throws Exception {
        Thread serverThread = null;
        MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
            }

            @Override
            public void complete() throws IOException {
            }

            @Override
            public void error() throws IOException {
            }
        });
        server.serverSocket_.close();
        server.serverSocket_ = new ServerSocket() {
            @Override
            public Socket accept() throws IOException {
                throw new RuntimeException("Simulate failure");
            }
        };
        serverThread = new Thread(server);
        serverThread.start();
        serverThread.join();
    }

    @Test
    public void testBridgeOfflineData() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuilder bld = new StringBuilder();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Thread serverThread;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                bld.append(id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                out.write(data);
            }

            @Override
            public void complete() throws IOException {
                latch.countDown();
            }

            @Override
            public void error() throws IOException {
                latch.countDown();
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            TCPAggregatorBridge bridge = new TCPAggregatorBridge("localhost", 60000);
            ByteArrayInputStream data = new ByteArrayInputStream("data".getBytes("UTF-8"));
            DataMetricLOB lob = DataMetricLOB.builder()
                                             .withName("heap")
                                             .withHelp("Help")
                                             .withSeverity(DataMetric.Severity.INFO)
                                             .withData(data)
                                             .build();
            bridge.sendOffline(ImmutableList.of(lob));
            latch.await();
            Assert.assertEquals("heap", bld.toString());
            Assert.assertEquals("data", new String(out.toByteArray(), "UTF-8"));
        }
        serverThread.join();
    }

    @Test
    public void testBridgeUrgentData() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuilder bld = new StringBuilder();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Thread serverThread;
        try (MessageServer server = new MessageServer(60000, new ServerChannelListener() {
            @Override
            public void start(@Nonnull String id) throws IOException {
                bld.append(id);
            }

            @Override
            public void nextFrame(@Nonnull byte[] data) throws IOException {
                out.write(data);
            }

            @Override
            public void complete() throws IOException {
                latch.countDown();
            }

            @Override
            public void error() throws IOException {
                latch.countDown();
            }
        });) {
            serverThread = new Thread(server);
            serverThread.start();
            TCPAggregatorBridge bridge = new TCPAggregatorBridge("localhost", 60000);
            ByteArrayInputStream data = new ByteArrayInputStream("data".getBytes("UTF-8"));
            DataMetricGauge scalar = DataMetricGauge.builder()
                                                      .withName("heap")
                                                      .withHelp("Help")
                                                      .withSeverity(DataMetric.Severity.INFO)
                                                      .withUrgent()
                                                      .build();
            scalar.setData(1.);
            bridge.sendUrgent(ImmutableList.of(scalar));
            latch.await();
            Assert.assertEquals("heap", bld.toString());
            Assert.assertEquals("{\n" +
                                "  \"data_\": 1.0,\n" +
                                "  \"type_\": \"SCALAR\",\n" +
                                "  \"name_\": \"heap\",\n" +
                                "  \"labels_\": [\n" +
                                "    \"label\"\n" +
                                "  ],\n" +
                                "  \"help_\": \"Help\",\n" +
                                "  \"severity_\": \"INFO\",\n" +
                                "  \"urgent_\": true,\n" +
                                "  \"labeledMetrics_\": {}\n" +
                                "}", new String(out.toByteArray(), "UTF-8"));
        }
        serverThread.join();
    }
}
