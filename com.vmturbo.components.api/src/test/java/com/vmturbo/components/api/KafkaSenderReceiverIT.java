package com.vmturbo.components.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.KafkaMessageProducer;

/**
 * Tests for {@link KafkaMessageProducer} and {@link KafkaMessageConsumer} against a live Kafka
 * instance.
 */
public class KafkaSenderReceiverIT {

    private static Descriptor messageDescriptor;
    private static FieldDescriptor fieldDescriptor;

    private final Logger logger = LogManager.getLogger(getClass());
    @Rule
    public KafkaServer kafkaServer = new KafkaServer();

    private KafkaMessageProducer kafkaProducer;
    private KafkaMessageConsumer kafkaConsumer;
    private ExecutorService threadPool;

    @BeforeClass
    public static void createDescriptor() throws Exception {
        final String fieldName = "counter";
        final DescriptorProtos.DescriptorProto.Builder desBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();
        final DescriptorProtos.FieldDescriptorProto.Builder fd1Builder =
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(1)
                        .setType(Type.TYPE_STRING);
        desBuilder.addField(fd1Builder.build());
        final String messageName = "AnimalPost";
        desBuilder.setName(messageName);
        final DescriptorProtos.DescriptorProto dsc = desBuilder.build();
        final DescriptorProtos.FileDescriptorProto fileDescP =
                DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(dsc).build();

        final Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
        final Descriptors.FileDescriptor dynamicDescriptor =
                Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
        messageDescriptor = dynamicDescriptor.findMessageTypeByName(messageName);
        fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
    }

    @Before
    public void init() throws Exception {
        kafkaConsumer =
                new KafkaMessageConsumer(kafkaServer.getBootstrapServers(), "test-consumer-group");
        kafkaProducer = new KafkaMessageProducer((kafkaServer.getBootstrapServers()));
        threadPool = Executors.newCachedThreadPool();
    }

    private static DynamicMessage createMessage(@Nonnull String value) {
        final DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(messageDescriptor);
        dmBuilder.setField(fieldDescriptor, value);
        return dmBuilder.build();
    }

    @After
    public void shutdown() {
        threadPool.shutdownNow();
        kafkaConsumer.close();
        kafkaProducer.close();
    }

    /**
     * Tessts, that number of messages is sent successfully though Kafka broker.
     *
     * @throws Exception on errors occur.
     */
    @Test
    public void testOneTopic() throws Exception {
        final int messagesCount = 100;
        final String topic = "Hogwarts-news";

        final List<DynamicMessage> messages = new ArrayList<>(messagesCount);
        for (int i = 0; i < messagesCount; i++) {
            messages.add(createMessage(Integer.toString(i)));
        }
        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        final List<DynamicMessage> received =
                Collections.synchronizedList(new ArrayList<>(messages.size()));
        receiver.addListener((msg, cmd) -> {
            received.add(msg);
            cmd.run();
        });
        for (DynamicMessage message : messages) {
            sender.sendMessage(message);
        }
        awaitEquals(messages, received, 30);
    }

    /**
     * Tests multiple topics simultaneously receiving messages into its separate listeners.
     *
     * @throws Exception on errors occur.
     */
    @Test
    public void testMultipleTopics() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final Future<Void> ronsMessages = checkTopic("Ron", 0, 100, latch);
        final Future<Void> hermionesMessages = checkTopic("Hermione", 100, 100, latch);
        final Future<Void> lunasMessages = checkTopic("Luna", 200, 50, latch);
        final Future<Void> nevillesMessages = checkTopic("Neville", 250, 10, latch);
        latch.countDown();
        ronsMessages.get();
        hermionesMessages.get();
        lunasMessages.get();
        nevillesMessages.get();
    }

    @Test
    public void testHugeMessages() throws Exception {
        final int size = 1024 * 1024;
        final StringBuilder sb = new StringBuilder(1024 * 1024);
        for (int i = 0; i < size; i++) {
            sb.append(i % 10);
        }
        final DynamicMessage hugeMessage = createMessage(sb.toString());

        final String topic = "Hogwarts-news";

        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer.getBootstrapServers());
        final AdminClient adminClient = AdminClient.create(props);
        final NewTopic newTopic = new NewTopic(topic, 1, (short)1);
        newTopic.configs(Collections.singletonMap("max.message.bytes", Integer.toString(size * 2)));
        adminClient.createTopics(Collections.singleton(newTopic));

        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        final List<DynamicMessage> received =
                Collections.synchronizedList(new ArrayList<>(1));
        receiver.addListener((msg, cmd) -> {
            received.add(msg);
            cmd.run();
        });
        sender.sendMessageSync(hugeMessage);
        awaitEquals(Collections.singletonList(hugeMessage), received, 30);
    }

    /**
     * Method creates amount of messages and sends them to Kafka broker. After it, it will spawn
     * a new thread (blocked by sendLatch) to receive all the messages from Kafka broker.
     *
     * @param topic topic to operate with
     * @param start start index of the messages (will be added to message body)
     * @param size number of messages to create/send/receive
     * @param sendLatch latch to await before start receiving messages.
     * @return future, which will hold assertions inside.
     * @throws InterruptedException if thread has been interrupted
     */
    private Future<Void> checkTopic(String topic, int start, int size,
            @Nonnull CountDownLatch sendLatch) throws InterruptedException {
        final List<DynamicMessage> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messages.add(createMessage(topic + "-" + Integer.toString(start + i)));
        }
        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        for (DynamicMessage message : messages) {
            sender.sendMessage(message);
        }

        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        return threadPool.submit(() -> {
            sendLatch.await();
            final List<DynamicMessage> received =
                    Collections.synchronizedList(new ArrayList<>(messages.size()));
            receiver.addListener((msg, cmd) -> {
                received.add(msg);
                cmd.run();
            });
            awaitEquals(messages, received, 30);
            return null;
        });
    }

    /**
     * Method awaits to collections to be equal for the specified amount of time.
     *
     * @param expected expected collection
     * @param actual actual results collection
     * @param timoutSec time (in seconds) to await for the success result. If during this
     *      interval {@code actual} collection is changed (appended), then await process is postponed.
     * @param <T> type of objects in collections to compare.
     * @throws InterruptedException if thread has been interrupted while waiting
     */
    private <T> void awaitEquals(@Nonnull List<T> expected, @Nonnull List<T> actual, int timoutSec)
            throws InterruptedException {
        Assert.assertFalse(expected.isEmpty());
        long targetTime = System.currentTimeMillis() + timoutSec * 1000;
        int prevSize = -1;
        while (System.currentTimeMillis() < targetTime) {
            if (Objects.equals(expected, actual)) {
                logger.info("Successfully validated {} messages", expected.size());
                return;
            } else {
                int newSize = actual.size();
                if (prevSize < newSize) {
                    prevSize = newSize;
                    targetTime = System.currentTimeMillis() + timoutSec * 1000;
                }
                Thread.sleep(1000);
            }
        }
        Assert.assertEquals(expected, actual);
        logger.info("Successfully validated {} messages", expected.size());
    }
}
