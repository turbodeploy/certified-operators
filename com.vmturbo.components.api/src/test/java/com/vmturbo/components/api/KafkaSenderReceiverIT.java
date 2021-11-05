package com.vmturbo.components.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.KafkaMessageProducer;

/**
 * Tests for {@link KafkaMessageProducer} and {@link KafkaMessageConsumer} against a live Kafka
 * instance.
 */
public class KafkaSenderReceiverIT {

    /**
     * We have a single embedded kafka server that gets started when this test class is
     * initialized.
     *
     * It's automatically started before any methods are run via the @ClassRule annotation.
     * It's automatically stopped after all of the tests are completed via the @ClassRule
     * annotation.     *
     */
    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource =
            new SharedKafkaTestResource()
                    // Start a cluster with 1 broker.
                    .withBrokers(1)
                    .withBrokerProperty("auto.create.topics.enable", "true")
                    .withBrokerProperty("offsets.topic.replication.factor", "1")
                    .withBrokerProperty("message.max.bytes", Integer.toString(1024 * 1024 * 10));
    protected static Descriptor messageDescriptor;
    protected static FieldDescriptor fieldDescriptor;
    protected static KafkaMessageConsumer kafkaConsumer;
    protected final Logger logger = LogManager.getLogger(getClass());
    protected KafkaMessageProducer kafkaProducer;
    protected ExecutorService threadPool;

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
        final Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(
                fileDescP, fileDescs);
        messageDescriptor = dynamicDescriptor.findMessageTypeByName(messageName);
        fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
    }

    /**
     * Close consumer.
     *
     * @throws Exception if any
     */
    @AfterClass
    public static void afterClass() throws Exception {
        kafkaConsumer.close();
    }

   protected static DynamicMessage createMessage(@Nonnull String value) {
        final DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(messageDescriptor);
        dmBuilder.setField(fieldDescriptor, value);
        return dmBuilder.build();
    }

    @Before
    public void init() throws Exception {
        kafkaConsumer = new KafkaMessageConsumer(getSharedKafkaTestResource().getKafkaConnectString(),
                "test-consumer-group");
        kafkaProducer = new KafkaMessageProducer(getSharedKafkaTestResource().getKafkaConnectString(),
                "", Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Integer.MAX_VALUE, Optional.empty());
        threadPool = Executors.newCachedThreadPool();
    }

    @After
    public void shutdown() {
        threadPool.shutdownNow();
        kafkaProducer.close();
    }

    /**
     * Tests, that number of messages is sent successfully though Kafka broker.
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
        final List<DynamicMessage> received = Collections.synchronizedList(
                new ArrayList<>(messages.size()));
        receiver.addListener((msg, cmd, tracingContext) -> {
            received.add(msg);
            cmd.run();
        });
        for (DynamicMessage message : messages) {
            sender.sendMessage(message);
        }
        kafkaConsumer.start();
        awaitEquals(messages, received, 30);
    }

    /**
     * Tests multiple topics simultaneously receiving messages into its separate listeners.
     *
     * @throws Exception on errors occur.
     */
    @Test
    public void testMultipleTopics() throws Exception {
        final Future<Void> ronsMessages = checkTopic("Ron", 0, 100);
        final Future<Void> hermionesMessages = checkTopic("Hermione", 100, 100);
        final Future<Void> lunasMessages = checkTopic("Luna", 200, 50);
        final Future<Void> nevillesMessages = checkTopic("Neville", 250, 10);
        kafkaConsumer.start();
        ronsMessages.get();
        hermionesMessages.get();
        lunasMessages.get();
        nevillesMessages.get();
    }

    /**
     * Test huge messages.
     *
     * @throws Exception on errors occur.
     */
    @Test
    public void testHugeMessages() throws Exception {
        final int size = 1024 * 1024;
        final StringBuilder sb = new StringBuilder(1024 * 1024);
        for (int i = 0; i < size; i++) {
            sb.append(i % 10);
        }
        final DynamicMessage hugeMessage = createMessage(sb.toString());

        final String topic = "Hogwarts-news-HugeMessage";

        final Properties props = new Properties();
        props.put("bootstrap.servers", getSharedKafkaTestResource().getKafkaConnectString());
        final AdminClient adminClient = AdminClient.create(props);
        final NewTopic newTopic = new NewTopic(topic, 1, (short)1);
        newTopic.configs(Collections.singletonMap("max.message.bytes", Integer.toString(size * 2)));
        adminClient.createTopics(Collections.singleton(newTopic));

        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        final List<DynamicMessage> received = Collections.synchronizedList(new ArrayList<>(1));
        receiver.addListener((msg, cmd, tracingContext) -> {
            received.add(msg);
            cmd.run();
        });
        sender.sendMessage(hugeMessage);
        kafkaConsumer.start();
        awaitEquals(Collections.singletonList(hugeMessage), received, 30);
    }

    /**
     * Tests sending messages during the only broker restart. It is expected, that all the
     * messages are successfully processed.
     *
     * @throws Exception if errors occur.
     */
    @Test
    public void testBrokerRestart() throws Exception {
        final List<DynamicMessage> sentMessages = Stream.of("The Ministry has fallen",
                "Scrimgeour is dead", "They're coming")
                .map(KafkaSenderReceiverIT::createMessage)
                .collect(Collectors.toList());
        final String topic = "patronus-message";
        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        sender.sendMessage(sentMessages.get(0));
        final List<DynamicMessage> receivedMessage = new ArrayList<>();
        final CountDownLatch commitLatch = new CountDownLatch(1);
        final CountDownLatch receivedLatch = new CountDownLatch(2);
        receiver.addListener((msg, commitCmd, tracingContext) -> {
            receivedLatch.countDown();
            try {
                commitLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            commitCmd.run();
            receivedMessage.add(msg);
        });
        kafkaConsumer.start();
        receivedLatch.await(30, TimeUnit.SECONDS);
        getSharedKafkaTestResource().getKafkaBrokers().getBrokerById(1).stop();
        getSharedKafkaTestResource().getKafkaBrokers().getBrokerById(1).start();
        for (int i = 1; i < sentMessages.size(); i++) {
            sender.sendMessage(sentMessages.get(i));
        }
        commitLatch.countDown();
        awaitEquals(sentMessages, receivedMessage, 120);
    }

    /**
     * Simple smoke test to ensure broker running appropriate listeners.
     */
    @Test
    public void validateListener() throws ExecutionException, InterruptedException {
        try (final AdminClient adminClient = getKafkaTestUtils().getAdminClient()) {
            final ConfigResource broker1Resource = new ConfigResource(ConfigResource.Type.BROKER,
                    "1");

            // Pull broker configs
            final Config configResult = adminClient.describeConfigs(
                    Collections.singletonList(broker1Resource)).values().get(broker1Resource).get();

            // Check listener
            final String actualListener = configResult.get("listeners").value();
            assertTrue("Expected " + getExpectedListenerProtocol() + ":// and found: "
                            + actualListener,
                    actualListener.contains(getExpectedListenerProtocol() + "://"));

            // Check inter broker protocol
            final String actualBrokerProtocol = configResult.get("security.inter.broker.protocol")
                    .value();
            assertEquals("Unexpected inter-broker protocol", getExpectedListenerProtocol(),
                    actualBrokerProtocol);
        }
    }

    /**
     * Method creates amount of messages and sends them to Kafka broker. After it, it will spawn
     * a new thread (blocked by sendLatch) to receive all the messages from Kafka broker.
     *
     * @param topic topic to operate with
     * @param start start index of the messages (will be added to message body)
     * @param size number of messages to create/send/receive
     * @return future, which will hold assertions inside.
     * @throws InterruptedException if thread has been interrupted
     * @throws CommunicationException if persistent communication exception occurred
     */
    private Future<Void> checkTopic(String topic, int start, int size)
            throws InterruptedException, CommunicationException {
        final List<DynamicMessage> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messages.add(createMessage(topic + "-" + (start + i)));
        }
        final IMessageSender<DynamicMessage> sender = kafkaProducer.messageSender(topic);
        for (DynamicMessage message : messages) {
            sender.sendMessage(message);
        }

        final IMessageReceiver<DynamicMessage> receiver = kafkaConsumer.messageReceiver(topic,
                msg -> DynamicMessage.parseFrom(messageDescriptor, msg));
        final List<DynamicMessage> received = Collections.synchronizedList(
                new ArrayList<>(messages.size()));
        receiver.addListener((msg, cmd, tracingContext) -> {
            received.add(msg);
            cmd.run();
        });
        return threadPool.submit(() -> {
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
     *         interval {@code actual} collection is changed (appended), then await process is
     *         postponed.
     * @param <T> type of objects in collections to compare.
     * @throws InterruptedException if thread has been interrupted while waiting
     */
    private <T> void awaitEquals(@Nonnull List<T> expected, @Nonnull List<T> actual, int timoutSec)
            throws InterruptedException {
        Assert.assertFalse(expected.isEmpty());
        long targetTime = System.currentTimeMillis() + timoutSec * 1000;
        int prevSize = -1;
        while (System.currentTimeMillis() < targetTime) {
            try {
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
            } catch (ConcurrentModificationException e) {
                // We do not synchronize lists for access. So, just ignoring and rechecking later...
            }
        }
        Assert.assertEquals(expected, actual);
        logger.info("Successfully validated {} messages", expected.size());
    }

    /**
     * Get Kafka test resource.
     *
     * @return Kafka test resource.
     */
    protected SharedKafkaTestResource getSharedKafkaTestResource() {
        return sharedKafkaTestResource;
    }

    /**
     * Simple accessor.
     *
     * @return Kafka test utils.
     */
    protected KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

    /**
     * Plain text protocol.
     *
     * @return Plain text protocol
     */
    protected String getExpectedListenerProtocol() {
        return "PLAINTEXT";
    }
}
