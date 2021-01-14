package com.vmturbo.extractor.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.extractor.export.schema.Entity;
import com.vmturbo.extractor.export.schema.ExportedObject;

/**
 * Test that ExtractorKafkaSender works as expected, respecting size limit.
 */
public class ExtractorKafkaSenderTest {

    private ExtractorKafkaSender extractorKafkaSender;
    private final IMessageSender<byte[]> kafkaMessageSender = mock(IMessageSender.class);
    private List<ExportedObject> objectsCapture;

    /**
     * Setup before each test.
     *
     * @throws CommunicationException if communication error occurred
     * @throws InterruptedException if thread is interrupted
     */
    @Before
    public void setUp() throws CommunicationException, InterruptedException {
        // capture objects sent to kafka
        this.objectsCapture = new ArrayList<>();
        doAnswer(inv -> {
            byte[] bytes = inv.getArgumentAt(0, byte[].class);
            if (bytes != null) {
                objectsCapture.addAll(ExportUtils.fromBytes(bytes));
            }
            return null;
        }).when(kafkaMessageSender).sendMessage(any());
        this.extractorKafkaSender = spy(new ExtractorKafkaSender(kafkaMessageSender));
    }

    /**
     * Test that ExtractorKafkaSender works if total size is less than max size limit.
     */
    @Test
    public void testSendToKafkaLessThanMaxRequestSize() {
        when(kafkaMessageSender.getMaxRequestSizeBytes()).thenReturn(Integer.MAX_VALUE);
        when(kafkaMessageSender.getRecommendedRequestSizeBytes()).thenReturn(Integer.MAX_VALUE);
        sendToKafkaAndVerify();
    }

    /**
     * Test that ExtractorKafkaSender works if total size is more than max size limit.
     */
    @Test
    public void testSendToKafkaMoreThanMaxRequestSize() {
        when(kafkaMessageSender.getMaxRequestSizeBytes()).thenReturn(60);
        when(kafkaMessageSender.getRecommendedRequestSizeBytes()).thenReturn(60);
        sendToKafkaAndVerify();
    }

    /**
     * Send entities to Kafka and verify they are sent.
     */
    private void sendToKafkaAndVerify() {
        // around 57 bytes
        final Entity entity1 = new Entity();
        entity1.setId(1L);
        entity1.setName("foo");
        entity1.setType("VIRTUAL_MACHINE");

        // around 58 bytes
        final Entity entity2 = new Entity();
        entity2.setId(2L);
        entity2.setName("bar");
        entity2.setType("PHYSICAL_MACHINE");
        // send
        extractorKafkaSender.send(Stream.of(entity1, entity2).map(e -> {
            ExportedObject object = new ExportedObject();
            object.setEntity(e);
            return object;
        }).collect(Collectors.toList()));

        // verify
        assertThat(objectsCapture.size(), is(2));

        Map<Long, Entity> entityById = objectsCapture.stream()
                .map(ExportedObject::getEntity)
                .collect(Collectors.toMap(Entity::getId, e -> e));

        assertThat(entityById.get(1L).getName(), is(entity1.getName()));
        assertThat(entityById.get(1L).getType(), is(entity1.getType()));
        assertThat(entityById.get(2L).getName(), is(entity2.getName()));
        assertThat(entityById.get(2L).getType(), is(entity2.getType()));
    }
}