package com.vmturbo.extractor.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.extractor.schema.json.export.Entity;
import com.vmturbo.extractor.schema.json.export.ExportedObject;

/**
 * Test that ExtractorKafkaSender works as expected.
 */
public class ExtractorKafkaSenderTest {

    private ExtractorKafkaSender extractorKafkaSender;
    private final IMessageSender<byte[]> kafkaMessageSender = mock(IMessageSender.class);
    private List<ExportedObject> objectsCapture;

    /**
     * Setup before each test.
     */
    @Before
    public void setUp() {
        // capture objects sent to kafka
        this.objectsCapture = new ArrayList<>();
        doAnswer(inv -> {
            byte[] bytes = inv.getArgumentAt(0, byte[].class);
            if (bytes != null) {
                objectsCapture.add(ExportUtils.fromBytes(bytes));
            }
            return CompletableFuture.completedFuture(null);
        }).when(kafkaMessageSender).sendMessageAsync(any());
        this.extractorKafkaSender = spy(new ExtractorKafkaSender(kafkaMessageSender, 10));
    }

    /**
     * Test that entities are sent to Kafka correctly.
     */
    @Test
    public void testSendToKafka() {
        // around 57 bytes
        final Entity entity1 = new Entity();
        entity1.setOid(1L);
        entity1.setName("foo");
        entity1.setType("VIRTUAL_MACHINE");

        // around 58 bytes
        final Entity entity2 = new Entity();
        entity2.setOid(2L);
        entity2.setName("bar");
        entity2.setType("PHYSICAL_MACHINE");
        // send
        int sentCount = extractorKafkaSender.send(Stream.of(entity1, entity2).map(e -> {
            ExportedObject object = new ExportedObject();
            object.setEntity(e);
            return object;
        }).collect(Collectors.toList()));

        // verify
        assertThat(sentCount, is(2));
        assertThat(objectsCapture.size(), is(2));

        Map<Long, Entity> entityById = objectsCapture.stream()
                .map(ExportedObject::getEntity)
                .collect(Collectors.toMap(Entity::getOid, e -> e));

        assertThat(entityById.get(1L).getName(), is(entity1.getName()));
        assertThat(entityById.get(1L).getType(), is(entity1.getType()));
        assertThat(entityById.get(2L).getName(), is(entity2.getName()));
        assertThat(entityById.get(2L).getType(), is(entity2.getType()));
    }
}