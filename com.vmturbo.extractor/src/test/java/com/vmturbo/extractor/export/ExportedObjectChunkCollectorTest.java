package com.vmturbo.extractor.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Test;

import com.vmturbo.components.api.chunking.GetSerializedSizeException;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.extractor.export.schema.ExportedObject;

/**
 * Test for {@link ExportedObjectChunkCollector}.
 */
public class ExportedObjectChunkCollectorTest {

    /**
     * Test that ExportedObjectChunkCollector works even getSerializedSize throws exception.
     *
     * @throws OversizedElementException element size is too large
     * @throws GetSerializedSizeException exception when getting serialized size
     */
    @Test
    public void testAddObjectWithSerializedException() throws OversizedElementException,
            GetSerializedSizeException {
        ExportedObjectChunkCollector chunkCollector = new ExportedObjectChunkCollector(10, 10);

        // add obj1
        ExportedObject obj1 = mock(ExportedObject.class);
        when(obj1.getSerializedSize()).thenReturn(5);

        Collection<ExportedObject> exportedObjects = chunkCollector.addToCurrentChunk(obj1);
        assertThat(exportedObjects, is(nullValue()));
        assertThat(chunkCollector.count(), is(1));

        // obj2 throw exception, still obj1 inside
        ExportedObject obj2 = mock(ExportedObject.class);
        when(obj2.getSerializedSize()).thenThrow(new NullPointerException("foo"));
        boolean exception = false;
        try {
            exportedObjects = chunkCollector.addToCurrentChunk(obj2);
        } catch (GetSerializedSizeException e) {
            exception = true;
        }
        assertThat(exception, is(true));
        assertThat(exportedObjects, is(nullValue()));
        assertThat(chunkCollector.count(), is(1));

        // add obj3, should return obj1 and obj3
        ExportedObject obj3 = mock(ExportedObject.class);
        when(obj3.getSerializedSize()).thenReturn(5);

        exportedObjects = chunkCollector.addToCurrentChunk(obj3);
        assertThat(exportedObjects, containsInAnyOrder(obj1, obj3));
        assertThat(chunkCollector.count(), is(0));
    }
}