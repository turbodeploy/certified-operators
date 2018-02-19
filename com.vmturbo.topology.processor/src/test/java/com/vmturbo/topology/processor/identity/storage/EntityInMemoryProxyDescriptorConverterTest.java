package com.vmturbo.topology.processor.identity.storage;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;

/**
 * Unit test for {@link EntityInMemoryProxyDescriptorConverter}.
 */
public class EntityInMemoryProxyDescriptorConverterTest {

    private final PropertyDescriptor descriptor1 = new PropertyDescriptorImpl("val1", 1);
    private final PropertyDescriptor descriptor2 = new PropertyDescriptorImpl("val2", 2);

    private final EntityInMemoryProxyDescriptorConverter converter =
            new EntityInMemoryProxyDescriptorConverter();

    @Test
    public void testRoundTrip() {
        EntityInMemoryProxyDescriptor desc = new EntityInMemoryProxyDescriptor(7L,
                Collections.singletonList(descriptor1), Collections.singletonList(descriptor2));

        EntityInMemoryProxyDescriptor result = converter.from(converter.to(desc));

        assertEquals(desc, result);
    }

    @Test
    public void testPastId() {
        EntityInMemoryProxyDescriptor desc = new EntityInMemoryProxyDescriptor(7L,
                Collections.singletonList(descriptor1), Collections.singletonList(descriptor2));
        String str = converter.to(desc);
        String pastName = EntityInMemoryProxyDescriptorConverter.PAST_ID_PROPS_PROP_NAMES.get(0);
        String replaced =
                str.replace(EntityInMemoryProxyDescriptorConverter.CUR_ID_PROPS_PROP_NAME, pastName);
        EntityInMemoryProxyDescriptor result = converter.from(replaced);
        assertEquals(desc, result);
    }
}
