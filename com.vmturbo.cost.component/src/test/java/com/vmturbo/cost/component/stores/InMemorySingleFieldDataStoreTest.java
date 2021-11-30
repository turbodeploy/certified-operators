package com.vmturbo.cost.component.stores;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link InMemorySingleFieldDataStore}.
 */
public class InMemorySingleFieldDataStoreTest {

    private static final String DATA = "Test Data";
    private final SingleFieldDataStore<String> store = new InMemorySingleFieldDataStore<>();

    /**
     * Test for {@link InMemorySingleFieldDataStore#setData}
     * and {@link InMemorySingleFieldDataStore#getData}.
     */
    @Test
    public void testSetGetData() {
        Assert.assertEquals(Optional.empty(), store.getData());
        store.setData(DATA);
        Assert.assertEquals(Optional.of(DATA), store.getData());
    }
}