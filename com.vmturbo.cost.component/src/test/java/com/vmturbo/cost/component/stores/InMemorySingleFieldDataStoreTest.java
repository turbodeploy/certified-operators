package com.vmturbo.cost.component.stores;

import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link InMemorySingleFieldDataStore}.
 */
public class InMemorySingleFieldDataStoreTest {

    private static final String DATA = "Test Data";
    private final DataFilterApplicator<String, Object> filterApplicator = mock(DataFilterApplicator.class);
    private final SingleFieldDataStore<String, Object> store = new InMemorySingleFieldDataStore<>(filterApplicator);

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