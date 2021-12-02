package com.vmturbo.cost.component.stores;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link InMemorySourceProjectedFieldsDataStore}.
 */
public class InMemorySourceProjectedFieldsDataStoreTest {

    private static final String SOURCE_DATA = "sourceData";
    private static final String PROJECTED_DATA = "projectedData";
    private final DiagnosableSingleFieldDataStore<String> sourceStore = Mockito.mock(
            DiagnosableSingleFieldDataStore.class);
    private final DiagnosableSingleFieldDataStore<String> projectedStore = Mockito.mock(
            DiagnosableSingleFieldDataStore.class);
    private final InMemorySourceProjectedFieldsDataStore<String, DiagnosableSingleFieldDataStore<String>>
            store = new InMemorySourceProjectedFieldsDataStore<>(sourceStore, projectedStore);

    /**
     * Test {@link InMemorySourceProjectedFieldsDataStore}.
     */
    @Test
    public void testStore() {
        // ARRANGE
        Mockito.when(sourceStore.getData()).thenReturn(Optional.of(SOURCE_DATA));
        Mockito.when(projectedStore.getData()).thenReturn(Optional.of(PROJECTED_DATA));

        // ASSERT
        Assert.assertEquals(Optional.of(SOURCE_DATA), store.getSourceData());
        Assert.assertEquals(Optional.of(PROJECTED_DATA), store.getProjectedData());

        // ASSERT
        store.setSourceData(SOURCE_DATA);
        Mockito.verify(sourceStore, Mockito.times(1)).setData(SOURCE_DATA);
        store.setProjectedData(PROJECTED_DATA);
        Mockito.verify(projectedStore, Mockito.times(1)).setData(PROJECTED_DATA);
    }
}