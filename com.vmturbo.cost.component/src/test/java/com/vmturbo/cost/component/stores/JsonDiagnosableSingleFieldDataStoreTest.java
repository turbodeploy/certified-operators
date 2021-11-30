package com.vmturbo.cost.component.stores;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonNull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Unit test for {@link JsonDiagnosableSingleFieldDataStore}.
 */
public class JsonDiagnosableSingleFieldDataStoreTest {

    private static final String DIAGS_FILE_NAME = "diags_file_name";
    private static final String DATA = "Test Data";
    private final Gson gson = new Gson();
    private final DiagnosableSingleFieldDataStore<String> store =
            new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                    DIAGS_FILE_NAME, gson, String.class);

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

    /**
     * Test for {@link DiagnosableSingleFieldDataStore#getFileName}.
     */
    @Test
    public void testGetFileName() {
        Assert.assertEquals(DIAGS_FILE_NAME, store.getFileName());
    }

    /**
     * Test for {@link DiagnosableSingleFieldDataStore#collectDiags}
     * and {@link DiagnosableSingleFieldDataStore#restoreDiags} when data store is empty.
     *
     * @throws DiagnosticsException unexpected exception
     * @throws UnsupportedEncodingException unexpected exception
     * @throws IOException unexpected exception
     */
    @Test
    public void testCollectRestoreDiagsEmptyStore() throws DiagnosticsException, IOException {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192)) {
            store.collectDiags(buffer);
            Assert.assertEquals(gson.toJson(JsonNull.INSTANCE),
                    buffer.toString(StandardCharsets.UTF_8.name()));

            store.restoreDiags(buffer.toByteArray(), null);
        }
        Assert.assertEquals(Optional.empty(), store.getData());
    }

    /**
     * Test for {@link DiagnosableSingleFieldDataStore#collectDiags}
     * and {@link DiagnosableSingleFieldDataStore#restoreDiags}.
     *
     * @throws DiagnosticsException unexpected exception
     * @throws IOException unexpected exception
     */
    @Test
    public void testCollectRestoreDiags() throws DiagnosticsException, IOException {
        store.setData(DATA);
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            store.collectDiags(buffer);
            Assert.assertEquals(gson.toJson(DATA), buffer.toString(StandardCharsets.UTF_8.name()));

            // Nullify data store before restoring.
            store.setData(null);
            Assert.assertEquals(Optional.empty(), store.getData());
            store.restoreDiags(buffer.toByteArray(), null);
        }
        Assert.assertEquals(Optional.of(DATA), store.getData());
    }
}