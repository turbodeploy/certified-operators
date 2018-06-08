package com.vmturbo.auth.component.store;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * It tests the KV-backed License store {@link LicenseKVStore}.
 */
public class LicenseKVStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void init() throws Exception {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void testStoreAndRetrieveLicense() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        LicenseKVStore store = new LicenseKVStore(keyValueStore);
        store.populateLicense(LicenseLocalStoreTest.C1_LICENSE);
        String license = store.getLicense().get();
        Assert.assertEquals(LicenseLocalStoreTest.C1_LICENSE, license);
    }

    @Test
    public void testStoreNullLicense() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        LicenseKVStore store = new LicenseKVStore(keyValueStore);
        exception.expect(RuntimeException.class);
        store.populateLicense(null);
    }

    @Test
    public void testRetrieveNullLicense() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        LicenseKVStore store = new LicenseKVStore(keyValueStore);
        Assert.assertFalse(store.getLicense().isPresent());

    }
}
