package com.vmturbo.auth.component.store;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.auth.component.licensing.LicenseDTOUtils;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.auth.component.licensing.store.LicenseLocalStore;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.licensing.utils.LicenseDeserializer;

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
        LicenseApiDTO licenseApiDTO = LicenseDeserializer.deserialize(LicenseLocalStoreTest.C1_LICENSE, null);
        licenseApiDTO.setUuid("C1"); // manually set a key since we don't auto-create them in the
                                    // storage service
        LicenseDTO licenseDTO = LicenseDTOUtils.iLicenseToLicenseDTO(licenseApiDTO);
        store.storeLicense(licenseDTO);
        Optional<LicenseDTO> license = store.getLicenses().stream().findFirst();
        Assert.assertEquals(licenseDTO, license.get());
    }

    @Test
    public void testStoreNullLicense() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        LicenseKVStore store = new LicenseKVStore(keyValueStore);
        exception.expect(RuntimeException.class);
        store.storeLicense(null);
    }

    @Test
    public void testRetrieveNullLicense() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        LicenseKVStore store = new LicenseKVStore(keyValueStore);
        Assert.assertEquals(0, store.getLicenses().size());
    }

    // test that IOexceptions during getLicenses() are actually thrown as IOExceptions
}
