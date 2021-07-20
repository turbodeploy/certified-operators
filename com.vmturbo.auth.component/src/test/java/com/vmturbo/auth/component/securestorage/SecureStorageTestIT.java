package com.vmturbo.auth.component.securestorage;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.impl.DefaultDSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.vmturbo.auth.component.store.DBStore;
import com.vmturbo.auth.component.store.db.tables.Storage;
import com.vmturbo.auth.component.store.db.tables.records.StorageRecord;
import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * The SecureStorageTestIT implements the secure storage tests.
 */
public class SecureStorageTestIT {
    /**
     * The store in test.
     */
    private DBStore store;

    /**
     * The DSL context.
     */
    private DefaultDSLContext dslContext;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
        KeyValueStore keyValueStore = new MapKeyValueStore();

        StorageRecord record = Mockito.mock(StorageRecord.class);

        dslContext = Mockito.mock(DefaultDSLContext.class);
        Mockito.doReturn(null).when(dslContext)
               .fetchLazy(new Storage(), Mockito.mock(Condition.class));
        Mockito.doReturn(record).when(dslContext)
               .newRecord(new Storage(), Mockito.mock(Record.class));

        store = Mockito.spy(new DBStore(dslContext, keyValueStore, "noURL"));
    }

    @After
    public void cleanUp() throws Exception {
    }

    @Test
    public void testCrypto() throws Exception {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt(data);
        assertEquals(data, CryptoFacility.decrypt(ciphertext));
    }

    @Test
    public void testCryptoSubject() throws Exception {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt("subject", data);
        assertEquals(data, CryptoFacility.decrypt("subject", ciphertext));
    }

    @Test(expected = SecurityException.class)
    public void testCryptoSubjectWrongSubject() throws Exception {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt("subject", data);
        assertEquals(data, CryptoFacility.decrypt("subjectBad", ciphertext));
    }

    @Test(expected = SecurityException.class)
    public void testGetDefaultRootPassword() {
        Assert.assertEquals("vmturbo", store.getRootSqlDBPassword());
    }
}
