package com.vmturbo.topology.processor.targets;

import static com.vmturbo.topology.processor.db.tables.TargetspecOid.TARGETSPEC_OID;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.processor.db.TopologyProcessor;

public class PersistentTargetSpecIdentityStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(TopologyProcessor.TOPOLOGY_PROCESSOR);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final long TARGET_OID_1 = 2333L;
    private static final long TARGET_OID_2 = 666L;
    private static final SimpleMatchingAttributes ATTR_1 = SimpleMatchingAttributes.newBuilder()
            .addAttribute("probeId", Long.toString(1))
            .addAttribute("address", "aaa.com")
            .build();
    private static final SimpleMatchingAttributes ATTR_2 = SimpleMatchingAttributes.newBuilder()
            .addAttribute("probeId", Long.toString(2))
            .addAttribute("address", "192.168.0.1")
            .build();
    private static final String TARGET_IDENTITY_ATTRS_1 = new Gson().toJson(ATTR_1);
    private static final String TARGET_IDENTITY_ATTRS_2 = new Gson().toJson(ATTR_2);

    private DSLContext dsl = dbConfig.getDslContext();

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Load the target spec DB with two entries, target spec-1 and target spec-2.
     * Fetch all the OID mappings into a map and verify.
     * @throws IdentityStoreException
     */
    @Test
    public void testFetchAllOidMappings() throws IdentityStoreException {
        // arrange
        final PersistentIdentityStore testIdentityStore = new PersistentTargetSpecIdentityStore(dsl);
        persistTargetSpecOids();
        // act
        Map<IdentityMatchingAttributes, Long> attrToOidMap = testIdentityStore.fetchAllOidMappings();
        // assert
        assertThat(attrToOidMap.size(), equalTo(2));
        assertThat(attrToOidMap.keySet(), containsInAnyOrder(ATTR_1, ATTR_2));
        assertThat(attrToOidMap.values(), containsInAnyOrder(TARGET_OID_1, TARGET_OID_2));
    }

    /**
     * Load the target spec DB with one entries, target spec 1.
     * Save a new entry, target spec 2
     * Fetch all the OID mappings into a map and verify.
     */
    @Test
    public void testSaveOidMappings() throws Exception {
        // arrange
        final PersistentIdentityStore testIdentityStore = new PersistentTargetSpecIdentityStore(dsl);
        dsl.insertInto(TARGETSPEC_OID)
                .set(TARGETSPEC_OID.ID, TARGET_OID_1)
                .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, TARGET_IDENTITY_ATTRS_1)
                .execute();
        Map<IdentityMatchingAttributes, Long> attrsToOidMap =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(ATTR_2, TARGET_OID_2)
                        .build();
        // act
        testIdentityStore.saveOidMappings(attrsToOidMap);
        // assert
        Map<IdentityMatchingAttributes, Long> attrToOidMap = testIdentityStore.fetchAllOidMappings();
        assertThat(attrToOidMap.size(), equalTo(2));
        assertThat(attrToOidMap.keySet(), containsInAnyOrder(ATTR_1, ATTR_2));
        assertThat(attrToOidMap.values(), containsInAnyOrder(TARGET_OID_1, TARGET_OID_2));
    }

    /**
     * Load the target spec DB with one entries, target spec 1.
     * Update the target spec 1 to target spec 2 with oid 1.
     * Fetch all the OID mappings into a map and verify.
     */
    @Test
    public void testUpdateOidMappings() throws Exception {
        // arrange
        final PersistentIdentityStore testIdentityStore = new PersistentTargetSpecIdentityStore(dsl);
        dsl.insertInto(TARGETSPEC_OID)
                .set(TARGETSPEC_OID.ID, TARGET_OID_1)
                .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, TARGET_IDENTITY_ATTRS_1)
                .execute();
        Map<IdentityMatchingAttributes, Long> updatedMap =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(ATTR_2, TARGET_OID_1)
                        .build();
        // act
        testIdentityStore.updateOidMappings(updatedMap);
        // assert
        Map<IdentityMatchingAttributes, Long> attrsToOidMap = testIdentityStore.fetchAllOidMappings();
        assertEquals(attrsToOidMap.size(), 1);
        Map.Entry<IdentityMatchingAttributes, Long> entry = attrsToOidMap.entrySet().iterator().next();
        assertEquals(entry.getKey(), ATTR_2);
        assertEquals((long) entry.getValue(), TARGET_OID_1);
    }

    /**
     * Test that deleting an OID deletes the OID row.
     *
     * @throws IdentityStoreException - should never happen
     */
    @Test
    public void testDeleteOidMappings() throws IdentityStoreException {
        final PersistentIdentityStore testIdentityStore = new PersistentTargetSpecIdentityStore(dsl);
        // arrange
        persistTargetSpecOids();
        Set<Long> oidsToRemove = Sets.newHashSet(TARGET_OID_1);
        // act
        testIdentityStore.removeOidMappings(oidsToRemove);
        // assert
        List<Long> oidsFound = dsl.select()
                .from(TARGETSPEC_OID)
                .fetch()
                .getValues(TARGETSPEC_OID.ID, Long.class);
        assertFalse("expect OID1 to be deleted", oidsFound.contains(TARGET_OID_1));
        assertTrue("expect OID2 to still remain", oidsFound.contains(TARGET_OID_2));
    }

    /**
     * Test that collecting and restoring target identifiers diags.
     *
     * @throws DiagnosticsException - should never happen
     */
    @Test
    public void testDiagsCollectAndRestore() throws DiagnosticsException {
        final PersistentIdentityStore testIdentityStore = new PersistentTargetSpecIdentityStore(dsl);
        persistTargetSpecOids();
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        testIdentityStore.collectDiags(appender);
        final ArgumentCaptor<String> diagsCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diagsCaptor.capture());
        assertEquals(2, diagsCaptor.getAllValues().size());
        // Clean up db before restore
        dsl.delete(TARGETSPEC_OID).execute();
        testIdentityStore.restoreDiags(diagsCaptor.getAllValues());
        final Result<Record> rs = dsl.select()
                .from(TARGETSPEC_OID)
                .fetch();
        assertEquals(2, rs.size());
        assertThat(rs.getValues(TARGETSPEC_OID.ID), containsInAnyOrder(TARGET_OID_1, TARGET_OID_2));
        assertThat(rs.getValues(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES),
                containsInAnyOrder(TARGET_IDENTITY_ATTRS_1, TARGET_IDENTITY_ATTRS_2));
    }

    /**
     * Persist the two rows in the TARGETSPEC_OID table.
     */
    private void persistTargetSpecOids() {
        dsl.insertInto(TARGETSPEC_OID)
                .set(TARGETSPEC_OID.ID, TARGET_OID_1)
                .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, TARGET_IDENTITY_ATTRS_1)
                .execute();
        dsl.insertInto(TARGETSPEC_OID)
                .set(TARGETSPEC_OID.ID, TARGET_OID_2)
                .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, TARGET_IDENTITY_ATTRS_2)
                .execute();
    }
}