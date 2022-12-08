package com.vmturbo.group.topologydatadefinition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.db.TestGroupDBEndpointConfig;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests for TopologyDataDefinitionStore.
 */
@RunWith(Parameterized.class)
public class TopologyDataDefinitionStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameter values.
     * @return parameter values
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameter values.
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public TopologyDataDefinitionStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(GroupComponent.GROUP_COMPONENT, configurableDbDialect, dialect, "group",
                TestGroupDBEndpointConfig::groupEndpoint);
        this.dsl = super.getDslContext();
    }

    private IdentityStore<TopologyDataDefinition> topologyDataDefinitionIdentityStore;

    private GroupDAO groupStore = Mockito.mock(GroupDAO.class);

    private TopologyDataDefinitionStore topologyDataDefinitionStore;

    private static final String MANUAL_ENTITY_NAME = "Manual Entity 1";

    private static final long GROUP_ID = 1L;

    private static final long IDENTITY_INITIALIZER_PREFIX = 1234L;

    private static final long UNUSED_OID = 123L;

    private static final TopologyDataDefinition MANUAL_TOPO_DATA_DEF =
        TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(EntityType.SERVICE,
            MANUAL_ENTITY_NAME, true, GROUP_ID, EntityType.VIRTUAL_MACHINE);

    private TopologyDataDefinitionEntry manualTopoDataDefEntry;

    private static final String AUTOMATED_ENTITY_PREFIX = "Automated-";

    private static final String TAG_KEY = "TAG-1";

    private static final TopologyDataDefinition AUTOMATED_TOPO_DATA_DEF =
        TopologyDataDefinitionTestUtils
            .createAutomatedTopologyDataDefinition(EntityType.BUSINESS_TRANSACTION,
                AUTOMATED_ENTITY_PREFIX, EntityType.VIRTUAL_MACHINE, TAG_KEY);

    private TopologyDataDefinitionEntry automatedTopoDataDefEntry;

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Create a new identity store and a new topologyDataDefinitionStore.
     *
     * @throws Exception if there is a problem creating the entities we are trying to create in the
     * topologyDataDefinitionStore.
     */
    @Before
    public void setup() throws Exception {
        topologyDataDefinitionIdentityStore =
            new CachingIdentityStore<>(
                new TopologyDataDefinitionAttributeExtractor(),
                new PersistentTopologyDataDefinitionIdentityStore(dsl),
                new IdentityInitializer(IDENTITY_INITIALIZER_PREFIX));
        topologyDataDefinitionStore = new TopologyDataDefinitionStore(dsl,
            topologyDataDefinitionIdentityStore, groupStore);
        when(groupStore.getExpectedMemberTypes(any(DSLContext.class), anyCollection())).thenReturn(
                HashBasedTable.create());
        manualTopoDataDefEntry =
            topologyDataDefinitionStore.createTopologyDataDefinition(MANUAL_TOPO_DATA_DEF);
        automatedTopoDataDefEntry =
            topologyDataDefinitionStore.createTopologyDataDefinition(AUTOMATED_TOPO_DATA_DEF);
    }

    /**
     * Test that create works - it creates a new entity and assigns it an OID the first time you
     * pass it a definition, but if you try to pass it the same definition twice, it throws an
     * exception.
     *
     * @throws Exception if topologyDataDefinitionStore throws one that we don't expect.
     */
    @Test
    public void testCreate() throws Exception {
        // Test that we created the manual definition properly in setup
        assertEquals(MANUAL_TOPO_DATA_DEF, manualTopoDataDefEntry.getDefinition());
        assertTrue(manualTopoDataDefEntry.hasId());

        // Test that we can't create the same manual entity definition again.
        expectedException.expect(StoreOperationException.class);
        topologyDataDefinitionStore.createTopologyDataDefinition(MANUAL_TOPO_DATA_DEF);

        // Test that the automated entity definition was properly created.
        assertEquals(AUTOMATED_TOPO_DATA_DEF, automatedTopoDataDefEntry.getDefinition());
        assertTrue(automatedTopoDataDefEntry.hasId());
        assertTrue(manualTopoDataDefEntry.getId()
            != automatedTopoDataDefEntry.getId());

        // Test that we can't create the same automated entity definition again.
        expectedException.expect(StoreOperationException.class);
        topologyDataDefinitionStore.createTopologyDataDefinition(AUTOMATED_TOPO_DATA_DEF);
    }

    /**
     * Test that we can create and get the two other types of manual topology data definitions.
     *
     * @throws Exception if topologyDataDefinitionStore throws an exception.
     */
    @Test
    public void testOtherManualTopologyDefinitions() throws Exception {
        final TopologyDataDefinition manualWithStaticEntities =
                TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(
                        EntityType.BUSINESS_TRANSACTION, "manualTopoDataDef2", true,
                        EntityType.VIRTUAL_MACHINE, Sets.newHashSet(100L, 101L, 102L));
        final TopologyDataDefinition manualWithEntityFilters =
                TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(
                EntityType.BUSINESS_APPLICATION, "manualTopoDataDef3", true,
                EntityType.APPLICATION_COMPONENT);
        long topoDefWithStaticEntitiesOid =
                topologyDataDefinitionStore.createTopologyDataDefinition(manualWithStaticEntities)
                        .getId();
        long topoDefWithEntityFiltersOid =
                topologyDataDefinitionStore.createTopologyDataDefinition(manualWithEntityFilters)
                        .getId();
        assertEquals(manualWithStaticEntities,
                topologyDataDefinitionStore.getTopologyDataDefinition(topoDefWithStaticEntitiesOid)
                        .get());
        assertEquals(manualWithEntityFilters,
                topologyDataDefinitionStore.getTopologyDataDefinition(topoDefWithEntityFiltersOid)
                        .get());
    }

    /**
     * Test that gets works as expected - it returns a TopologyDataDefinition if you give it an
     * existing OID and it returns Optional.empty otherwise.
     */
    @Test
    public void testGet() {
        // Test that get works for an existing definition.
        Optional<TopologyDataDefinition> optTopoDef =
            topologyDataDefinitionStore.getTopologyDataDefinition(manualTopoDataDefEntry.getId());
        assertTrue(optTopoDef.isPresent());
        assertEquals(MANUAL_TOPO_DATA_DEF, optTopoDef.get());

        // Test that get returns Optional.empty for a non existent ID.
        optTopoDef = topologyDataDefinitionStore
            .getTopologyDataDefinition(UNUSED_OID);
        assertFalse(optTopoDef.isPresent());
    }

    /**
     * Test that getAllTopologyDataDefinitions works - it returns a collection of all existing
     * TopologyDataDefinitionEntrys.
     */
    @Test
    public void testGetAll() {
       assertThat(topologyDataDefinitionStore.getAllTopologyDataDefinitions(),
           containsInAnyOrder(manualTopoDataDefEntry, automatedTopoDataDefEntry));
       assertEquals(2, topologyDataDefinitionStore.getAllTopologyDataDefinitions().size());
    }

    /**
     * Test that update works - if you give it an OID that exists it updates that definition, but
     * if you give it a bogus OID it does nothing.
     *
     * @throws Exception with topologyDataDefinitionStore does.
     */
    @Test
    public void testUpdate() throws Exception {
        // check that we can update an existing definition.
        final long newGroupId = 2;
        final TopologyDataDefinition updatedManualDef =
            TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(EntityType.SERVICE,
            MANUAL_ENTITY_NAME, false, newGroupId, EntityType.APPLICATION_COMPONENT);
        final Optional<TopologyDataDefinition> optUpdatedDef =
            topologyDataDefinitionStore.updateTopologyDataDefinition(manualTopoDataDefEntry.getId(),
                updatedManualDef);
        assertTrue(optUpdatedDef.isPresent());
        assertEquals(newGroupId, optUpdatedDef.get().getManualEntityDefinition()
            .getAssociatedEntities(0).getAssociatedGroup().getId());

        // Check that we can't update a definition with a non existent OID.
        expectedException.expect(StoreOperationException.class);
        topologyDataDefinitionStore.updateTopologyDataDefinition(UNUSED_OID,
            MANUAL_TOPO_DATA_DEF);

        // Test that we can change a manual topology data definition to automatic
        final TopologyDataDefinition updateFromManualToAutoDef = TopologyDataDefinitionTestUtils
                .createAutomatedTopologyDataDefinition(EntityType.BUSINESS_ACCOUNT, "foobar",
                        EntityType.VIRTUAL_MACHINE, "TAG_AUTO");
        topologyDataDefinitionStore.updateTopologyDataDefinition(manualTopoDataDefEntry.getId(),
                updateFromManualToAutoDef);
        assertEquals(updateFromManualToAutoDef,
                topologyDataDefinitionStore.getTopologyDataDefinition(
                        manualTopoDataDefEntry.getId()).get());
    }

    /**
     * Test that delete works - it deletes an existing definition if you pass in its OID and returns
     * true, and it does nothing if you pass in a bogus OID and returns false.
     */
    @Test
    public void testDelete() throws StoreOperationException {
        // preconditions
        Collection<TopologyDataDefinitionEntry> entityDefs =
            topologyDataDefinitionStore.getAllTopologyDataDefinitions();
        assertEquals(2, entityDefs.size());
        assertTrue(entityDefs.contains(automatedTopoDataDefEntry));

        // now delete automated entity def
        assertTrue(topologyDataDefinitionStore
            .deleteTopologyDataDefinition(automatedTopoDataDefEntry.getId()));
        entityDefs = topologyDataDefinitionStore.getAllTopologyDataDefinitions();
        assertEquals(1, entityDefs.size());
        assertFalse(entityDefs.contains(automatedTopoDataDefEntry));

        // now try to delete a non existent entity
        assertFalse(topologyDataDefinitionStore.deleteTopologyDataDefinition(UNUSED_OID));
        entityDefs = topologyDataDefinitionStore.getAllTopologyDataDefinitions();
        assertEquals(1, entityDefs.size());
        assertTrue(entityDefs.contains(manualTopoDataDefEntry));
    }

    /**
     * Test that diags are created and restored properly.
     *
     * @throws Exception if topologyDataDefinitionStore does.
     */
    @Test
    public void testDiags() throws Exception {
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        // collect the diags with the original two topology data definitions
        topologyDataDefinitionStore.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.times(2)).appendString(diags.capture());

        // create a topology data definition that will be deleted when we restore from diags
        topologyDataDefinitionStore.createTopologyDataDefinition(TopologyDataDefinitionTestUtils
                .createManualTopologyDataDefinition(EntityType.BUSINESS_APPLICATION,
                        "ShouldGetDeleted", true, EntityType.PHYSICAL_MACHINE));
        // confirm that we now have 3 topology data definitions
        Assert.assertEquals(3,
                topologyDataDefinitionStore.getAllTopologyDataDefinitions().size());
        topologyDataDefinitionStore.restoreDiags(diags.getAllValues(), dsl);
        // we should only have the orginal two definitions - not the third definition created after
        // the capture but before the restore.
        Assert.assertEquals(2,
                topologyDataDefinitionStore.getAllTopologyDataDefinitions().size());
        assertThat(topologyDataDefinitionStore.getAllTopologyDataDefinitions(),
                containsInAnyOrder(manualTopoDataDefEntry, automatedTopoDataDefEntry));
    }
}
