package com.vmturbo.auth.component.widgetset;

import static com.vmturbo.auth.component.store.db.Tables.WIDGETSET;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.assertj.core.util.Lists;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.auth.component.store.db.tables.records.WidgetsetRecord;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class})
@TestPropertySource(properties = {"originalSchemaName=auth"})
public class WidgetsetDbStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;
    private DSLContext dsl;
    private WidgetsetDbStore testDbStore;

    private static final long USER_OID_1 = 111L;
    private static final long USER_OID_2 = 222L;
    private static final long USER_OID_3 = 333L;
    private static final long OID_DOESNT_EXIST = 999999L;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();

        // create the test instance
        testDbStore = new WidgetsetDbStore(dsl);

    }


    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreateWidgetSet() {
        // Arrange

        // Act
        WidgetsetRecord result = testDbStore.createWidgetSet(Widgets.WidgetsetInfo.newBuilder()
                .setCategory("a")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(true)
                .setWidgets("widgets-string")
                .build(), USER_OID_1);
        // Assert
        WidgetsetRecord testFetch = dsl.selectFrom(WIDGETSET)
                .where(WIDGETSET.OID.eq(result.getOid()))
                .fetchOne();
        assertNotNull(testFetch.getOid());
    }

    /**
     * widgets 1 & 2 are category "a" and "b", owned by user id 1, so included.
     * widget 3 is category b but owned by 2 so excluded.
     */
    @Test
    public void testSearchCategories() {
        // Arrange
        addDbWidgets(testDbStore);
        List<String> categoriesList = Lists.newArrayList("a", "b");
        // Act
        Iterator<WidgetsetRecord> result = testDbStore.search(categoriesList, null, USER_OID_1);
        // Assert
        ArrayList<WidgetsetRecord> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(2));
        assertThat(resultList, containsInAnyOrder(widgetsetRecord1, widgetsetRecord2));
    }

    /**
     * widgets 1, 2, 3 are scopeType "scope-type", but only 3 is owned by 2.
     */
    @Test
    public void testSearchScopeType() {
        // Arrange
        addDbWidgets(testDbStore);
        String scopeType = "scope-type";
        // Act
        Iterator<WidgetsetRecord> result = testDbStore.search(null, scopeType, USER_OID_2);
        // Assert
        ArrayList<WidgetsetRecord> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList, containsInAnyOrder(widgetsetRecord3));
    }

    /**
     * only widget 3 is owned by user 2, but widget 4 is shared
     */
    @Test
    public void testSearchSharedType() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Iterator<WidgetsetRecord> result = testDbStore.search(null, null, USER_OID_2);
        // Assert
        ArrayList<WidgetsetRecord> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(2));
        assertThat(resultList, containsInAnyOrder(widgetsetRecord3, widgetsetRecord4));
    }

    /**
     * Fetch widgetset2 for user oid 1
     */
    @Test
    public void testFetchFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.fetch(widgetsetRecord2.getOid(), USER_OID_1);
        // Assert
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetsetRecord2));
    }

    /**
     * Fetch unknown widget ID
     */
    @Test
    public void testFetchNotFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.fetch(99999L, USER_OID_1);
        // Assert
        assertFalse(result.isPresent());
    }

    /**
     * Fetch different owner
     */
    @Test
    public void testFetchDifferentOwnerNotFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.fetch(widgetsetRecord3.getOid(), USER_OID_1);
        // Assert
        assertFalse(result.isPresent());
    }


    /**
     * Fetch shared widgetset with  owner
     */
    @Test
    public void testFetchDifferentOwnerShared() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.fetch(widgetsetRecord4.getOid(), USER_OID_1);
        // Assert
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetsetRecord4));
    }

    /**
     * update widgetset1 'widgets' field.
     */
    @Test
    public void testUpdateSuccess() {
        // Arrange
        addDbWidgets(testDbStore);
        Widgets.WidgetsetInfo widgetset1Update = widgetsetInfo1.toBuilder()
                .setWidgets("new-widgets-string")
                .build();
        // Act
        WidgetsetRecord result = testDbStore.update(widgetsetRecord1.getOid(), widgetset1Update,
                USER_OID_1);
        // Assert
        assertNotNull(result);
        Optional<WidgetsetRecord> updated = testDbStore.fetch(widgetsetRecord1.getOid(), USER_OID_1);
        assertTrue(updated.isPresent());
        assertThat(updated.get().getWidgets(), equalTo("new-widgets-string"));
    }

    /**
     * update widgetset1 'widgets' field.
     */
    @Test
    public void testUpdateDoesntExist() {
        // Arrange
        addDbWidgets(testDbStore);
        thrown.expect(NoDataFoundException.class);
        // Act
        WidgetsetRecord result = testDbStore.update(OID_DOESNT_EXIST, widgetsetInfo1, USER_OID_1);
        // Assert
        fail("Shouldn't get here - exception should have been thrown: " + result);
    }

    /**
     * cannot update widgetset1 'widgets' field from user 2.
     */
    @Test
    public void testUpdateWronguser() {
        // Arrange
        addDbWidgets(testDbStore);
        thrown.expect(NoDataFoundException.class);
        // Act
        testDbStore.update(widgetsetRecord1.getOid(), widgetsetInfo1, USER_OID_2);
        // Assert
        fail("Should have thrown exception");
    }

    /**
     * cannot update widgetset4 'widgets' field from user 2 even though it is shared
     */
    @Test
    public void testUpdateWrongUserShared() {
        // Arrange
        addDbWidgets(testDbStore);
        thrown.expect(NoDataFoundException.class);
        // Act
        testDbStore.update(widgetsetRecord4.getOid(), widgetsetInfo4, USER_OID_2);
        // Assert
        fail("Should have thrown exception");
    }

    @Test
    public void testDeleteFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.delete(widgetsetRecord1.getOid(), USER_OID_1);
        // Assert
        assertNotNull(result);
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetsetRecord1));
        Optional<WidgetsetRecord> afterDelete = testDbStore.fetch(widgetsetRecord1.getOid(), USER_OID_1);
        assertFalse(afterDelete.isPresent());
    }

    @Test
    public void testDeleteNotFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<WidgetsetRecord> result = testDbStore.delete(OID_DOESNT_EXIST, USER_OID_1);
        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    public void testTransferOwnershipSuccess() {
        // Arrange
        addDbWidgets(testDbStore);
        List<Long> widgetOids = ImmutableList.of(widgetsetRecord1.getOid(),
            widgetsetRecord2.getOid());
        // Act
        Iterator<WidgetsetRecord> iter = testDbStore.transferOwnership(
                Collections.singleton(USER_OID_1), USER_OID_2);
        assertNotNull(iter);
        int counter = 0;
        while (iter.hasNext()) {
            WidgetsetRecord record = iter.next();
            assertEquals(USER_OID_2, record.getOwnerOid().longValue());
            assertTrue(widgetOids.contains(record.getOid()));
            counter ++;
        }
        assertEquals(2, counter);
    }

    @Test
    public void testTransferOwnershipNotfound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Iterator<WidgetsetRecord> iter = testDbStore.transferOwnership(
                Collections.singleton(OID_DOESNT_EXIST), USER_OID_1);
        assertNotNull(iter);
        assertFalse(iter.hasNext());
    }

    private Widgets.WidgetsetInfo widgetsetInfo1;
    private Widgets.WidgetsetInfo widgetsetInfo4;
    private WidgetsetRecord widgetsetRecord1;
    private WidgetsetRecord widgetsetRecord2;
    private WidgetsetRecord widgetsetRecord3;
    private WidgetsetRecord widgetsetRecord4;


    private void addDbWidgets(WidgetsetDbStore testDbStore) {
        widgetsetInfo1 = Widgets.WidgetsetInfo.newBuilder()
                .setDisplayName("Widget Set 1")
                .setCategory("a")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build();
        widgetsetRecord1 = testDbStore.createWidgetSet(widgetsetInfo1, USER_OID_1);
        Widgets.WidgetsetInfo widgetsetInfo2 = Widgets.WidgetsetInfo.newBuilder()
                .setDisplayName("Widget Set 2")
                .setCategory("b")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build();
        widgetsetRecord2 = testDbStore.createWidgetSet(widgetsetInfo2, USER_OID_1);
        Widgets.WidgetsetInfo widgetsetInfo3 = Widgets.WidgetsetInfo.newBuilder()
                .setDisplayName("Widget Set 3")
                .setCategory("b")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build();
        widgetsetRecord3 = testDbStore.createWidgetSet(widgetsetInfo3, USER_OID_2);
        widgetsetInfo4 = Widgets.WidgetsetInfo.newBuilder()
                .setCategory("c")
                .setScope("scope")
                .setScopeType("scope-type-2")
                .setSharedWithAllUsers(true)
                .setWidgets("widgets-string")
                .build();
        widgetsetRecord4 = testDbStore.createWidgetSet(widgetsetInfo4, USER_OID_3);
    }


}