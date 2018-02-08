package com.vmturbo.auth.component.widgetset;

import static com.vmturbo.auth.component.store.db.Tables.WIDGETSET;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

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

    private static long USER_OID_1 = 1L;
    private static long USER_OID_2 = 2L;

    private static long OID_DOESNT_EXIST = 999999L;

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
        Widgets.Widgetset result = testDbStore.createWidgetSet(1L, Widgets.WidgetsetInfo.newBuilder()
                .setCategory("a")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(true)
                .setWidgets("widgets-string")
                .build());
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
        Iterator<Widgets.Widgetset> result = testDbStore.search(USER_OID_1, categoriesList, null);
        // Assert
        ArrayList<Widgets.Widgetset> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(2));
        assertThat(resultList, containsInAnyOrder(widgetset1, widgetset2));
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
        Iterator<Widgets.Widgetset> result = testDbStore.search(USER_OID_2, null, scopeType);
        // Assert
        ArrayList<Widgets.Widgetset> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList, containsInAnyOrder(widgetset3));
    }

    /**
     * only widget 3 is owned by user 2, but widget 4 is shared
     */
    @Test
    public void testSearchSharedType() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Iterator<Widgets.Widgetset> result = testDbStore.search(USER_OID_2, null, null);
        // Assert
        ArrayList<Widgets.Widgetset> resultList = Lists.newArrayList(result);
        assertThat(resultList.size(), equalTo(2));
        assertThat(resultList, containsInAnyOrder(widgetset3, widgetset4));
    }

    /**
     * Fetch widgetset2 for user oid 1
     */
    @Test
    public void testFetchFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<Widgets.Widgetset> result = testDbStore.fetch(USER_OID_1, widgetset2.getOid());
        // Assert
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetset2));
    }

    /**
     * Fetch unknown widget ID
     */
    @Test
    public void testFetchNotFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<Widgets.Widgetset> result = testDbStore.fetch(USER_OID_1, 99999L);
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
        Optional<Widgets.Widgetset> result = testDbStore.fetch(USER_OID_1, widgetset3.getOid());
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
        Optional<Widgets.Widgetset> result = testDbStore.fetch(USER_OID_1, widgetset4.getOid());
        // Assert
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetset4));
    }

    /**
     * update widgetset1 'widgets' field.
     */
    @Test
    public void testUpdateSuccess() {
        // Arrange
        addDbWidgets(testDbStore);
        Widgets.WidgetsetInfo widgetset1Update = widgetset1.getInfo().toBuilder()
                .setWidgets("new-widgets-string")
                .build();
        // Act
        Widgets.Widgetset result = testDbStore.update(USER_OID_1, widgetset1.getOid(),
                widgetset1Update);
        // Assert
        assertNotNull(result);
        Optional<Widgets.Widgetset> updated = testDbStore.fetch(USER_OID_1, widgetset1.getOid());
        assertTrue(updated.isPresent());
        assertThat(updated.get().getInfo().getWidgets(), equalTo("new-widgets-string"));
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
        Widgets.Widgetset result = testDbStore.update(USER_OID_1, OID_DOESNT_EXIST,
                widgetset1.getInfo());
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
        testDbStore.update(USER_OID_2, widgetset1.getOid(), widgetset1.getInfo());
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
        testDbStore.update(USER_OID_2, widgetset4.getOid(), widgetset4.getInfo());
        // Assert
        fail("Should have thrown exception");
    }

    @Test
    public void testDeleteFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<Widgets.Widgetset> result = testDbStore.delete(USER_OID_1, widgetset1.getOid());
        // Assert
        assertNotNull(result);
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(widgetset1));
        Optional<Widgets.Widgetset> afterDelete = testDbStore.fetch(USER_OID_1, widgetset1.getOid());
        assertFalse(afterDelete.isPresent());
    }

    @Test
    public void testDeleteNotFound() {
        // Arrange
        addDbWidgets(testDbStore);
        // Act
        Optional<Widgets.Widgetset> result = testDbStore.delete(USER_OID_1, OID_DOESNT_EXIST);
        // Assert
        assertFalse(result.isPresent());
    }

    private Widgets.Widgetset widgetset1;
    private Widgets.Widgetset widgetset2;
    private Widgets.Widgetset widgetset3;
    private Widgets.Widgetset widgetset4;

    private void addDbWidgets(WidgetsetDbStore testDbStore) {
        widgetset1 = testDbStore.createWidgetSet(USER_OID_1, Widgets.WidgetsetInfo.newBuilder()
                .setCategory("a")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build());
        widgetset2 = testDbStore.createWidgetSet(USER_OID_1, Widgets.WidgetsetInfo.newBuilder()
                .setCategory("b")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build());
        widgetset3 = testDbStore.createWidgetSet(USER_OID_2, Widgets.WidgetsetInfo.newBuilder()
                .setCategory("b")
                .setScope("scope")
                .setScopeType("scope-type")
                .setSharedWithAllUsers(false)
                .setWidgets("widgets-string")
                .build());
        long USER_OID_3 = 3L;
        widgetset4 = testDbStore.createWidgetSet(USER_OID_3, Widgets.WidgetsetInfo.newBuilder()
                .setCategory("c")
                .setScope("scope")
                .setScopeType("scope-type-2")
                .setSharedWithAllUsers(true)
                .setWidgets("widgets-string")
                .build());
    }


}