package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.schema.Tables.ENTITY;
import static com.vmturbo.extractor.schema.Tables.SCOPE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import org.jooq.DSLContext;
import org.jooq.InsertValuesStep5;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.tables.records.EntityRecord;
import com.vmturbo.extractor.schema.tables.records.ScopeRecord;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Test class for {@link ScopeManager}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true", "sqlDialect=POSTGRES"})
public class ScopeManagerTest {

    private ScopeManager scopeManager;
    private DSLContext dsl;

    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    private final DataPack<Long> oidPack = new LongDataPack();

    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        final DbEndpoint endpoint = dbConfig.ingesterEndpoint();
        endpointRule.addEndpoints(endpoint);
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        final WriterConfig config = mock(WriterConfig.class);
        doReturn(10).when(config).insertTimeoutSeconds();
        this.scopeManager = new ScopeManager(oidPack, endpoint, config, pool,
                dbConfig.getDbFetchSize());
        this.dsl = endpoint.dslContext();
    }

    /**
     * Test that the scope manager captures asserted scope relationships correctly into its internal
     * state.
     */
    @Test
    public void testScopesAccumulateCorrectly() {
        scopeManager.addInCurrentScope(1L, 100L);
        scopeManager.addInCurrentScope(1L, 101L);
        scopeManager.addInCurrentScope(2L, 200L, 201L, 203L);
        scopeManager.addInCurrentScope(1L, 100L, 201L);
        assertThat(scopeManager.getCurrentScopingSeeds(100L), containsInAnyOrder(1L));
        assertThat(scopeManager.getCurrentScopingSeeds(101L), containsInAnyOrder(1L));
        assertThat(scopeManager.getCurrentScopingSeeds(200L), containsInAnyOrder(2L));
        assertThat(scopeManager.getCurrentScopingSeeds(201L), containsInAnyOrder(1L, 2L));
        assertThat(scopeManager.getCurrentScopingSeeds(202L), hasSize(0));
        assertThat(scopeManager.getCurrentScopingSeeds(203L), containsInAnyOrder(2L));
    }

    /**
     * Test that adding a symmetric scope adds the same scope both to the seed oid and the scope
     * oid.
     */
    @Test
    public void testSymmetricScope() {
        scopeManager.addInCurrentScope(1L, true, 100L);
        assertThat(scopeManager.getCurrentScopingSeeds(100L), containsInAnyOrder(1L));
        assertThat(scopeManager.getCurrentScopingSeeds(1L), containsInAnyOrder(100L));
    }

    /**
     * Test that if prior state is empty when starting a topology, the prior state will be loaded
     * from the database.
     *
     * @throws UnsupportedDialectException if db endpoint is malformed
     * @throws InterruptedException        if we're interrupted
     * @throws SQLException                if there's a DB error
     */
    @Test
    public void testReloadOnRestart() throws UnsupportedDialectException, InterruptedException, SQLException {
        OffsetDateTime time = OffsetDateTime.now();
        preload(1L, time, 100L, 101L, 201L);
        preload(2L, time, 200L, 201L, 203L);
        scopeManager.startTopology(TopologyInfo.newBuilder()
                .setCreationTime(time.toEpochSecond()).build());
        assertThat(scopeManager.getPriorScopingSeeds(100L), containsInAnyOrder(1L));
        assertThat(scopeManager.getPriorScopingSeeds(101L), containsInAnyOrder(1L));
        assertThat(scopeManager.getPriorScopingSeeds(102L), hasSize(0));
        assertThat(scopeManager.getPriorScopingSeeds(200L), containsInAnyOrder(2L));
        assertThat(scopeManager.getPriorScopingSeeds(201L), containsInAnyOrder(1L, 2L));
        assertThat(scopeManager.getPriorScopingSeeds(203L), containsInAnyOrder(2L));
    }

    /**
     * Test that scope records are correct through three different topology cycles, including new
     * scoping relationships in all three, dropped relationships in each of the last two, and
     * reappearing relationships after the final topology.
     *
     * @throws UnsupportedDialectException if endpoint is malformed
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if DB error
     */
    @Test
    public void testMultiCycleScopes() throws UnsupportedDialectException, InterruptedException, SQLException {
        OffsetDateTime t1 = OffsetDateTime.now();
        setupEntities(100L, 101L, 200L, 201L, 300L);
        Int2IntMap types = getEntityTypes();
        // topology 1
        scopeManager.startTopology(TopologyInfo.newBuilder().setCreationTime(t1.toInstant().toEpochMilli()).build());
        scopeManager.addInCurrentScope(1L, 100L, 101L);
        scopeManager.addInCurrentScope(2L, 200L);
        scopeManager.finishTopology(types);
        Set<ScopeRecord> records = fetchScopeRecords(t1.getOffset());
        assertThat(records.size(), is(3));
        checkPersisted(records, t1, Constants.MAX_TIMESTAMP, 1L, 100L, 101L);
        checkPersisted(records, t1, Constants.MAX_TIMESTAMP, 2L, 200L);
        // topology 2: drop 1/101, add 2/201 and 3/300
        OffsetDateTime t2 = t1.plus(10, ChronoUnit.MINUTES);
        scopeManager.startTopology(TopologyInfo.newBuilder().setCreationTime(t2.toInstant().toEpochMilli()).build());
        scopeManager.addInCurrentScope(1L, 100L);
        scopeManager.addInCurrentScope(2L, 200L, 201L);
        scopeManager.addInCurrentScope(3L, 300L);
        scopeManager.finishTopology(types);
        records = fetchScopeRecords(t1.getOffset());
        assertThat(records.size(), is(5));
        checkPersisted(records, t1, Constants.MAX_TIMESTAMP, 1L, 100L);
        checkPersisted(records, t1, t1, 1L, 101L);
        checkPersisted(records, t1, Constants.MAX_TIMESTAMP, 2L, 200L);
        checkPersisted(records, t2, Constants.MAX_TIMESTAMP, 2L, 201L);
        checkPersisted(records, t2, Constants.MAX_TIMESTAMP, 3L, 300L);
        // topology 3: drop 1/100, re-add 1/101, and all of entity 2
        OffsetDateTime t3 = t1.plus(20, ChronoUnit.MINUTES);
        scopeManager.startTopology(TopologyInfo.newBuilder().setCreationTime(t3.toInstant().toEpochMilli()).build());
        scopeManager.addInCurrentScope(1L, 101L);
        scopeManager.addInCurrentScope(3L, 300L);
        scopeManager.finishTopology(types);
        records = fetchScopeRecords(t1.getOffset());
        assertThat(records.size(), is(6));
        checkPersisted(records, t1, t2, 1L, 100L);
        checkPersisted(records, t1, t1, 1L, 101L);
        checkPersisted(records, t3, Constants.MAX_TIMESTAMP, 1L, 101L);
        checkPersisted(records, t1, t2, 2L, 200L);
        checkPersisted(records, t2, t2, 2L, 201L);
        checkPersisted(records, t2, Constants.MAX_TIMESTAMP, 3L, 300L);
    }

    @Nonnull
    private Set<ScopeRecord> fetchScopeRecords(ZoneOffset zone) {
        final List<ScopeRecord> recs = dsl.fetch(SCOPE);
        recs.forEach(r -> {
            // to prevent false comparison failures with expected records in tests, we normalize
            // timestamp values to use a consistent timezone offset, since the latter is included
            // in the `equals` definition of `OffsetDateTime` class.
            if (r.getStart().isEqual(ScopeManager.EPOCH_TIMESTAMP)) {
                // fixed value with UTC timestamp
                r.setStart(ScopeManager.EPOCH_TIMESTAMP);
            } else {
                r.setStart(r.getStart().withOffsetSameInstant(zone));
            }
            if (r.getFinish().isEqual(Constants.MAX_TIMESTAMP)) {
                // another fixed value with UTC
                r.setFinish(Constants.MAX_TIMESTAMP);
            } else {
                r.setFinish(r.getFinish().withOffsetSameInstant(zone));
            }
        });
        return new HashSet<>(recs);
    }

    private void preload(long oid, OffsetDateTime time, long... scopeOids) {
        final InsertValuesStep5<ScopeRecord, Long, Long, EntityType, OffsetDateTime, OffsetDateTime> insert;
        insert = dsl.insertInto(SCOPE,
                SCOPE.SEED_OID, SCOPE.SCOPED_OID, SCOPE.SCOPED_TYPE, SCOPE.START, SCOPE.FINISH);
        for (final long scopeOid : scopeOids) {
            insert.values(oid, scopeOid, EntityType.VIRTUAL_MACHINE, time, Constants.MAX_TIMESTAMP);
        }
        insert.execute();
    }

    private void setupEntities(long... oids) {
        final InsertValuesStep5<EntityRecord, Long, String, EntityType, OffsetDateTime, OffsetDateTime> insert;
        insert = dsl.insertInto(ENTITY, ENTITY.OID, ENTITY.NAME, ENTITY.TYPE, ENTITY.FIRST_SEEN, ENTITY.LAST_SEEN);
        for (final long oid : oids) {
            insert.values(oid, "x", EntityType.VIRTUAL_MACHINE, OffsetDateTime.now(), OffsetDateTime.now());
        }
        insert.execute();
    }

    private Int2IntMap getEntityTypes() {
        Int2IntMap result = new Int2IntOpenHashMap();
        dsl.select(ENTITY.OID, ENTITY.TYPE).from(ENTITY).fetch()
                .forEach(r -> result.put(oidPack.toIndex(r.get(0, Long.class)),
                        r.get(1, EntityType.class).ordinal()));
        return result;
    }

    private void checkPersisted(Set<ScopeRecord> records,
            OffsetDateTime start, OffsetDateTime finish, long oid, long... scopedOids) {
        for (final long scopedOid : scopedOids) {
            final ScopeRecord rec = SCOPE.newRecord();
            rec.setSeedOid(oid);
            rec.setScopedOid(scopedOid);
            rec.setScopedType(oid == 0L ? EntityType._NONE_ : EntityType.VIRTUAL_MACHINE);
            rec.setStart(start);
            rec.setFinish(finish);
            assertThat(records, hasItem(rec));
        }
    }
}
