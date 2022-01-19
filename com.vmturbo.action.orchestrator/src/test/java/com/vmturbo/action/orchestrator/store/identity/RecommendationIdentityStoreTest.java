package com.vmturbo.action.orchestrator.store.identity;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test for {@link RecommendationIdentityStore}.
 */
@RunWith(Parameterized.class)
public class RecommendationIdentityStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public RecommendationIdentityStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private static final ActionInfo MULTI_MOVE_1 = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(createActionEntity(1))
            .addChanges(createChangeProvider(2, 3))
            .addChanges(createChangeProvider(4, 5))).build();
    private static final ActionInfo MULTI_MOVE_2 = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(createActionEntity(1))
            .addChanges(createChangeProvider(4, 5))).build();
    private static final ActionInfo MULTI_MOVE_3 = ActionInfo.newBuilder().setMove(
            Move.newBuilder().setTarget(createActionEntity(1))).build();

    private static final long OID_1 = 1001;
    private static final long OID_2 = 1002;
    private static final long OID_3 = 1003;
    private static final ActionInfoModelCreator CREATOR = new ActionInfoModelCreator();
    private static final ActionInfoModel MODEL_1 = CREATOR.apply(MULTI_MOVE_1);
    private static final ActionInfoModel MODEL_2 = CREATOR.apply(MULTI_MOVE_2);
    private static final ActionInfoModel MODEL_3 = CREATOR.apply(MULTI_MOVE_3);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private RecommendationIdentityStore store;

    /**
     * Set up for tests.
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void init() throws SQLException, UnsupportedDialectException, InterruptedException {
        store = new RecommendationIdentityStore(dsl, 1000);
    }

    /**
     * Tests how the data is being saved to identity store and hot is it retrieved back.
     */
    @Test
    public void testPutAndFetch() {
        final ActionInfo multiMove1 = ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(createActionEntity(1))
                .addChanges(createChangeProvider(2, 3))
                .addChanges(createChangeProvider(4, 5))).build();
        final ActionInfo multiMove2 = ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(createActionEntity(1))
                .addChanges(createChangeProvider(4, 5))).build();
        final ActionInfo multiMove3 = ActionInfo.newBuilder().setMove(
                Move.newBuilder().setTarget(createActionEntity(1))).build();
        final ActionInfo multiMove4 = ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(createActionEntity(10))
                .addChanges(createChangeProvider(2, 3))
                .addChanges(createChangeProvider(4, 5))).build();
        final ActionInfo multiMove5 = ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(createActionEntity(11))
                .addChanges(createChangeProvider(12, 13))
                .addChanges(createChangeProvider(14, 15))).build();
        final long oid1 = 1001;
        final long oid2 = 1002;
        final long oid3 = 1003;
        final long oid4 = 1004;
        final ActionInfoModel model1 = CREATOR.apply(multiMove1);
        final ActionInfoModel model2 = CREATOR.apply(multiMove2);
        final ActionInfoModel model3 = CREATOR.apply(multiMove3);
        final ActionInfoModel model4 = CREATOR.apply(multiMove4);
        final ActionInfoModel model5 = CREATOR.apply(multiMove5);
        store.persistModels(
                ImmutableMap.of(model1, oid1, model2, oid2, model3, oid3, model4, oid4));
        Assert.assertEquals(ImmutableMap.of(model1, oid1, model2, oid2, model3, oid3, model4, oid4),
                store.fetchOids(Arrays.asList(model1, model2, model3, model4, model5)));
        Assert.assertEquals(ImmutableMap.of(model1, oid1, model3, oid3),
                store.fetchOids(Arrays.asList(model1, model3, model5)));
    }

    /**
     * Tests that if one action's details are a subset of another actions' details, it will be
     * retrieved correctly.
     */
    @Test
    public void testSubsets() {
        store.persistModels(ImmutableMap.of(MODEL_1, OID_1, MODEL_2, OID_2, MODEL_3, OID_3));
        Assert.assertEquals(Collections.singletonMap(MODEL_1, OID_1),
                store.fetchOids(Collections.singleton(MODEL_1)));
        Assert.assertEquals(Collections.singletonMap(MODEL_2, OID_2),
                store.fetchOids(Collections.singleton(MODEL_2)));
        Assert.assertEquals(Collections.singletonMap(MODEL_3, OID_3),
                store.fetchOids(Collections.singleton(MODEL_3)));
    }

    /**
     * Tests when a new action's details are a superset for an existing action's details.
     */
    @Test
    public void testNewActionSupersetsExisting() {
        store.persistModels(Collections.singletonMap(MODEL_2, OID_2));
        Assert.assertEquals(Collections.emptyMap(),
                store.fetchOids(Collections.singleton(MODEL_1)));
    }

    /**
     * Tests when a new action's details are a subset for an existing action's details.
     */
    @Test
    public void testNewActionSubsetsExisting() {
        store.persistModels(Collections.singletonMap(MODEL_1, OID_1));
        Assert.assertEquals(Collections.emptyMap(),
                store.fetchOids(Collections.singleton(MODEL_2)));
    }

    /**
     * Tests wrong value set to constructor's modelsChunkSize parameter.
     */
    @Test
    public void testWrongModelChunkSize() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("modelsChunkSize");
        new RecommendationIdentityStore(dsl, -1);
    }

    @Nonnull
    private static ChangeProvider createChangeProvider(long src, long dst) {
        return ChangeProvider.newBuilder().setSource(createActionEntity(src)).setDestination(
                createActionEntity(dst)).build();
    }

    @Nonnull
    private static ActionEntity createActionEntity(long oid) {
        return ActionEntity.newBuilder()
                .setId(oid)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setType(15)
                .build();
    }
}
