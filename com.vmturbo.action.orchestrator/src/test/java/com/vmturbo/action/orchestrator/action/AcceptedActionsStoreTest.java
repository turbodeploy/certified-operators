package com.vmturbo.action.orchestrator.action;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

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
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test for {@link AcceptedActionsStore}.
 */
@RunWith(Parameterized.class)
public class AcceptedActionsStoreTest extends MultiDbTestBase {

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
    public AcceptedActionsStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Rule to expect exceptions, if required.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AcceptedActionsStore acceptedActionsStore;

    private static final long RECOMMENDATION_ID_1 = 1L;
    private static final long RECOMMENDATION_ID_2 = 2L;
    private static final String ACCEPTING_USER_1 = "admin";
    private static final String ACCEPTING_USER_2 = "user";
    private static final String ACCEPTING_USER_TYPE = "TURBO_USER";
    private static final LocalDateTime ACTION_LATEST_RECOMMENDATION_TIME =
            LocalDateTime.now(Clock.fixed(Instant.parse("2020-05-17T09:55:00Z"), ZoneOffset.UTC));
    private static final LocalDateTime ACCEPTED_TIME_1 =
            LocalDateTime.now(Clock.fixed(Instant.parse("2020-05-17T10:00:00Z"), ZoneOffset.UTC));
    private static final LocalDateTime ACCEPTED_TIME_2 =
            LocalDateTime.now(Clock.fixed(Instant.parse("2020-05-17T10:05:00Z"), ZoneOffset.UTC));
    private static final Set<Long> ASSOCIATED_SETTINGS_POLICIES_1 = Sets.newHashSet(12L, 13L);
    private static final Set<Long> ASSOCIATED_SETTINGS_POLICIES_2 = Sets.newHashSet(14L);

    /**
     * Set up for tests.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void setUp() throws SQLException, UnsupportedDialectException, InterruptedException {
        acceptedActionsStore = new AcceptedActionsStore(dsl);
    }

    /**
     * Tests persisting acceptance for action.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAcceptanceForAction() throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_2, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_2, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_2);

        final List<AcceptedActionInfo> collectionWithSingleAcceptedAction =
                acceptedActionsStore.getAcceptedActions(Collections.singletonList(
                        RECOMMENDATION_ID_1));
        final List<AcceptedActionInfo> allAcceptedActions =
                acceptedActionsStore.getAllAcceptedActions();

        Assert.assertEquals(1, collectionWithSingleAcceptedAction.size());
        final AcceptedActionInfo acceptedAction =
                collectionWithSingleAcceptedAction.iterator().next();
        Assert.assertEquals(RECOMMENDATION_ID_1, acceptedAction.getRecommendationId());
        Assert.assertEquals(ACCEPTING_USER_1, acceptedAction.getAcceptedBy());
        Assert.assertEquals(ACCEPTING_USER_TYPE, acceptedAction.getAcceptorType());
        Assert.assertEquals(ACTION_LATEST_RECOMMENDATION_TIME,
                acceptedAction.getLatestRecommendationTime());
        Assert.assertEquals(ACCEPTED_TIME_1, acceptedAction.getAcceptedTime());
        Assert.assertEquals(ASSOCIATED_SETTINGS_POLICIES_1,
                Sets.newHashSet(acceptedAction.getRelatedPolicies().iterator()));

        Assert.assertEquals(2, allAcceptedActions.size());
        Assert.assertEquals(Sets.newHashSet(RECOMMENDATION_ID_1, RECOMMENDATION_ID_2), allAcceptedActions.stream()
                .map(AcceptedActionInfo::getRecommendationId)
                .collect(Collectors.toSet()));
    }

    /**
     * Tests persisting acceptance for already accepted action.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAlreadyAcceptedAction() throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        expectedException.expect(ActionStoreOperationException.class);
        expectedException.expectMessage(
                "Action " + RECOMMENDATION_ID_1 + " has been already accepted by " + ACCEPTING_USER_1);
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_2, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
    }

    /**
     * Test updating latest recommendation time for accepted action. Case when accepted action
     * was re-recommended by market during waiting start of execution window.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testUpdatingLatestRecommendationTime()
            throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        acceptedActionsStore.updateLatestRecommendationTime(Collections.singleton(
                RECOMMENDATION_ID_1));

        final List<AcceptedActionInfo> acceptedActions =
                acceptedActionsStore.getAcceptedActions(Collections.singleton(RECOMMENDATION_ID_1));
        Assert.assertEquals(1, acceptedActions.size());
        final AcceptedActionInfo acceptedAction = acceptedActions.iterator().next();
        Assert.assertEquals(RECOMMENDATION_ID_1, acceptedAction.getRecommendationId());
        Assert.assertTrue(acceptedAction.getLatestRecommendationTime()
                .isAfter(ACTION_LATEST_RECOMMENDATION_TIME));
    }

    /**
     * Test deleting accepted action. Case when accepted action was successfully executed and
     * after it we should delete acceptance from store.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testDeletingAcceptedAction() throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        acceptedActionsStore.deleteAcceptedAction(RECOMMENDATION_ID_1);

        final List<AcceptedActionInfo> acceptedActions =
                acceptedActionsStore.getAcceptedActions(Collections.singleton(RECOMMENDATION_ID_1));
        Assert.assertTrue(acceptedActions.isEmpty());
    }

    /**
     * Test removing expired accepted actions.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testRemovingExpiredAcceptedActions() throws ActionStoreOperationException {
        final long minsAcceptanceTTL = 100L;
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        acceptedActionsStore.removeExpiredActions(minsAcceptanceTTL);

        final List<AcceptedActionInfo> acceptedActions =
                acceptedActionsStore.getAllAcceptedActions();
        Assert.assertTrue(acceptedActions.isEmpty());
    }

    /**
     * Test getting accepting users for accepted actions.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testGettingAcceptorsForActions() throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_2, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_2, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_2);

        final Map<Long, String> acceptorsForActions =
                acceptedActionsStore.getAcceptorsForAllActions();

        final String acceptingUserForAction1 = acceptorsForActions.get(RECOMMENDATION_ID_1);
        final String acceptingUserForAction2 = acceptorsForActions.get(RECOMMENDATION_ID_2);

        Assert.assertEquals(2, acceptorsForActions.size());
        Assert.assertEquals(ACCEPTING_USER_1, acceptingUserForAction1);
        Assert.assertEquals(ACCEPTING_USER_2, acceptingUserForAction2);
    }

    /**
     * Test deleting acceptance for actions associated with policy. Case when we should delete
     * acceptance for actions after deleting policy.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testDeletingAcceptancesForActionsAssociatedWithPolicies()
            throws ActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_2, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_2, ACCEPTED_TIME_2, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_2);
        acceptedActionsStore.removeAcceptanceForActionsAssociatedWithPolicy(
                ASSOCIATED_SETTINGS_POLICIES_2.iterator().next());

        final List<AcceptedActionInfo> acceptedActions =
                acceptedActionsStore.getAllAcceptedActions();
        Assert.assertEquals(1, acceptedActions.size());
        Assert.assertEquals(RECOMMENDATION_ID_1, acceptedActions.iterator().next().getRecommendationId());
    }
}
