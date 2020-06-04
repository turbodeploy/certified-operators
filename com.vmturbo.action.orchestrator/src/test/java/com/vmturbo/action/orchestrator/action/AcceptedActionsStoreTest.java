package com.vmturbo.action.orchestrator.action;

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

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.exception.AcceptedActionStoreOperationException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test for {@link AcceptedActionsStore}.
 */
public class AcceptedActionsStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);
    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();
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
     */
    @Before
    public void setUp() {
        acceptedActionsStore = new AcceptedActionsStore(dbConfig.getDslContext());
    }

    /**
     * Tests persisting acceptance for action.
     *
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAcceptanceForAction() throws AcceptedActionStoreOperationException {
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAlreadyAcceptedAction() throws AcceptedActionStoreOperationException {
        acceptedActionsStore.persistAcceptedAction(RECOMMENDATION_ID_1, ACTION_LATEST_RECOMMENDATION_TIME,
                ACCEPTING_USER_1, ACCEPTED_TIME_1, ACCEPTING_USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        expectedException.expect(AcceptedActionStoreOperationException.class);
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testUpdatingLatestRecommendationTime()
            throws AcceptedActionStoreOperationException {
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testDeletingAcceptedAction() throws AcceptedActionStoreOperationException {
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testRemovingExpiredAcceptedActions() throws AcceptedActionStoreOperationException {
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testGettingAcceptorsForActions() throws AcceptedActionStoreOperationException {
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
     * @throws AcceptedActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testDeletingAcceptancesForActionsAssociatedWithPolicies()
            throws AcceptedActionStoreOperationException {
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
