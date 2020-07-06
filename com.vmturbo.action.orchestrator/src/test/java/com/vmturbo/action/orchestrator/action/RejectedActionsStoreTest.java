package com.vmturbo.action.orchestrator.action;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
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
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test for {@link RejectedActionsStore}.
 */
public class RejectedActionsStoreTest {
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

    private RejectedActionsStore rejectedActionsStore;

    private static final long RECOMMENDATION_ID_1 = 1L;
    private static final long RECOMMENDATION_ID_2 = 2L;
    private static final long RECOMMENDATION_ID_3 = 3L;
    private static final long RECOMMENDATION_ID_4 = 4L;
    private static final String USER_1 = "admin";
    private static final String USER_2 = "user";
    private static final String USER_TYPE = "TURBO_USER";
    private static final LocalDateTime TIME_1 =
            LocalDateTime.now(Clock.fixed(Instant.parse("2020-05-17T10:00:00Z"), ZoneOffset.UTC));
    private static final LocalDateTime TIME_2 =
            LocalDateTime.now(Clock.fixed(Instant.parse("2020-05-17T10:05:00Z"), ZoneOffset.UTC));
    private static final Set<Long> ASSOCIATED_SETTINGS_POLICIES_1 = Sets.newHashSet(12L, 13L);
    private static final Set<Long> ASSOCIATED_SETTINGS_POLICIES_2 = Sets.newHashSet(14L);

    /**
     * Set up for tests.
     */
    @Before
    public void setUp() {
        rejectedActionsStore = new RejectedActionsStore(dbConfig.getDslContext());
    }

    /**
     * Tests persisting rejection for action.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingRejectionForAction() throws ActionStoreOperationException {
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_1, USER_1, TIME_1, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_2, USER_2, TIME_1, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_2);

        final List<RejectedActionInfo> allRejectedActions =
                rejectedActionsStore.getAllRejectedActions();

        Assert.assertEquals(2, allRejectedActions.size());
        Assert.assertEquals(Sets.newHashSet(RECOMMENDATION_ID_1, RECOMMENDATION_ID_2),
                allRejectedActions.stream()
                        .map(RejectedActionInfo::getRecommendationId)
                        .collect(Collectors.toSet()));
        final Optional<RejectedActionInfo> rejectedAction = allRejectedActions.stream()
                .filter(rejAction -> rejAction.getRecommendationId() == RECOMMENDATION_ID_1)
                .findFirst();
        Assert.assertTrue(rejectedAction.isPresent());
        Assert.assertEquals(RECOMMENDATION_ID_1, rejectedAction.get().getRecommendationId());
        Assert.assertEquals(USER_1, rejectedAction.get().getRejectedBy());
        Assert.assertEquals(USER_TYPE, rejectedAction.get().getRejectingUserType());
        Assert.assertEquals(TIME_1, rejectedAction.get().getRejectedTime());
        Assert.assertEquals(ASSOCIATED_SETTINGS_POLICIES_1,
                Sets.newHashSet(rejectedAction.get().getRelatedPolicies().iterator()));
    }

    /**
     * Tests persisting rejection for already rejected action.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAlreadyRejectedAction() throws ActionStoreOperationException {
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_1, USER_1, TIME_1, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);

        expectedException.expect(ActionStoreOperationException.class);
        expectedException.expectMessage(
                "Action " + RECOMMENDATION_ID_1 + " has been already rejected by " + USER_1);
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_1, USER_1, TIME_2, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
    }

    /**
     * Test removing expired rejected actions.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testRemovingExpiredRejectedActions() throws ActionStoreOperationException {
        final long minsRejectionTTL = 100L;
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_1, USER_1, TIME_1,
                USER_TYPE, ASSOCIATED_SETTINGS_POLICIES_1);

        rejectedActionsStore.removeExpiredRejectedActions(minsRejectionTTL);

        final List<RejectedActionInfo> rejectedActions =
                rejectedActionsStore.getAllRejectedActions();
        Assert.assertTrue(rejectedActions.isEmpty());
    }

    /**
     * Test deleting rejections for actions associated with policy. Case when we
     * should delete rejections for actions after deleting policy.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testDeletingRejectionsForActionsAssociatedWithPolicies()
            throws ActionStoreOperationException {
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_3, USER_1, TIME_1, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_1);
        rejectedActionsStore.persistRejectedAction(RECOMMENDATION_ID_4, USER_2, TIME_2, USER_TYPE,
                ASSOCIATED_SETTINGS_POLICIES_2);
        rejectedActionsStore.removeRejectionsForActionsAssociatedWithPolicy(
                ASSOCIATED_SETTINGS_POLICIES_2.iterator().next());

        final List<RejectedActionInfo> rejectedActions =
                rejectedActionsStore.getAllRejectedActions();
        Assert.assertEquals(1, rejectedActions.size());
        Assert.assertEquals(RECOMMENDATION_ID_3,
                rejectedActions.iterator().next().getRecommendationId());
    }
}
