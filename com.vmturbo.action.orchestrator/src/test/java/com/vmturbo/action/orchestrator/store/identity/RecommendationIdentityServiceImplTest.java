package com.vmturbo.action.orchestrator.store.identity;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * This is a unit test to cover logic in {@link ActionInfoModelCreator} and {@link
 * IdentityServiceImpl}.
 */
public class RecommendationIdentityServiceImplTest {

    private static final long TTL_MILLIS = 24 * 60 * 60 * 1000;

    private static final ActionEntity vm1 = ActionEntity.newBuilder()
            .setId(1000L)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
    private static final ActionEntity vm2 = ActionEntity.newBuilder()
            .setId(1001L)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
    private static final ActionEntity pm1 = ActionEntity.newBuilder()
            .setId(1002L)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();
    private static final ActionEntity pm2 = ActionEntity.newBuilder()
            .setId(1003L)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();
    private static final ActionEntity volume1 =
            ActionEntity.newBuilder().setId(1004L).setType(EntityType.VIRTUAL_VOLUME_VALUE).build();
    private static final ActionEntity volume2 =
            ActionEntity.newBuilder().setId(1005L).setType(EntityType.VIRTUAL_VOLUME_VALUE).build();

    private final ActionInfo move = ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                    .addChanges(ChangeProvider.newBuilder().setSource(pm1).setDestination(pm2))
                    .setTarget(vm1))
            .build();

    private final ActionInfo resize = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                    .setTarget(vm1)
                    .setCommodityType(CommodityType.newBuilder().setType(1))
                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                    .setNewCapacity(1.5f)
                    .setOldCapacity(1.3f))
            .build();
    private final ActionInfo reconfigure = ActionInfo.newBuilder()
            .setReconfigure(Reconfigure.newBuilder().setTarget(vm2).setSource(pm2))
            .build();
    private final ActionInfo provision = ActionInfo.newBuilder()
            .setProvision(Provision.newBuilder().setEntityToClone(pm1).setProvisionedSeller(1100L))
            .build();
    private final ActionInfo activate =
            ActionInfo.newBuilder().setActivate(Activate.newBuilder().setTarget(vm1)).build();
    private final ActionInfo deactivate =
            ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder().setTarget(vm2)).build();

    /**
     * DB configuration rule - to migrate DB for tests.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);
    /**
     * DB cleanup rule - to remove all the data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanupRule = dbConfig.cleanupRule();
    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionInfoModelCreator attributeExtractor;
    private IdentityServiceImpl<ActionInfo, ActionInfoModel> identityService;
    private RecommendationIdentityStore store;
    private Clock clock;

    /**
     * Static initializer.
     */
    @BeforeClass
    public static void initStatic() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Initializes a test.
     */
    @Before
    public void initialize() {
        attributeExtractor = new ActionInfoModelCreator();
        store = Mockito.spy(new RecommendationIdentityStore(dbConfig.getDslContext()));
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(0L);
        identityService =
                new IdentityServiceImpl<>(store, new ActionInfoModelCreator(), clock, TTL_MILLIS);
    }

    /**
     * Tests how OIDs are assigned.
     */
    @Test
    public void testOidAssignment() {
        final List<Long> update1 =
                identityService.getOidsForObjects(Collections.singletonList(move));

        final List<Long> update2 = identityService.getOidsForObjects(
                Arrays.asList(move, resize, reconfigure, provision, activate, deactivate));
        Assert.assertThat(update2, Matchers.hasItem(update1.iterator().next()));
        Assert.assertThat(update2, Matchers.hasSize(6));
    }

    /**
     * Tests persistence of OIDs assignment after identity service restart.
     */
    @Test
    public void testRestartOfStore() {
        final List<Long> update1 = identityService.getOidsForObjects(Arrays.asList(move, resize));

        identityService = new IdentityServiceImpl<>(store, attributeExtractor, clock, TTL_MILLIS);

        final List<Long> update2 =
                identityService.getOidsForObjects(Arrays.asList(move, provision));
        Assert.assertThat(update2, Matchers.hasItem(update1.get(0)));
        Assert.assertThat(update2, Matchers.hasSize(2));
    }

    /**
     * Tests behaviour for a malformed action.
     */
    @Test
    public void testMalformedAction() {
        final ActionInfo action = ActionInfo.newBuilder().build();
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Could not find a suitable field extractor for action type");
        identityService.getOidsForObjects(Collections.singletonList(action));
    }

    /**
     * Tests how the data is purged from the cache after some timeout passed.
     */
    @Test
    public void testPurgeTimeoutedData() {
        final List<Long> result =
                identityService.getOidsForObjects(Collections.singletonList(move));
        identityService.pruneObsoleteCache();
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.verify(store).fetchOids(Mockito.argThat(new CollectionSize<>(1)));
        identityService.pruneObsoleteCache();
        final List<Long> result2 =
                identityService.getOidsForObjects(Collections.singletonList(move));
        Assert.assertEquals(result, result2);
        Mockito.verify(store).fetchOids(Mockito.argThat(new CollectionSize<>(1)));

        Mockito.when(clock.millis()).thenReturn(TTL_MILLIS + -1L);
        identityService.pruneObsoleteCache();
        final List<Long> result3 =
                identityService.getOidsForObjects(Collections.singletonList(move));
        Assert.assertEquals(result, result3);
        Mockito.verify(store, Mockito.times(1)).fetchOids(Mockito.argThat(new CollectionSize<>(1)));

        Mockito.when(clock.millis()).thenReturn(3 * TTL_MILLIS);
        identityService.pruneObsoleteCache();
        final List<Long> result4 =
                identityService.getOidsForObjects(Collections.singletonList(move));
        Assert.assertEquals(result, result4);
        Mockito.verify(store, Mockito.times(2)).fetchOids(Mockito.argThat(new CollectionSize<>(1)));
    }

    /**
     * Matcher to match collection by size.
     *
     * @param <T> collection items type
     */
    private static class CollectionSize<T> extends BaseMatcher<Collection<T>> {

        private final int expectedSize;

        CollectionSize(int expectedSize) {
            this.expectedSize = expectedSize;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof Collection)) {
                return false;
            }
            final Collection<?> collection = (Collection<?>)item;
            return collection.size() == expectedSize;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Collection size is expected to be " + expectedSize);
        }
    }
}
