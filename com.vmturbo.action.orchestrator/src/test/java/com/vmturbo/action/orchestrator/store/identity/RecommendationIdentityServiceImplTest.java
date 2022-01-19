package com.vmturbo.action.orchestrator.store.identity;

import java.sql.SQLException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * This is a unit test to cover logic in {@link ActionInfoModelCreator} and {@link
 * IdentityServiceImpl}.
 */
@RunWith(Parameterized.class)
public class RecommendationIdentityServiceImplTest extends MultiDbTestBase {

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
    public RecommendationIdentityServiceImplTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

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

    private static final ActionEntity wc1 = ActionEntity.newBuilder()
            .setId(2000L)
            .setType(EntityType.WORKLOAD_CONTROLLER_VALUE)
            .build();
    private static final ActionEntity cs1 = ActionEntity.newBuilder()
            .setId(2001L)
            .setType(EntityType.CONTAINER_SPEC_VALUE)
            .build();
    private static final ActionEntity cs2 = ActionEntity.newBuilder()
            .setId(2002L)
            .setType(EntityType.CONTAINER_SPEC_VALUE)
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
            .setReconfigure(Reconfigure.newBuilder().setTarget(vm2).setSource(pm2).setIsProvider(false))
            .build();
    private final ActionInfo provision = ActionInfo.newBuilder()
            .setProvision(Provision.newBuilder().setEntityToClone(pm1).setProvisionedSeller(1100L))
            .build();
    private final ActionInfo activate =
            ActionInfo.newBuilder().setActivate(Activate.newBuilder().setTarget(vm1)).build();
    private final ActionInfo deactivate =
            ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder().setTarget(vm2)).build();

    private static final ActionInfo atomicResize1 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124).setNewCapacity(456)
                    )
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs2)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124).setNewCapacity(456)
                    )
                    .build())
            .build();

    private static final ActionInfo atomicResize2 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs2)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124).setNewCapacity(456)
                    )
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124)
                            .setNewCapacity(456)
                    )
                    .build())
            .build();

    private static final ActionInfo atomicResize3 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124).setNewCapacity(456)
                    )
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(2))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124)
                            .setNewCapacity(456)
                    )
                    .build())
            .build();

    private static final ActionInfo atomicResize4 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(2))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124)
                            .setNewCapacity(456)
                    )
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124).setNewCapacity(456)
                    )
                    .build())
            .build();

    private static final ActionInfo atomicResize5 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs1)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124)
                            .setNewCapacity(456)
                    )
                    .build())
            .build();

    private static final ActionInfo atomicResize6 = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(wc1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(cs2)
                            .setCommodityType(CommodityType.newBuilder().setType(1))
                            .setCommodityAttribute(CommodityAttribute.CAPACITY)
                            .setOldCapacity(124)
                            .setNewCapacity(456)
                    )
                    .build())
            .build();

    /**
     * Rule to expect exceptions, if required.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionInfoModelCreator attributeExtractor;
    private IdentityServiceImpl<ActionInfo, String, ActionInfoModel> identityService;
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
        store = Mockito.spy(new RecommendationIdentityStore(dsl, 1000));
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(0L);
        identityService =
                new IdentityServiceImpl<>(store, new ActionInfoModelCreator(),
                    ActionInfoModel::getActionHexHash, clock, TTL_MILLIS);
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
     * Test OID assignment for Atomic Resize actions.
     */
    @Test
    public void testOidAssignmentForAtomicResize() {
        final List<Long> update1 =
                identityService.getOidsForObjects(Collections.singletonList(atomicResize1));

        final List<Long> update2
                = identityService.getOidsForObjects(Collections.singletonList(atomicResize2));

        Assert.assertThat(update2, Matchers.hasSize(1));
        Assert.assertEquals(update2.get(0), update1.get(0));

        final List<Long> update3 =
                identityService.getOidsForObjects(Collections.singletonList(atomicResize3));

        final List<Long> update4
                = identityService.getOidsForObjects(Collections.singletonList(atomicResize4));

        Assert.assertThat(update4, Matchers.hasSize(1));
        Assert.assertEquals(update4.get(0), update3.get(0));

        final List<Long> update5 =
                identityService.getOidsForObjects(Collections.singletonList(atomicResize5));

        Assert.assertThat(update5, Matchers.hasSize(1));
        Assert.assertNotEquals(update5.get(0), update1.get(0));
        Assert.assertNotEquals(update5.get(0), update3.get(0));

        final List<Long> update6
                = identityService.getOidsForObjects(Collections.singletonList(atomicResize6));

        Assert.assertThat(update6, Matchers.hasSize(1));
        Assert.assertNotEquals(update6.get(0), update1.get(0));
        Assert.assertNotEquals(update6.get(0), update3.get(0));

        final ActionInfo atomicResize7 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(wc1)
                        .addResizes(createResizeInfo(200120012001L, 100))
                        .addResizes(createResizeInfo(200120012001L, 101))
                        .addResizes(createResizeInfo(200220022002L, 100))
                        .addResizes(createResizeInfo(200220022002L, 101))
                        .addResizes(createResizeInfo(200320032003L, 100))
                        .addResizes(createResizeInfo(200320032003L, 101))
                        .addResizes(createResizeInfo(200420042004L, 100))
                        .addResizes(createResizeInfo(200420042004L, 101))
                        .addResizes(createResizeInfo(200520052005L, 100))
                        .addResizes(createResizeInfo(200520052005L, 101))
                )
                .build();

        final List<Long> update7
                = identityService.getOidsForObjects(Collections.singletonList(atomicResize7));
        Assert.assertThat(update7, Matchers.hasSize(1));
    }

    private ResizeInfo.Builder createResizeInfo(long id, int commType) {
        ActionEntity ae = ActionEntity.newBuilder()
                .setId(id)
                .setType(15)
                .build();
        return ResizeInfo.newBuilder()
                .setTarget(ae)
                .setCommodityType(CommodityType.newBuilder().setType(commType))
                .setCommodityAttribute(CommodityAttribute.CAPACITY)
                .setOldCapacity(124).setNewCapacity(456);
    }

    /**
     * Tests persistence of OIDs assignment after identity service restart.
     */
    @Test
    public void testRestartOfStore() {
        final List<Long> update1 = identityService.getOidsForObjects(Arrays.asList(move, resize));

        identityService = new IdentityServiceImpl<>(store, attributeExtractor,
            ActionInfoModel::getActionHexHash, clock, TTL_MILLIS);

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
