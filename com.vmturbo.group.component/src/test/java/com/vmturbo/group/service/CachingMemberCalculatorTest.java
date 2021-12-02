package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.GroupMemberCachePopulator.CachedGroupMembers;
import com.vmturbo.group.service.GroupMemberCachePopulator.CachedGroupMembers.Type;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for {@link CachingMemberCalculator}.
 */
public class CachingMemberCalculatorTest {
    private static final long GROUP_1_ID = 1L;
    private static final long GROUP_2_ID = 2L;
    private static final long GROUP_3_ID = 3L;
    private static final long GROUP_4_ID = 4L;
    private static final long GROUP_5_ID = 5L;
    private static final long GROUP_6_ID = 6L;

    private GroupMemberCalculator internalCalculator = mock(GroupMemberCalculator.class);

    private GroupDAO groupDAO = mock(GroupDAO.class);

    private static final long TOPOLOGY_ID = 1L;

    private static final long CONTEXT_ID = 2L;

    private Thread mockInitializer = mock(Thread.class);

    private BiFunction<Runnable, String, Thread> threadFactory = mock(BiFunction.class);

    /**
     * Common setup before each test.
     */
    @Before
    public void setup() {
        when(threadFactory.apply(any(), anyString())).thenReturn(mockInitializer);
    }

    /**
     * Test regrouping on new topology availability.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegrouping() throws Exception {
        // ARRANGE
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                  groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
            .setId(1)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("foo"))
            .build();
        Set<Long> g1Members = Sets.newHashSet( 9L, 10L);
        Grouping g2 = Grouping.newBuilder()
            .setId(2)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("bar"))
            .build();
        Set<Long> g2Members = Sets.newHashSet(10L, 11L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1, g2));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
            .thenReturn(g1Members);
        when(internalCalculator.getGroupMembers(groupDAO, g2.getDefinition(), false))
            .thenReturn(g2Members);

        // ACT
        memberCalculator.regroup();

        // ASSERT
        verify(internalCalculator, times(2))
            .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());

        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false),
            is(g1Members));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g2.getId()), false),
            is(g2Members));

        // lets check if entities have correct parent
        Map<Long, Set<Long>> entityParents = memberCalculator.getEntityGroups(
            groupDAO, ImmutableSet.of(9L, 10L, 11L), Collections.emptySet());
        assertThat(entityParents.get(9L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(10L), is(ImmutableSet.of(1L, 2L)));
        assertThat(entityParents.get(11L), is(Collections.singleton(2L)));

        // The internal calculator shouldn't have gotten called anymore, since we cached the
        // members.
        verify(internalCalculator, times(2))
            .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());
    }

    /**
     * Test that if calculation of a single group's members fails, regrouping doesn't stop.
     *
     * @throws Exception to satisfy compiler
     */
    @Test
    public void testExecutionContinuesAfterSingleGroupFailure() throws Exception {
        // GIVEN
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
                .setId(1)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("foo"))
                .build();
        Grouping g2 = Grouping.newBuilder()
                .setId(2)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("bar"))
                .build();
        Set<Long> g2Members = Sets.newHashSet(10L, 11L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1, g2));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
                .thenThrow(new BadSqlGrammarException(null, "SqlQuery", new SQLException()));
        when(internalCalculator.getGroupMembers(groupDAO, g2.getDefinition(), false))
                .thenReturn(g2Members);

        // WHEN
        memberCalculator.regroup();

        // THEN
        LongSet cachedGroupIds = memberCalculator.getCachedGroupIds();
        // Verify that both groups were cached: even though an exception occurred during calculation
        // for group 1 members, the execution continued and group 2 was also cached.
        assertEquals(2, cachedGroupIds.size());
        assertTrue(cachedGroupIds.contains(g1.getId()));
        assertTrue(cachedGroupIds.contains(g2.getId()));
    }

    /**
     * Test that given two groups g1 and g2, such that g1 is a member of g2 and g2 is a member of
     * g1 we can resolve them properly without ending up in a infinite loop.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRecursiveGroupResolution() throws Exception {
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
            internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
            .setId(1)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("foo"))
            .build();
        Set<Long> g1Members = Sets.newHashSet(2L);
        Grouping g2 = Grouping.newBuilder()
            .setId(2)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("bar"))
            .build();
        Set<Long> g2Members = Sets.newHashSet(1L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1, g2));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
            .thenReturn(g1Members);
        when(internalCalculator.getGroupMembers(groupDAO, g2.getDefinition(), false))
            .thenReturn(g2Members);
        memberCalculator.regroup();
        memberCalculator.getGroupMembers(groupDAO, Collections.singletonList(1L), true);
        verify(internalCalculator, times(2))
            .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());
    }

    /**
     * Test getting expanded members recursively.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetExpandedMembers() throws Exception {
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        GroupDefinition parentGroupDefinition = GroupDefinition.newBuilder()
                .setDisplayName("foo").build();
        Grouping parentGroup = Grouping.newBuilder()
            .setId(1)
            .setDefinition(parentGroupDefinition)
            .build();
        GroupDefinition childGroupDefinition = GroupDefinition.newBuilder()
                .setDisplayName("bar").build();
        Grouping childGroup = Grouping.newBuilder()
            .setId(2)
            .setDefinition(childGroupDefinition)
            .build();
        // The child group, plus an entity.
        Set<Long> parentGroupMembers = Sets.newHashSet(childGroup.getId(), 9L);
        Set<Long> childGroupMembers = Sets.newHashSet(10L, 11L);
        when(internalCalculator.getGroupMembers(groupDAO, parentGroupDefinition, false))
            .thenReturn(parentGroupMembers);
        when(internalCalculator.getGroupMembers(groupDAO, childGroupDefinition, false))
            .thenReturn(childGroupMembers);
        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(parentGroup, childGroup));
        when(groupDAO.getGroupIds(any())).thenReturn(Sets.newHashSet(parentGroup.getId(), childGroup.getId()));

        memberCalculator.regroup();

        // Expand parent group
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(parentGroup.getId()), true),
                is(Sets.newHashSet(9L, 10L, 11L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(childGroup.getId()), true),
                is(Sets.newHashSet(10L, 11L)));
    }

    /**
     * Test that newly created user group notifications cache the group members.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupCreated() throws Exception {
        // Regrouping set to "true" because otherwise we don't eagerly cache created groups.
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));
        GroupDefinition groupDefinition = GroupDefinition.newBuilder()
                .setDisplayName("foo")
                .build();

        Set<Long> members = Sets.newHashSet(9L, 10L);
        when(internalCalculator.getGroupMembers(groupDAO, groupDefinition, false))
                .thenReturn(members);


        memberCalculator.onUserGroupCreated(1L, groupDefinition);

        // make sure the associated entries also added for entity to parent
        Map<Long, Set<Long>> entityParents = memberCalculator.getEntityGroups(
            groupDAO, ImmutableSet.of(9L, 10L), Collections.emptySet());
        assertThat(entityParents.get(9L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(10L), is(Collections.singleton(1L)));

        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, groupDefinition, false);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
            Collections.singleton(1L), false), is(members));

        // No additional query to the underlying calculator.
        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, groupDefinition, false);
    }

    /**
     * Test that updated user group notifications cache the new group members.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupUpdated() throws Exception {
        // ARRANGE
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
                .setId(1)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("foo"))
                .build();
        Set<Long> initialMembers = Sets.newHashSet(8L, 9L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
                .thenReturn(initialMembers);

        // Trigger regrouping.
        memberCalculator.regroup();

        GroupDefinition updatedDef = GroupDefinition.newBuilder()
                .setDisplayName("foo1")
                .build();
        Set<Long> updatedMembers = Sets.newHashSet(9L, 10L, 11L);

        when(memberCalculator.getGroupMembers(groupDAO, updatedDef, false))
                .thenReturn(updatedMembers);

        // Trigger group update.
        memberCalculator.onUserGroupUpdated(g1.getId(), updatedDef);

        Map<Long, Set<Long>> entityParents = memberCalculator.getEntityGroups(
            groupDAO, ImmutableSet.of(8L, 9L, 10L, 11L), Collections.emptySet());

        // ASSERT
        // Internal calculator should have been calculated once on the first regrouping,
        // and once on update.
        verify(internalCalculator, times(2))
                .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());

        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false),
                is(updatedMembers));

        // make sure the associated entries are correct for entity to parent cache
        assertThat(entityParents.get(8L), is(Collections.emptySet()));
        assertThat(entityParents.get(9L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(10L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(11L), is(Collections.singleton(1L)));

        // The internal calculator shouldn't have gotten called anymore, since we cached the
        // members.
        verify(internalCalculator, times(2))
                .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());
    }

    /**
     * Test that if a user group is deleted we remove it from the cache.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupDeleted() throws Exception {
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
                .setId(1)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("foo"))
                .build();
        Set<Long> initialMembers = Sets.newHashSet(8L, 9L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1));
        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false))
                .thenReturn(initialMembers);

        // Ask for the members to put them into the cache.
        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(g1.getId()), false), is(initialMembers));

        // The underlying calculator now returns empty.
        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false))
                .thenReturn(Collections.emptySet());

        // Trigger group update.
        memberCalculator.onUserGroupDeleted(g1.getId());
        Map<Long, Set<Long>> entityParents = memberCalculator.getEntityGroups(
            groupDAO, ImmutableSet.of(8L, 9L), Collections.emptySet());

        // make sure that the relation to deleted group has been removed
        assertThat(entityParents.get(8L), is(Collections.emptySet()));
        assertThat(entityParents.get(9L), is(Collections.emptySet()));

        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(g1.getId()), false), is(Collections.emptySet()));
    }

    /**
     * Test that there is an initial regroup scheduled at construction time.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegroupOnInitialize() throws Exception {
        final ArgumentCaptor<Runnable> initCaptor = ArgumentCaptor.forClass(Runnable.class);
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));
        verify(threadFactory).apply(initCaptor.capture(), anyString());

        Grouping g1 = Grouping.newBuilder()
                .setId(1)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("foo"))
                .build();
        Set<Long> initialMembers = Sets.newHashSet(8L, 9L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
                .thenReturn(initialMembers);

        initCaptor.getValue().run();

        verify(internalCalculator).getGroupMembers(groupDAO, g1.getDefinition(), false);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(g1.getId()), false), is(initialMembers));
    }

    /**
     * Test the the case that we are getting groups with specific type for the entities.
     *
     * @throws StoreOperationException if something goes wrong.
     */
    @Test
    public void getEntityGroupsWithType() throws StoreOperationException {
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
            internalCalculator, Type.SET, true, threadFactory,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, Type.SET.getFactory(), true));

        Grouping g1 = Grouping.newBuilder()
            .setId(1)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName("foo"))
            .build();
        Set<Long> g1Members = Sets.newHashSet( 9L, 10L);
        Grouping g2 = Grouping.newBuilder()
            .setId(2)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                .setDisplayName("bar"))
            .build();
        Set<Long> g2Members = Sets.newHashSet(10L, 11L);

        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(g1, g2));
        when(internalCalculator.getGroupMembers(groupDAO, g1.getDefinition(), false))
            .thenReturn(g1Members);
        when(internalCalculator.getGroupMembers(groupDAO, g2.getDefinition(), false))
            .thenReturn(g2Members);

        memberCalculator.regroup();

        // ACT
        Map<Long, Set<Long>> entityParents = memberCalculator.getEntityGroups(
            groupDAO, ImmutableSet.of(9L, 10L, 11L),
                Collections.singleton(CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER));

        // ASSERT
        assertThat(entityParents.get(9L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(10L), is(Collections.singleton(1L)));
        assertThat(entityParents.get(11L), is(Collections.emptySet()));
    }

    /**
     * Test the fastutil {@link CachedGroupMembers}.
     */
    @Test
    public void testFastutilMembers() {
        final CachedGroupMembers m = Type.fromString("set").getFactory().get();
        final LongSet output = new LongOpenHashSet();
        assertFalse(m.get(output::add));
        assertThat(output, is(LongSets.EMPTY_SET));

        m.set(Sets.newHashSet(1L, 2L));
        assertTrue(m.get(output::add));
        assertThat(output, containsInAnyOrder(1L, 2L));
    }

    /**
     * Test the bitmap {@link CachedGroupMembers}.
     */
    @Test
    public void testBitmapMembers() {
        final CachedGroupMembers m = Type.fromString("bitmap").getFactory().get();
        final LongSet output = new LongOpenHashSet();
        assertFalse(m.get(output::add));
        assertThat(output, is(LongSets.EMPTY_SET));

        m.set(Sets.newHashSet(1L, 2L));
        assertTrue(m.get(output::add));
        assertThat(output, containsInAnyOrder(1L, 2L));
    }

    /**
     * Test the case that some updates happens during regroup. The new cache
     * should have the updates happening during regroup.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    @Ignore("This test is failing occasionally. The reason is getting investigated in OM-77963")
    public void testConcurrentUpdateOnRegroup() throws Exception {
        // ARRANGE
        // this semaphore is used to block regroup operation for a period of time
        // so we can simulate operations during regroup
        final Semaphore semaphore = new Semaphore(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean returnNewValues = new AtomicBoolean(false);

        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> {
                    latch.countDown();
                    try {
                        semaphore.acquire();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        semaphore.release();
                    }
                    final Long2ShortOpenHashMap groupToType = createGroupToTypeMap(
                            returnNewValues.get());
                    final Long2ObjectOpenHashMap<CachedGroupMembers> groupIdToMemberIds =
                            createGroupIdToMemberIds(returnNewValues.get());
                    final Long2ObjectOpenHashMap<LongSet> entityIdToParentGroupIds =
                            createEntityIdToParentGroupIds(returnNewValues.get());
                    return ImmutableGroupMembershipRelationships.builder()
                            .success(true)
                            .groupIdToGroupMemberIdsMap(groupIdToMemberIds)
                            .entityIdToGroupIdsMap(entityIdToParentGroupIds)
                            .groupIdToType(groupToType)
                            .build();
                });

        // ACT 1: Perform the regroup operation
        memberCalculator.regroup();

        // ASSERT 1
        assertThat(Arrays.asList(memberCalculator.getCachedGroupIds().toArray()),
                containsInAnyOrder(GROUP_1_ID, GROUP_2_ID, GROUP_3_ID, GROUP_4_ID, GROUP_5_ID));
        assertThat(memberCalculator.getEmptyGroupIds(groupDAO), containsInAnyOrder(GROUP_3_ID));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_1_ID), false),
                equalTo(ImmutableSet.of(100L, 101L, 102L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_2_ID), false),
                equalTo(ImmutableSet.of(101L, 103L, 104L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_3_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_4_ID), false),
                equalTo(ImmutableSet.of(100L, 101L, 102L, 103L, 104L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_5_ID), false),
                equalTo(ImmutableSet.of(203L, 204L)));
         Map<Long, Set<Long>> parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                 .of(100L, 101L, 102L, 103L, 104L, 105L, 203L, 204L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_4_ID)));
        assertThat(parents.get(101L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(102L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_4_ID)));
        assertThat(parents.get(103L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(104L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(105L), equalTo(Collections.emptySet()));
        assertThat(parents.get(203L), equalTo(ImmutableSet.of(GROUP_5_ID)));
        assertThat(parents.get(204L), equalTo(ImmutableSet.of(GROUP_5_ID)));

        // ARRANGE 2
        semaphore.acquire();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<CachingMemberCalculator.RegroupingResult> regroupFuture =
                executorService.submit(() -> memberCalculator.regroup());

        // We need to wait until the cache is running regroup. Otherwise, the changes
        // will get lost. In reality that will not happen as we will see the changes
        // that took place when we read groups from database. However, we are using
        // mock data. Therefore, we need to make sure that the regroup operation
        // started before proceeding.
        if (!latch.await(5, TimeUnit.MINUTES))  {
            fail("The regroup operation failed to start in the timeout period.");
        }

        final GroupDefinition group5 = GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                .setDisplayName("group5").build();
        when(internalCalculator.getGroupMembers(any(), eq(group5), anyBoolean()))
                .thenReturn(ImmutableSet.of(100L, 105L));

        // ACT 2: Create a group  during the regroup operation
        memberCalculator.onUserGroupCreated(GROUP_6_ID, group5);

        // ASSERT 2
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_6_ID), false),
                equalTo(ImmutableSet.of(100L, 105L)));
        parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                .of(100L, 105L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_6_ID, GROUP_4_ID)));
        assertThat(parents.get(105L), equalTo(ImmutableSet.of(GROUP_6_ID)));

        // ARRANGE 3
        final GroupDefinition group2_updated = GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                .setDisplayName("group2_updated").build();
        when(internalCalculator.getGroupMembers(any(), eq(group2_updated), anyBoolean()))
                .thenReturn(ImmutableSet.of(101L, 104L, 106L));
        final GroupDefinition group5_updated = GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                .setDisplayName("group5_updated").build();
        when(internalCalculator.getGroupMembers(any(), eq(group5_updated), anyBoolean()))
                .thenReturn(ImmutableSet.of(105L, 106L));
        final GroupDefinition group1_updated = GroupDefinition.newBuilder()
                .setType(CommonDTO.GroupDTO.GroupType.REGULAR)
                .setDisplayName("group1_updated").build();
        when(internalCalculator.getGroupMembers(any(), eq(group1_updated), anyBoolean()))
                .thenReturn(Collections.emptySet());

        // ACT 3: Update three groups
        memberCalculator.onUserGroupUpdated(GROUP_2_ID, group2_updated);
        memberCalculator.onUserGroupUpdated(GROUP_6_ID, group5_updated);
        memberCalculator.onUserGroupUpdated(GROUP_1_ID, group1_updated);

        // ASSERT 3
        assertThat(Arrays.asList(memberCalculator.getCachedGroupIds().toArray()),
                containsInAnyOrder(GROUP_1_ID, GROUP_2_ID, GROUP_3_ID, GROUP_4_ID, GROUP_5_ID, GROUP_6_ID));
        assertThat(memberCalculator.getEmptyGroupIds(groupDAO), containsInAnyOrder(GROUP_3_ID,
                GROUP_1_ID));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_1_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_2_ID), false),
                equalTo(ImmutableSet.of(101L, 104L, 106L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_3_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_4_ID), false),
                equalTo(ImmutableSet.of(100L, 101L, 102L, 103L, 104L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_6_ID), false),
                equalTo(ImmutableSet.of(105L, 106L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_5_ID), false),
                equalTo(ImmutableSet.of(203L, 204L)));
        parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                .of(100L, 101L, 102L, 103L, 104L, 105L, 106L, 203L, 204L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(Collections.singleton(GROUP_4_ID)));
        assertThat(parents.get(101L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(102L), equalTo(Collections.singleton(GROUP_4_ID)));
        assertThat(parents.get(103L), equalTo(Collections.singleton(GROUP_4_ID)));
        assertThat(parents.get(104L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(105L), equalTo(Collections.singleton(GROUP_6_ID)));
        assertThat(parents.get(106L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_6_ID)));
        assertThat(parents.get(203L), equalTo(ImmutableSet.of(GROUP_5_ID)));
        assertThat(parents.get(204L), equalTo(ImmutableSet.of(GROUP_5_ID)));

        // ACT 4: Delete a group
        memberCalculator.onUserGroupDeleted(GROUP_4_ID);

        // ASSERT 4
        assertThat(Arrays.asList(memberCalculator.getCachedGroupIds().toArray()),
                containsInAnyOrder(GROUP_1_ID, GROUP_2_ID, GROUP_3_ID, GROUP_5_ID, GROUP_6_ID));
        assertThat(memberCalculator.getEmptyGroupIds(groupDAO), containsInAnyOrder(GROUP_3_ID,
                GROUP_1_ID));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_1_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_2_ID), false),
                equalTo(ImmutableSet.of(101L, 104L, 106L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_3_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_4_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_6_ID), false),
                equalTo(ImmutableSet.of(105L, 106L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_5_ID), false),
                equalTo(ImmutableSet.of(203L, 204L)));
        parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                .of(100L, 101L, 102L, 103L, 104L, 105L, 106L, 203L, 204L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(Collections.emptySet()));
        assertThat(parents.get(101L), equalTo(Collections.singleton(GROUP_2_ID)));
        assertThat(parents.get(102L), equalTo(Collections.emptySet()));
        assertThat(parents.get(103L), equalTo(Collections.emptySet()));
        assertThat(parents.get(104L), equalTo(Collections.singleton(GROUP_2_ID)));
        assertThat(parents.get(105L), equalTo(Collections.singleton(GROUP_6_ID)));
        assertThat(parents.get(106L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_6_ID)));
        assertThat(parents.get(203L), equalTo(ImmutableSet.of(GROUP_5_ID)));
        assertThat(parents.get(204L), equalTo(ImmutableSet.of(GROUP_5_ID)));

        // ARRANGE 5
        // release the semaphore so the group goes on
        returnNewValues.set(true);
        semaphore.release();

        // ACT 5 Verify the changes during regroup are not lost
        regroupFuture.get();

        // ASSERT 5: Here we expect that we have both changes coming from db changes
        // and also changes that happen to the user groups during the regroup operation.
        assertThat(Arrays.asList(memberCalculator.getCachedGroupIds().toArray()),
                containsInAnyOrder(GROUP_1_ID, GROUP_2_ID, GROUP_5_ID, GROUP_6_ID));
        assertThat(memberCalculator.getEmptyGroupIds(groupDAO), containsInAnyOrder(
                GROUP_1_ID));
        // Group 1 got updated during regroup and now it should be empty
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_1_ID), false),
                equalTo(Collections.emptySet()));
        // Group 2 updated during regroup and it should have new groups
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_2_ID), false),
                equalTo(ImmutableSet.of(101L, 104L, 106L)));
        // Group 3 is no longer discovered, so if we ask for its members we should return emtpy
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_3_ID), false),
                equalTo(Collections.emptySet()));
        // Group 4 got deleted by the user during regroup so if we get its memeber, we should
        // return empty
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_4_ID), false),
                equalTo(Collections.emptySet()));
        // Group 5 members got updated as a result of regroup. Now it should have the new members
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_5_ID), false),
                equalTo(ImmutableSet.of(204L, 205L)));
        // Group 6 got created and updated during regroup. Now it should have the latest members.
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_6_ID), false),
                equalTo(ImmutableSet.of(105L, 106L)));
        parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                .of(100L, 101L, 102L, 103L, 104L, 105L, 106L, 203L, 204L, 205L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(Collections.emptySet()));
        assertThat(parents.get(101L), equalTo(Collections.singleton(GROUP_2_ID)));
        assertThat(parents.get(102L), equalTo(Collections.emptySet()));
        assertThat(parents.get(103L), equalTo(Collections.emptySet()));
        assertThat(parents.get(104L), equalTo(Collections.singleton(GROUP_2_ID)));
        assertThat(parents.get(105L), equalTo(Collections.singleton(GROUP_6_ID)));
        assertThat(parents.get(106L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_6_ID)));
        assertThat(parents.get(203L), equalTo(Collections.emptySet()));
        assertThat(parents.get(204L), equalTo(ImmutableSet.of(GROUP_5_ID)));
        assertThat(parents.get(205L), equalTo(ImmutableSet.of(GROUP_5_ID)));
    }

    /**
     * Tests the case that regroup fails.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testRegroupFailure() throws Exception {
        // ARRANGE
        final AtomicBoolean returnFailure = new AtomicBoolean(false);

        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, true, threadFactory,
                () -> {
                    final Long2ShortOpenHashMap groupToType = createGroupToTypeMap(false);
                    final Long2ObjectOpenHashMap<CachedGroupMembers> groupIdToMemberIds =
                            createGroupIdToMemberIds(false);
                    final Long2ObjectOpenHashMap<LongSet> entityIdToParentGroupIds =
                            createEntityIdToParentGroupIds(false);
                    if (!returnFailure.get()) {
                        return ImmutableGroupMembershipRelationships.builder()
                                .success(true)
                                .groupIdToGroupMemberIdsMap(groupIdToMemberIds)
                                .entityIdToGroupIdsMap(entityIdToParentGroupIds)
                                .groupIdToType(groupToType)
                                .build();
                    } else {
                        return ImmutableGroupMembershipRelationships.builder()
                                .success(false)
                                .build();
                    }

                });
        memberCalculator.regroup();
        returnFailure.set(true);

        // ACT
        final CachingMemberCalculator.RegroupingResult regroupResult = memberCalculator.regroup();

        // ASSERT
        assertFalse(regroupResult.isSuccessfull());
        assertThat(Arrays.asList(memberCalculator.getCachedGroupIds().toArray()),
                containsInAnyOrder(GROUP_1_ID, GROUP_2_ID, GROUP_3_ID, GROUP_4_ID, GROUP_5_ID));
        assertThat(memberCalculator.getEmptyGroupIds(groupDAO), containsInAnyOrder(GROUP_3_ID));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_1_ID), false),
                equalTo(ImmutableSet.of(100L, 101L, 102L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_2_ID), false),
                equalTo(ImmutableSet.of(101L, 103L, 104L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_3_ID), false),
                equalTo(Collections.emptySet()));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_4_ID), false),
                equalTo(ImmutableSet.of(100L, 101L, 102L, 103L, 104L)));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(GROUP_5_ID), false),
                equalTo(ImmutableSet.of(203L, 204L)));
        Map<Long, Set<Long>> parents = memberCalculator.getEntityGroups(groupDAO, ImmutableSet
                .of(100L, 101L, 102L, 103L, 104L, 105L, 203L, 204L), Collections.emptySet());
        assertThat(parents.get(100L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_4_ID)));
        assertThat(parents.get(101L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(102L), equalTo(ImmutableSet.of(GROUP_1_ID, GROUP_4_ID)));
        assertThat(parents.get(103L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(104L), equalTo(ImmutableSet.of(GROUP_2_ID, GROUP_4_ID)));
        assertThat(parents.get(105L), equalTo(Collections.emptySet()));
        assertThat(parents.get(203L), equalTo(ImmutableSet.of(GROUP_5_ID)));
        assertThat(parents.get(204L), equalTo(ImmutableSet.of(GROUP_5_ID)));
    }

    /**
     * Create a map of group types as a result of regroup.
     *
     * @param returnNewValue this is used to simulate changes in regroup in consequent regroups.
     * @return the map from group id to type.
     */
    private Long2ShortOpenHashMap createGroupToTypeMap(final boolean returnNewValue) {
        final Long2ShortOpenHashMap groupToType = new Long2ShortOpenHashMap();
        groupToType.put(GROUP_1_ID, (short)CommonDTO.GroupDTO.GroupType.REGULAR.getNumber());
        groupToType.put(GROUP_2_ID, (short)CommonDTO.GroupDTO.GroupType.REGULAR.getNumber());
        if (!returnNewValue) {
            groupToType.put(GROUP_3_ID, (short)CommonDTO.GroupDTO.GroupType.RESOURCE.getNumber());
        }
        groupToType.put(GROUP_4_ID, (short)CommonDTO.GroupDTO.GroupType.REGULAR.getNumber());
        groupToType.put(GROUP_5_ID, (short)CommonDTO.GroupDTO.GroupType.RESOURCE.getNumber());
        return groupToType;
    }

    private Long2ObjectOpenHashMap<CachedGroupMembers> createGroupIdToMemberIds(
            final boolean returnNewValue) {
        final Long2ObjectOpenHashMap<CachedGroupMembers> groupIdToMemberIds =
                new Long2ObjectOpenHashMap<>();
        CachedGroupMembers group1Members = Type.SET.getFactory().get();
        group1Members.set(Arrays.asList(100L, 101L, 102L));
        groupIdToMemberIds.put(GROUP_1_ID, group1Members);
        CachedGroupMembers group2Members = Type.SET.getFactory().get();
        group2Members.set(Arrays.asList(101L, 103L, 104L));
        groupIdToMemberIds.put(GROUP_2_ID, group2Members);
        if (!returnNewValue) {
            CachedGroupMembers group3Members = Type.SET.getFactory().get();
            group3Members.set(Arrays.asList());
            groupIdToMemberIds.put(GROUP_3_ID, group3Members);
        }
        CachedGroupMembers group4Members = Type.SET.getFactory().get();
        group4Members.set(Arrays.asList(100L, 101L, 102L, 103L, 104L));
        groupIdToMemberIds.put(GROUP_4_ID, group4Members);
        CachedGroupMembers group5Members = Type.SET.getFactory().get();
        if (!returnNewValue) {
            group5Members.set(Arrays.asList(203L, 204L));
        } else {
            group5Members.set(Arrays.asList(204L, 205L));
        }
        groupIdToMemberIds.put(GROUP_5_ID, group5Members);
        return groupIdToMemberIds;
    }

    private Long2ObjectOpenHashMap<LongSet> createEntityIdToParentGroupIds(
            final boolean returnNewValue) {
        final Long2ObjectOpenHashMap<LongSet> entityIdToParentGroupIds =
                new Long2ObjectOpenHashMap<>();
        entityIdToParentGroupIds.put(100L, new LongOpenHashSet(Arrays.asList(GROUP_1_ID, GROUP_4_ID)));
        entityIdToParentGroupIds.put(101L, new LongOpenHashSet(Arrays.asList(GROUP_1_ID,
                GROUP_2_ID, GROUP_4_ID)));
        entityIdToParentGroupIds.put(102L, new LongOpenHashSet(Arrays.asList(GROUP_1_ID, GROUP_4_ID)));
        entityIdToParentGroupIds.put(103L, new LongOpenHashSet(Arrays.asList(GROUP_2_ID, GROUP_4_ID)));
        entityIdToParentGroupIds.put(104L, new LongOpenHashSet(Arrays.asList(GROUP_2_ID, GROUP_4_ID)));
        entityIdToParentGroupIds.put(204L, new LongOpenHashSet(Arrays.asList(GROUP_5_ID)));
        if (!returnNewValue) {
            entityIdToParentGroupIds.put(203L, new LongOpenHashSet(Arrays.asList(GROUP_5_ID)));
        } else {
            entityIdToParentGroupIds.put(205L, new LongOpenHashSet(Arrays.asList(GROUP_5_ID)));
        }

        return entityIdToParentGroupIds;
    }
}
