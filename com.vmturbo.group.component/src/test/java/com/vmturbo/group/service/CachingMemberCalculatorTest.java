package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator.CachedGroupMembers;
import com.vmturbo.group.service.CachingMemberCalculator.CachedGroupMembers.Type;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for {@link CachingMemberCalculator}.
 */
public class CachingMemberCalculatorTest {

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
                internalCalculator, Type.SET, true, threadFactory);

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
     * Test that given two groups g1 and g2, such that g1 is a member of g2 and g2 is a member of
     * g1 we can resolve them properly without ending up in a infinite loop.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRecursiveGroupResolution() throws Exception {
        final CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
            internalCalculator, Type.SET, true, threadFactory);

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
                internalCalculator, Type.SET, true, threadFactory);

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
                internalCalculator, Type.SET, true, threadFactory);
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
                internalCalculator, Type.SET, true, threadFactory);

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
                internalCalculator, Type.SET, true, threadFactory);

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
                internalCalculator, Type.SET, true, threadFactory);
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
            internalCalculator, Type.SET, true, threadFactory);

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
}
