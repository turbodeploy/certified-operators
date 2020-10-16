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
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

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
        Set<Long> g1Members = Sets.newHashSet(8L, 9L);
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
        memberCalculator.onSourceTopologyAvailable(TOPOLOGY_ID, CONTEXT_ID);

        // ASSERT
        verify(internalCalculator, times(2))
            .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());

        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false),
            is(g1Members));
        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g2.getId()), false),
            is(g2Members));

        // The internal calculator shouldn't have gotten called anymore, since we cached the
        // members.
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
                internalCalculator, Type.SET, false, threadFactory);

        Grouping parentGroup = Grouping.newBuilder()
            .setId(1)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("foo"))
            .build();
        Grouping childGroup = Grouping.newBuilder()
            .setId(2)
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("bar"))
            .build();
        // The child group, plus an entity.
        Set<Long> parentGroupMembers = Sets.newHashSet(childGroup.getId(), 9L);
        Set<Long> childGroupMembers = Sets.newHashSet(10L, 11L);
        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(parentGroup.getId()), false))
            .thenReturn(parentGroupMembers);
        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(childGroup.getId()), false))
            .thenReturn(childGroupMembers);
        when(groupDAO.getGroups(any())).thenReturn(Arrays.asList(parentGroup, childGroup));
        when(groupDAO.getGroupIds(any())).thenReturn(Sets.newHashSet(parentGroup.getId(), childGroup.getId()));

        memberCalculator.onSourceTopologyAvailable(TOPOLOGY_ID, CONTEXT_ID);

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

        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, groupDefinition, false);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
            Collections.singleton(1L), false), is(members));

        // No additional query to the underlying calculator.
        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, groupDefinition, false);
    }

    /**
     * Test that newly created user group notifications do not cache the group members if
     * doRegrouping = false.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupCreatedNoRegrouping() throws Exception {
        // Regrouping set to "false" to avoid eager caching.because otherwise we don't eagerly cache created groups.
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, false, threadFactory);
        GroupDefinition groupDefinition = GroupDefinition.newBuilder()
                .setDisplayName("foo")
                .build();

        Set<Long> members = Sets.newHashSet(9L, 10L);
        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(1L), false))
                .thenReturn(members);


        memberCalculator.onUserGroupCreated(1L, groupDefinition);

        verifyZeroInteractions(internalCalculator);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(1L), false), is(members));

        // Underlying query.
        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, Collections.singleton(1L), false);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(1L), false), is(members));

        // No additional query to the underlying calculator.
        verify(internalCalculator, times(1)).getGroupMembers(groupDAO, Collections.singleton(1L), false);
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
        memberCalculator.onSourceTopologyAvailable(TOPOLOGY_ID, CONTEXT_ID);

        GroupDefinition updatedDef = GroupDefinition.newBuilder()
                .setDisplayName("foo1")
                .build();
        Set<Long> updatedMembers = Sets.newHashSet(10L, 11L);

        when(memberCalculator.getGroupMembers(groupDAO, updatedDef, false))
                .thenReturn(updatedMembers);

        // Trigger group update.
        memberCalculator.onUserGroupUpdated(g1.getId(), updatedDef);

        // ASSERT
        // Internal calculator should have been calculated once on the first regrouping,
        // and once on update.
        verify(internalCalculator, times(2))
                .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());

        assertThat(memberCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false),
                is(updatedMembers));

        // The internal calculator shouldn't have gotten called anymore, since we cached the
        // members.
        verify(internalCalculator, times(2))
                .getGroupMembers(any(IGroupStore.class), any(GroupDefinition.class), anyBoolean());
    }

    /**
     * Test that updated user group notifications do not cache the updated group members if
     * doRegrouping = false.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupUpdatedNoRegrouping() throws Exception {
        // No regrouping.
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, false, threadFactory);

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

        GroupDefinition updatedDef = GroupDefinition.newBuilder()
                .setDisplayName("foo1")
                .build();
        Set<Long> updatedMembers = Sets.newHashSet(10L, 11L);

        when(internalCalculator.getGroupMembers(groupDAO, Collections.singleton(g1.getId()), false))
                .thenReturn(updatedMembers);

        // Trigger group update.
        memberCalculator.onUserGroupUpdated(g1.getId(), updatedDef);

        assertThat(memberCalculator.getGroupMembers(groupDAO,
                Collections.singleton(g1.getId()), false), is(updatedMembers));
    }

    /**
     * Test that if a user group is deleted we remove it from the cache.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUserGroupDeleted() throws Exception {
        CachingMemberCalculator memberCalculator = new CachingMemberCalculator(groupDAO,
                internalCalculator, Type.SET, false, threadFactory);

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