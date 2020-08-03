package com.vmturbo.group.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.User;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class TemporaryGroupCacheTest {

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final long GROUP_ID = 7L;

    private final GroupDefinition testGrouping = GroupDefinition.newBuilder()
                    .setType(GroupType.REGULAR)
                    .setDisplayName("TestGroup")
                    .setStaticGroupMembers(StaticMembers
                                    .newBuilder()
                                    .addMembersByType(StaticMembersByType
                                                    .newBuilder()
                                                    .setType(MemberType
                                                                .newBuilder()
                                                                .setEntity(2)
                                                            )
                                                    .addAllMembers(Arrays.asList(101L, 102L))
                                                    )
                                    )
                    .build();

    final GroupDTO.Origin origin = GroupDTO.Origin
                    .newBuilder()
                    .setUser(User
                                .newBuilder()
                                .setUsername("administrator")
                            )
                    .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
    }

    /**
     * Tests the creation, getting, and deletion of a group.
     * @throws InvalidTempGroupException when something goes wrong.
     */
    @Test
    public void testCreateGetDeleteGrouping() throws InvalidTempGroupException {
        final TemporaryGroupCache temporaryGroupCache =
                        new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);
        final Collection<MemberType> expectedTypes = ImmutableSet
                        .of(MemberType.newBuilder().setEntity(2).build());
        final Grouping grouping = temporaryGroupCache.create(testGrouping, origin,
                        expectedTypes);
        final Grouping expectedGrouping = Grouping.newBuilder()
                        .setId(GROUP_ID)
                        .setDefinition(testGrouping)
                        .setOrigin(origin)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(false)
                        .build();

        assertEquals(expectedGrouping, grouping);
        assertEquals(grouping, temporaryGroupCache.getGrouping(GROUP_ID).get());
        assertEquals(grouping, temporaryGroupCache.deleteGrouping(GROUP_ID).get());
        assertFalse(temporaryGroupCache.getGrouping(GROUP_ID).isPresent());
    }
}
