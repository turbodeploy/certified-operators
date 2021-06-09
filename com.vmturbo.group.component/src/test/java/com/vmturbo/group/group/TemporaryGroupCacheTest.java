package com.vmturbo.group.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.User;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class TemporaryGroupCacheTest {

    private IdentityProvider identityProvider = mock(IdentityProvider.class);
    private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver =
            mock(GroupEnvironmentTypeResolver.class);
    private final GroupSeverityCalculator groupSeverityCalculator =
            mock(GroupSeverityCalculator.class);

    private static final long GROUP_ID = 7L;

    private final long entityoid1 = 101L;
    private final long entityoid2 = 102L;
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
                                                    .addAllMembers(
                                                            Arrays.asList(entityoid1, entityoid2))
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
     */
    @Test
    public void testCreateGetDeleteGrouping() {
        final TemporaryGroupCache temporaryGroupCache = new TemporaryGroupCache(identityProvider,
                groupEnvironmentTypeResolver, groupSeverityCalculator, 1, TimeUnit.MINUTES);
        final Collection<MemberType> expectedTypes = ImmutableSet
                        .of(MemberType.newBuilder().setEntity(2).build());
        EntityWithOnlyEnvironmentTypeAndTargets entity =
                EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                        .setOid(entityoid1)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build();
        final GroupEnvironment environment =
                new GroupEnvironment(EnvironmentType.CLOUD, CloudType.AWS);
        final Severity severity = Severity.MINOR;
        when(groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(anyLong(), any(), any()))
                .thenReturn(environment);
        when(groupSeverityCalculator.calculateSeverity(Collections.singleton(entityoid1)))
                .thenReturn(severity);
        final Grouping grouping = temporaryGroupCache.create(testGrouping, origin, expectedTypes,
                Collections.singletonList(PartialEntity.newBuilder()
                        .setWithOnlyEnvironmentTypeAndTargets(entity)
                        .build()));
        final Grouping expectedGrouping = Grouping.newBuilder()
                        .setId(GROUP_ID)
                        .setDefinition(testGrouping)
                        .setOrigin(origin)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(false)
                        .setEnvironmentType(environment.getEnvironmentType())
                        .setCloudType(environment.getCloudType())
                        .setSeverity(severity)
                        .build();

        assertEquals(expectedGrouping, grouping);
        assertEquals(grouping, temporaryGroupCache.getGrouping(GROUP_ID).get());
        assertEquals(grouping, temporaryGroupCache.deleteGrouping(GROUP_ID).get());
        assertFalse(temporaryGroupCache.getGrouping(GROUP_ID).isPresent());
    }

    /**
     * Tests that when environment type in the optimization metadata is provided, the environment
     * type of the new temp group is set to that value.
     */
    @Test
    public void testCreateWithOptimizationMetadata() {
        // GIVEN
        final TemporaryGroupCache temporaryGroupCache = new TemporaryGroupCache(identityProvider,
                groupEnvironmentTypeResolver, groupSeverityCalculator, 1, TimeUnit.MINUTES);
        final Collection<MemberType> expectedTypes = ImmutableSet
                .of(MemberType.newBuilder().setEntity(2).build());
        final GroupDefinition groupDef = GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("TestGroup")
                .setStaticGroupMembers(StaticMembers
                        .newBuilder()
                        .addMembersByType(StaticMembersByType
                                .newBuilder()
                                .setType(MemberType.newBuilder().setEntity(2))
                                .addAllMembers(Arrays.asList(entityoid1, entityoid2))))
                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build())
                .build();

        EntityWithOnlyEnvironmentTypeAndTargets entity1 =
                EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                        .setOid(entityoid1)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build();
        EntityWithOnlyEnvironmentTypeAndTargets entity2 =
                EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                        .setOid(entityoid2)
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .build();
        List<PartialEntity> inputlist = new ArrayList<>();
        inputlist.add(
                PartialEntity.newBuilder().setWithOnlyEnvironmentTypeAndTargets(entity1).build());
        inputlist.add(
                PartialEntity.newBuilder().setWithOnlyEnvironmentTypeAndTargets(entity2).build());
        final GroupEnvironment environment =
                new GroupEnvironment(EnvironmentType.HYBRID, CloudType.AWS);
        final Severity severity = Severity.MINOR;
        when(groupEnvironmentTypeResolver
                .getEnvironmentAndCloudTypeForGroup(anyLong(), any(), any()))
                .thenReturn(environment);
        when(groupSeverityCalculator.calculateSeverity(any())).thenReturn(severity);

        // WHEN
        final Grouping grouping = temporaryGroupCache.create(groupDef, origin, expectedTypes,
                inputlist);

        // THEN
        assertEquals(EnvironmentType.CLOUD, grouping.getEnvironmentType());
    }

    /**
     * Tests that when we don't have information about the group's entities, we fall back to
     * defaults.
     */
    @Test
    public void testCreateWithNullEntityInfo() {
        // GIVEN
        final TemporaryGroupCache temporaryGroupCache = new TemporaryGroupCache(identityProvider,
                groupEnvironmentTypeResolver, groupSeverityCalculator, 1, TimeUnit.MINUTES);
        final Collection<MemberType> expectedTypes = ImmutableSet
                .of(MemberType.newBuilder().setEntity(2).build());
        final GroupDefinition groupDef = GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("TestGroup")
                .setStaticGroupMembers(StaticMembers
                        .newBuilder()
                        .addMembersByType(StaticMembersByType
                                .newBuilder()
                                .setType(MemberType.newBuilder().setEntity(2))
                                .addAllMembers(Arrays.asList(entityoid1, entityoid2))))
                .build();

        // WHEN
        final Grouping grouping = temporaryGroupCache.create(groupDef, origin, expectedTypes, null);

        // THEN
        assertEquals(EnvironmentType.UNKNOWN_ENV, grouping.getEnvironmentType());
        assertEquals(CloudType.UNKNOWN_CLOUD, grouping.getCloudType());
        assertEquals(Severity.NORMAL, grouping.getSeverity());
    }
}
