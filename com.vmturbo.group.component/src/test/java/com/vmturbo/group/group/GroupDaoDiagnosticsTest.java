package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.service.MockTransactionProvider;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit test for {@link GroupDaoDiagnostics}.
 */
public class GroupDaoDiagnosticsTest {

    private final TestGroupGenerator groupGenerator = new TestGroupGenerator();
    private static final Set<MemberType> EXPECTED_MEMBERS =
            ImmutableSet.of(MemberType.newBuilder().setEntity(1).build(),
                    MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                    MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build());

    /**
     * Tests how the data is restored from diagnostics. All the groups dumped initially should
     * be restored using {@link StringDiagnosable} interfaces
     * call.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDiags() throws Exception {
        final MockTransactionProvider transactionProvider = new MockTransactionProvider();
        final IGroupStore groupStore = transactionProvider.getGroupStore();
        final GroupDaoDiagnostics diagnostics = new GroupDaoDiagnostics(transactionProvider);
        final String src1 = "discovered-group-1";
        final String src2 = "discovered-group-2";
        final Grouping group1 = createUploadedGroup(src1, Arrays.asList(1L, 2L));
        final Grouping group2 = createUploadedGroup(src2, Arrays.asList(1L, 3L));
        final Grouping groupNew = createUploadedGroup(src2, Arrays.asList(1L, 3L));
        final Grouping userGroup = createUserGroup(group1.getId(), group2.getId());
        final Grouping userParentGroup = createUserGroup(userGroup.getId());

        Mockito.when(groupStore.getGroups(GroupFilter.newBuilder().build()))
                .thenReturn(Arrays.asList(userParentGroup, userGroup, group1, group2, groupNew));

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        diagnostics.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        Mockito.verify(groupStore, Mockito.never()).deleteAllGroups();

        diagnostics.restoreDiags(diags.getAllValues(), groupStore);
        ArgumentCaptor<Long> oidCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<GroupDefinition> defCaptor = ArgumentCaptor.forClass(GroupDefinition.class);
        Mockito.verify(groupStore)
                .updateDiscoveredGroups(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(groupStore, Mockito.times(2))
                .createGroup(oidCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.any(),
                        Mockito.anyBoolean());
        Assert.assertEquals(Arrays.asList(userGroup.getId(), userParentGroup.getId()),
                oidCaptor.getAllValues());
    }

    @Nonnull
    private Grouping createUploadedGroup(@Nonnull String srcId,
            @Nonnull Collection<Long> targetIds) {
        final DiscoveredGroup discoveredGroup =
                groupGenerator.createUploadedGroup(srcId, targetIds);
        return Grouping.newBuilder()
                .setId(discoveredGroup.getOid())
                .setOrigin(Origin.newBuilder()
                        .setDiscovered(Origin.Discovered.newBuilder()
                                .setSourceIdentifier(srcId)
                                .addAllDiscoveringTargetId(targetIds)))
                .setDefinition(discoveredGroup.getDefinition())
                .build();
    }

    @Nonnull
    private Grouping createUserGroup(Long... children) {
        final GroupDefinition definition =
                GroupDefinition.newBuilder(groupGenerator.createGroupDefinition())
                        .clearStaticGroupMembers()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(
                                                MemberType.newBuilder().setGroup(GroupType.REGULAR))
                                        .addAllMembers(Arrays.asList(children))))
                        .build();
        return Grouping.newBuilder()
                .setId(groupGenerator.nextOid())
                .setDefinition(definition)
                .setOrigin(groupGenerator.createUserOrigin())
                .addAllExpectedTypes(EXPECTED_MEMBERS)
                .setSupportsMemberReverseLookup(true)
                .build();
    }
}
