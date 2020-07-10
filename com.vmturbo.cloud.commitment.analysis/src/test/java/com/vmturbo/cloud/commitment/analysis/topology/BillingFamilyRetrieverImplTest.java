package com.vmturbo.cloud.commitment.analysis.topology;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;

/**
 * Class for testing BillingFamilyRetriever.
 */
public class BillingFamilyRetrieverImplTest {

    private final GroupMemberRetriever groupMemberRetriever = mock(GroupMemberRetriever.class);

    private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory =
            new DefaultBillingFamilyRetrieverFactory(groupMemberRetriever);

    /**
     * Test billing family for account.
     */
    @Test
    public void testGetBillingFamilyForAccount() {

        // setup billing family groups
        final Grouping groupingA = Grouping.newBuilder()
                .setId(1L)
                .build();
        final Set<Long> entitiesA = ImmutableSet.of(2L, 3L);
        final GroupAndMembers groupAndMembersA = ImmutableGroupAndMembers.builder()
                .group(groupingA)
                .entities(entitiesA)
                .members(entitiesA)
                .build();

        final Grouping groupingB = Grouping.newBuilder()
                .setId(4L)
                .build();
        final Set<Long> entitiesB = ImmutableSet.of(5L, 6L);
        final GroupAndMembers groupAndMembersB = ImmutableGroupAndMembers.builder()
                .group(groupingB)
                .entities(entitiesB)
                .members(entitiesB)
                .build();

        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(
                Lists.newArrayList(groupAndMembersA, groupAndMembersB));

        final BillingFamilyRetriever billingFamilyRetriever = billingFamilyRetrieverFactory.newInstance();

        // check assertions
        assertThat(billingFamilyRetriever.getBillingFamilyForAccount(2L),
                equalTo(Optional.of(groupAndMembersA)));
        assertThat(billingFamilyRetriever.getBillingFamilyForAccount(5L),
                equalTo(Optional.of(groupAndMembersB)));
        assertThat(billingFamilyRetriever.getBillingFamilyForAccount(1L),
                equalTo(Optional.empty()));
    }
}
