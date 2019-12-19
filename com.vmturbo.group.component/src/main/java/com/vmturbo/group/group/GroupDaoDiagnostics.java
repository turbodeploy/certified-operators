package com.vmturbo.group.group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;

/**
 * Group DAO diagnostics provider. This class is responsible for loading and dumping diagnostics
 * from group store.
 */
public class GroupDaoDiagnostics implements Diagnosable {

    private final TransactionProvider transactionProvider;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs diagnostics provider with the specified transaction provider.
     *
     * @param transactionProvider transaction provider
     */
    public GroupDaoDiagnostics(@Nonnull TransactionProvider transactionProvider) {
        this.transactionProvider = Objects.requireNonNull(transactionProvider);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<String> collectDiagsStream() throws DiagnosticsException {
        try {
            final List<String> diags = transactionProvider.transaction(
                    stores -> collectDiagsStream(stores.getGroupStore()));
            return diags.stream();
        } catch (StoreOperationException e) {
            logger.error("Failed to get diags", e);
            throw new DiagnosticsException(Collections.singletonList(e.getMessage()));
        }
    }

    @Nonnull
    private List<String> collectDiagsStream(@Nonnull IGroupStore groupStore) {
        final Collection<Grouping> allGroups = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder().build());
        // We need to sort all the groups so that dependent groups will go after the groups
        // they depend on. Otherwise, loading these groups will throw an exception.
        final List<Grouping> sortedGroups =
                CollectionUtils.sortWithDependencies(allGroups, Grouping::getId,
                        grouping -> getGroupStaticSubGroups(grouping.getDefinition()));
        final List<Grouping> discovered = sortedGroups.stream()
                .filter(grouping -> grouping.getOrigin().hasDiscovered())
                .collect(Collectors.toList());
        final List<Grouping> notDiscovered = sortedGroups.stream()
                .filter(grouping -> !grouping.getOrigin().hasDiscovered())
                .collect(Collectors.toList());
        logger.info("Collected diags for {} discovered groups and {} created groups.",
                discovered.size(), notDiscovered.size());

        return Arrays.asList(ComponentGsonFactory.createGsonNoPrettyPrint().toJson(discovered),
                ComponentGsonFactory.createGsonNoPrettyPrint().toJson(notDiscovered));
    }

    private Set<Long> getGroupStaticSubGroups(@Nonnull GroupDefinition group) {
        if (!group.hasStaticGroupMembers()) {
            return Collections.emptySet();
        } else {
            final Set<Long> children = new HashSet<>();
            for (StaticMembersByType membersByType : group.getStaticGroupMembers()
                    .getMembersByTypeList()) {
                if (membersByType.getType().hasGroup()) {
                    children.addAll(membersByType.getMembersList());
                }
            }
            return children;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        try {
            transactionProvider.transaction(stores -> {
                restoreDiags(collectedDiags, stores.getGroupStore());
                return null;
            });
        } catch (StoreOperationException e) {
            logger.error("Failed to restore diags", e);
            throw new DiagnosticsException(Collections.singletonList(e.getMessage()));
        }
    }

    private void restoreDiags(@Nonnull List<String> collectedDiags, @Nonnull IGroupStore groupStore)
            throws StoreOperationException {
        // Replace all existing groups with the ones in the collected diags.
        final Collection<GroupDTO.Grouping> discoveredGroups =
                ComponentGsonFactory.createGsonNoPrettyPrint()
                        .fromJson(collectedDiags.get(0),
                                new TypeToken<Collection<Grouping>>() {}.getType());
        final Collection<GroupDTO.Grouping> nonDiscoveredGroups =
                ComponentGsonFactory.createGsonNoPrettyPrint()
                        .fromJson(collectedDiags.get(1),
                                new TypeToken<Collection<GroupDTO.Grouping>>() {}.getType());
        logger.info(
                "Attempting to restore {} discovered groups and {} non-discovered groups from diagnostics.",
                discoveredGroups.size(), nonDiscoveredGroups.size());

        groupStore.deleteAllGroups();
        logger.info("Removed all the groups.");
        final Collection<DiscoveredGroup> discoveredGroupsConverted =
                new ArrayList<>(discoveredGroups.size());
        for (GroupDTO.Grouping group : discoveredGroups) {
            final Origin.Discovered origin = group.getOrigin().getDiscovered();
            final DiscoveredGroup discoveredGroup =
                    new DiscoveredGroup(group.getId(), group.getDefinition(),
                            origin.getSourceIdentifier(),
                            new HashSet<>(origin.getDiscoveringTargetIdList()),
                            group.getExpectedTypesList(), group.getSupportsMemberReverseLookup());
            discoveredGroupsConverted.add(discoveredGroup);
        }
        groupStore.updateDiscoveredGroups(discoveredGroupsConverted, Collections.emptyList(),
                Collections.emptySet());
        for (GroupDTO.Grouping group : nonDiscoveredGroups) {
            groupStore.createGroup(group.getId(), group.getOrigin(), group.getDefinition(),
                    new HashSet<>(group.getExpectedTypesList()),
                    group.getSupportsMemberReverseLookup());
        }
        logger.info("Restored groups {} from diagnostics",
                discoveredGroups.size() + nonDiscoveredGroups.size());
    }
}
