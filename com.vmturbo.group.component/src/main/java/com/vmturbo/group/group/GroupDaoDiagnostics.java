package com.vmturbo.group.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import io.grpc.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;

/**
 * Group DAO diagnostics provider. This class is responsible for loading and dumping diagnostics
 * from group store.
 */
public class GroupDaoDiagnostics implements DiagsRestorable {

    /**
     * The file name for the groups dump collected from the {@link com.vmturbo.group.group.IGroupStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    private static final String GROUPS_DUMP_FILE = "groups_dump";

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
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        try {
            transactionProvider.transaction(
                    stores -> collectGroupsOrdered(stores.getGroupStore(), appender));
        } catch (StoreOperationException e) {
            if (e.getCause() instanceof DiagnosticsException) {
                throw (DiagnosticsException)e.getCause();
            }
            logger.error("Failed to get diags", e);
            throw new DiagnosticsException(Collections.singletonList(e.getMessage()));
        }
    }

    @Nonnull
    private int collectGroupsOrdered(@Nonnull IGroupStore groupStore,
            @Nonnull DiagnosticsAppender appender) throws StoreOperationException {
        final Collection<Grouping> allGroups = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder().build());
        // We need to sort all the groups so that dependent groups will go after the groups
        // they depend on. Otherwise, loading these groups will throw an exception.
        final List<Grouping> sortedGroups =
                CollectionUtils.sortWithDependencies(allGroups, Grouping::getId,
                        grouping -> getGroupStaticSubGroups(grouping.getDefinition()));
        try {
            final int discoveredGroups =
                    dumpGroups(sortedGroups, grouping -> grouping.getOrigin().hasDiscovered(),
                            appender);
            final int notDiscoveredGroups =
                    dumpGroups(sortedGroups, grouping -> !grouping.getOrigin().hasDiscovered(),
                            appender);
            logger.info("Collected diags for {} discovered groups and {} created groups.",
                    discoveredGroups, notDiscoveredGroups);
            return discoveredGroups + notDiscoveredGroups;
        } catch (DiagnosticsException e) {
            throw new StoreOperationException(Status.INTERNAL, "Diagnostics failure occurred", e);
        }
    }

    private int dumpGroups(@Nonnull Collection<Grouping> sortedGroups,
            @Nonnull Predicate<Grouping> filter, @Nonnull DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        final Iterator<Grouping> groups = sortedGroups.stream().filter(filter).iterator();
        int counter = 0;
        while (groups.hasNext()) {
            final Grouping discoveredGroup =  groups.next();
            appender.appendString(gson.toJson(discoveredGroup));
            counter++;
        }
        return counter;
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
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        // Replace all existing groups with the ones in the collected diags.
        final Collection<Grouping> allGroups = collectedDiags.stream()
                .map(string -> gson.fromJson(string, Grouping.class))
                .collect(Collectors.toList());
        groupStore.deleteAllGroups();
        logger.info("Removed all the groups.");
        final Collection<DiscoveredGroup> discoveredGroupsConverted =
                new ArrayList<>(allGroups.size());
        final Collection<Grouping> nonDiscoveredGroups = new ArrayList<>(allGroups.size());
        for (GroupDTO.Grouping group : allGroups) {
            if (group.getOrigin().hasDiscovered()) {
                final Origin.Discovered origin = group.getOrigin().getDiscovered();
                final DiscoveredGroup discoveredGroup =
                        new DiscoveredGroup(group.getId(), group.getDefinition(), origin.getSourceIdentifier(),
                                new HashSet<>(origin.getDiscoveringTargetIdList()), group.getExpectedTypesList(),
                                group.getSupportsMemberReverseLookup());
                discoveredGroupsConverted.add(discoveredGroup);
            } else {
                nonDiscoveredGroups.add(group);
            }
        }
        logger.info(
                "Attempting to restore {} discovered groups and {} non-discovered groups from diagnostics.",
                discoveredGroupsConverted.size(), nonDiscoveredGroups.size());
        groupStore.updateDiscoveredGroups(discoveredGroupsConverted, Collections.emptyList(),
                Collections.emptySet());
        for (GroupDTO.Grouping group : nonDiscoveredGroups) {
            groupStore.createGroup(group.getId(), group.getOrigin(), group.getDefinition(),
                    new HashSet<>(group.getExpectedTypesList()),
                    group.getSupportsMemberReverseLookup());
        }
        logger.info("Restored {} groups from diagnostics", allGroups.size());
    }

    @Nonnull
    @Override
    public String getFileName() {
        return GROUPS_DUMP_FILE;
    }
}
