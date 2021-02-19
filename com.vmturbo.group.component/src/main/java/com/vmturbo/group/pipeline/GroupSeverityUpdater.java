package com.vmturbo.group.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Class responsible for updating the severity for all groups.
 *
 * <p>Severity updates happen also on the following cases:
 * - After a new source topology has been announced - all groups should be updated since group
 * membership might have changed.
 * (See {@link com.vmturbo.group.pipeline.Stages.StoreSupplementaryGroupInfoStage})
 * - After a user group has been created or updated - this specific group should be updated.
 * (See {@link com.vmturbo.group.service.GroupRpcService}
 */
public class GroupSeverityUpdater {

    private static final Logger logger = LogManager.getLogger();

    private final CachingMemberCalculator memberCache;

    private final GroupSeverityCalculator severityCalculator;

    private final IGroupStore groupStore;

    /**
     * Constructor.
     *
     * @param memberCache group membership cache.
     * @param severityCalculator calculates severity for groups.
     * @param groupStore for database operations.
     */
    public GroupSeverityUpdater(@Nonnull final CachingMemberCalculator memberCache,
            @Nonnull final GroupSeverityCalculator severityCalculator,
            @Nonnull final IGroupStore groupStore) {
        this.memberCache = memberCache;
        this.severityCalculator = severityCalculator;
        this.groupStore = groupStore;
    }

    /**
     * Updates severity for all groups.
     */
    public void refreshGroupSeverities() {
        final StopWatch stopWatch = new StopWatch("groupSeverityRefresh");
        Collection<GroupSupplementaryInfo> groupsToUpdate = new ArrayList<>();
        Map<Severity, Long> groupCountsBySeverity = new EnumMap<>(Severity.class);
        LongSet groupIds = memberCache.getCachedGroupIds();
        stopWatch.start("refresh");
        for (long groupId : groupIds) {
            // get group's members
            final Set<Long> groupEntities;
            try {
                groupEntities = memberCache.getGroupMembers(groupStore,
                        Collections.singleton(groupId), true);
            } catch (RuntimeException | StoreOperationException e) {
                logger.error("Skipping severity update for group with uuid: " + groupId
                        + " due to failure to retrieve its entities. Error: ", e);
                continue;
            }
            // calculate severity based on members' severity
            Severity groupSeverity = severityCalculator.calculateSeverity(groupEntities);
            // try to insert 1. if a value is already there, add 1 to it
            groupCountsBySeverity.merge(groupSeverity, 1L, Long::sum);
            // emptiness, environment & cloud type are ignored during severity updates, so here
            // we fill them with dummy values
            groupsToUpdate.add(new GroupSupplementaryInfo(groupId, false, 0, 0,
                    groupSeverity.getNumber()));
        }
        // update database records in a batch
        final int updatedGroups = groupStore.updateBulkGroupsSeverity(groupsToUpdate);
        stopWatch.stop();
        logger.info("Successfully refreshed severities for {} (out of {}) groups in {} ms. Breakdown:\n{}",
                updatedGroups,
                groupIds.size(),
                stopWatch.getLastTaskTimeMillis(),
                severitiesBreakdown(groupCountsBySeverity));
    }

    /**
     * Returns a structured representation of severity counts.
     *
     * @param groupCountsBySeverity group counts broken down by severity.
     * @return A string with the severity counts, one row for each severity.
     */
    private String severitiesBreakdown(Map<Severity, Long> groupCountsBySeverity) {
        StringBuilder result = new StringBuilder();
        groupCountsBySeverity.forEach((severity, count) -> {
            result.append("    ").append(severity).append(" : ").append(count).append("\n");
        });
        return result.toString();
    }
}
