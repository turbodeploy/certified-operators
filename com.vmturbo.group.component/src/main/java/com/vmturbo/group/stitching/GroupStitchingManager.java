package com.vmturbo.group.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The {@link GroupStitchingManager} coordinates group stitching operations.
 *
 * <p>Stitching is the process of taking the groups discovered from different targets and applying
 * certain operations to produce a unified group topology.
 */
public class GroupStitchingManager {

    private static final Logger logger = LogManager.getLogger();

    /**
     * List of stitching operations to be performed in order.
     */
    private static final List<GroupStitchingOperation> STITCHING_OPERATIONS = ImmutableList.of(
            new ResourceGroupStitchingOperation()
    );

    /**
     * A metric that tracks duration of execution for group stitching.
     */
    private static final DataMetricSummary GROUP_STITCHING_EXECUTION_DURATION_SUMMARY =
            DataMetricSummary.builder()
                    .withName("group_stitching_execution_duration_seconds")
                    .withHelp("Duration of execution of all group stitching operations.")
                    .build()
                    .register();

    /**
     * Stitch the groups discovered by different targets together to produce a unified group.
     *
     * @param stitchingContext The input context containing the groups to be stitched.
     * @return A {@link GroupStitchingContext} that contains the results of applying the stitching
     *         operations to the groups.
     */
    public GroupStitchingContext stitch(@Nonnull final GroupStitchingContext stitchingContext) {
        final DataMetricTimer executionTimer = GROUP_STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();

        logger.debug("Applying {} group stitching operations.", STITCHING_OPERATIONS.size());
        GroupStitchingContext curStitchingContext = stitchingContext;
        for (GroupStitchingOperation groupStitchingOperation : STITCHING_OPERATIONS) {
            final Collection<StitchingGroup> groups =
                    groupStitchingOperation.getScope(stitchingContext);
            curStitchingContext = groupStitchingOperation.stitch(groups, curStitchingContext);
        }
        // check for duplicates before saving to DB
        removeDuplicateGroups(curStitchingContext);

        executionTimer.observe();
        return curStitchingContext;
    }

    /**
     * If there is a group with same source id and group type, it's considered an error and
     * should be dropped.
     *
     * @param stitchingContext the context containing all groups
     */
    void removeDuplicateGroups(@Nonnull final GroupStitchingContext stitchingContext) {
        final Map<String, StitchingGroup> preservedGroupById = new HashMap<>();
        final Map<String, List<StitchingGroup>> duplicateGroupsById = new HashMap<>();

        stitchingContext.getAllStitchingGroups().forEach(group -> {
            final String groupIdentifyingKey = GroupProtoUtil.createIdentifyingKey(
                    group.getGroupDefinition().getType(), group.getSourceId());
            StitchingGroup existing = preservedGroupById.get(groupIdentifyingKey);
            if (existing == null) {
                preservedGroupById.put(groupIdentifyingKey, group);
            } else {
                // found another group with same identifying key (id & type)!
                duplicateGroupsById.computeIfAbsent(groupIdentifyingKey, k -> new ArrayList<>())
                        .add(group);
            }
        });

        duplicateGroupsById.forEach((groupIdentifyingKey, duplicateGroups) -> {
            final List<Long> targetsToDiscard = duplicateGroups.stream()
                    .map(StitchingGroup::getSourceTargetId)
                    .collect(Collectors.toList());
            logger.error("Found {} groups with same identification ({}), only keeping the one " +
                            "from target {}, and discarding others from targets {}",
                    duplicateGroups.size() + 1, groupIdentifyingKey,
                    preservedGroupById.get(groupIdentifyingKey).getSourceTargetId(),
                    targetsToDiscard);
            // remove those duplicate groups for other targets
            stitchingContext.removeGroups(duplicateGroups);
        });
    }
}
