package com.vmturbo.topology.processor.group.discovery;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.DiscoveredCollectionsServiceGrpc;
import com.vmturbo.common.protobuf.group.DiscoveredCollectionsServiceGrpc.DiscoveredCollectionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsRequest;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;

/**
 * The {@link DiscoveredGroupUploader} is the interface for the discovery operation to upload
 * discovered {@link CommonDTO.GroupDTO}s to the Group component.
 * <p>
 * The uploader acts as a queue:
 * - When processing discovery results, the processing thread queues up discovered groups via calls
 *   to {@link DiscoveredGroupUploader#queueDiscoveredGroups(long, List)}.
 * - Asynchronously, an internal thread polls the "queue" and uploads any discovered groups since
 *   the last poll to the group component.
 */
public class DiscoveredGroupUploader {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Having this keyword in the group_name field of a GroupDTO coming from VCenter means
     * that the group is a folder.
     * <p>
     * We care about this because we DON'T want to support mapping folders for now (2017).
     */
    static String VC_FOLDER_KEYWORD = "Folder";

    private final DiscoveredCollectionsServiceBlockingStub uploadStub;

    private final DiscoveredGroupInterpreter discoveredGroupInterpreter;

    /**
     * A map from targetId to the list of the most recent {@link DiscoveredGroupInfo} for that
     * target.
     * <p>
     * This is for debugging purposes only - to support easily viewing the latest discovered
     * groups.
     */
    @GuardedBy("latestGroupByTargetLock")
    private final Map<Long, List<InterpretedGroup>> latestGroupByTarget = new HashMap<>();

    @GuardedBy("latestGroupByTargetLock")
    private final Map<Long, List<DiscoveredPolicyInfo>> latestPoliciesByTarget = new HashMap<>();

    private final Object latestGroupByTargetLock = new Object();

    /**
     * A map from targetId to the most recent list of {@link CommonDTO.GroupDTO}s discovered
     * by that target and not yet uploaded to the Group component.
     * <p>
     * Once the DTO's are converted and uploaded to the Group component there should be no
     * entry for that targetId until the next discovery cycle of the target.
     */
    @GuardedBy("queueLock")
    private Map<Long, List<InterpretedGroup>> pendingGroupsByTarget = new HashMap<>();

    private final Object queueLock = new Object();

    @VisibleForTesting
    DiscoveredGroupUploader(@Nonnull final Channel groupChannel,
            @Nonnull final DiscoveredGroupInterpreter discoveredGroupInterpreter) {
        this.uploadStub =
            DiscoveredCollectionsServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.discoveredGroupInterpreter = discoveredGroupInterpreter;

        // Do not start the GroupUploader in this constructor.
    }

    public DiscoveredGroupUploader(@Nonnull final Channel groupChannel,
                                   @Nonnull final EntityStore entityStore) {
        this(groupChannel, new DiscoveredGroupInterpreter(entityStore));

    }

    public void queueDiscoveredGroups(final long targetId,
                                      @Nonnull final List<CommonDTO.GroupDTO> groups) {
        final List<InterpretedGroup> interpretedDtos =
                discoveredGroupInterpreter.interpretSdkGroupList(groups, targetId);
        synchronized (latestGroupByTarget) {
            latestGroupByTarget.put(targetId, interpretedDtos);
            final DiscoveredPolicyInfoParser parser = new DiscoveredPolicyInfoParser(groups);
            latestPoliciesByTarget.put(targetId, parser.parsePoliciesOfGroups());
        }

        synchronized (queueLock) {
            // Only queue successfully interpreted DTOs for uploading to the Group component.
            //
            // The list of groups for a particular target completely overwrites whatever the
            // previous list of groups for that target was.
            pendingGroupsByTarget.put(targetId, interpretedDtos);
        }
    }

    /**
     * Get the latest {@link DiscoveredGroupInfo} for each target ID.
     *
     * @return A map, with the ID of the target as the key.
     */
    @Nonnull
    public Map<Long, List<DiscoveredGroupInfo>> getDiscoveredGroupInfoByTarget() {
        synchronized (latestGroupByTargetLock) {
            return latestGroupByTarget.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey,
                        entry -> entry.getValue().stream()
                            .map(InterpretedGroup::createDiscoveredGroupInfo)
                            .collect(Collectors.toList())));
        }
    }

    @Nonnull
    @VisibleForTesting
    Map<Long, List<InterpretedGroup>> pollQueuedGroups() {
        synchronized (queueLock) {
            final Map<Long, List<InterpretedGroup>> ret = pendingGroupsByTarget;
            pendingGroupsByTarget = new HashMap<>();
            return ret;
        }
    }

    /**
     * Re-queue groups returned by a call to {@link this#pollQueuedGroups()} for later processing.
     * This is done if there was a recoverable attempting to process the groups - e.g. if the group
     * component is down.
     *
     * @param oldGroups The groups to re-queue.
     */
    @VisibleForTesting
    void requeueGroups(@Nonnull final Map<Long, List<InterpretedGroup>> oldGroups) {
        synchronized (queueLock) {
            // If a fresher set of groups was discovered between the call to
            // pollQueuedGroups and now, we do NOT want to overwrite it!
            oldGroups.forEach(pendingGroupsByTarget::putIfAbsent);
        }
    }

    public void processQueuedGroups() {
        final Map<Long, List<InterpretedGroup>> groupsByTargetId = pollQueuedGroups();
        try {
            groupsByTargetId.forEach((targetId, groups) -> {
                final StoreDiscoveredCollectionsRequest.Builder req =
                        StoreDiscoveredCollectionsRequest.newBuilder()
                            .setTargetId(targetId);
                groups.forEach(interpretedDto -> {
                    interpretedDto.getDtoAsCluster().ifPresent(req::addDiscoveredCluster);
                    interpretedDto.getDtoAsGroup().ifPresent(req::addDiscoveredGroup);
                });
                List<DiscoveredPolicyInfo> policiesByTarget = latestPoliciesByTarget.get(targetId);
                if (policiesByTarget != null) {
                    req.addAllDiscoveredPolicyInfos(policiesByTarget);
                }
                uploadStub.storeDiscoveredCollections(req.build());
            });
        } catch (RuntimeException e) {
            requeueGroups(groupsByTargetId);
            throw e;
        }
    }

    /**
     * Called when a target has been removed. Queue an empty Group list for that target.
     * This should effectively delete all previously discovered Groups and Clusters for the given
     * target.
     *
     * @param targetId ID of the target that was removed.
     */
    public void targetRemoved(long targetId) {
        queueDiscoveredGroups(targetId, Collections.emptyList());
    }
}
