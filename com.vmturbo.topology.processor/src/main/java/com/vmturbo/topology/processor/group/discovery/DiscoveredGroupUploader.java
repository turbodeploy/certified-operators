package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc;
import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;

/**
 * The {@link DiscoveredGroupUploader} is the interface for the discovery operation to upload
 * discovered {@link CommonDTO.GroupDTO}s, Policies, and Settings to the Group component.
 * <p>
 * The uploader should is thread safe. Discovered groups, policies, and settings may be
 * set while an upload is in progress. Upload happens during a
 * {@link com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline} stage in the
 * broadcast pipeline, while discoveries happen asynchronously with this pipeline.
 *
 * Uploading discovered groups does NOT clear the latest discovered groups, policies, and settings
 * for targets known to the uploader. Thus, if no new groups, policies, or settings are set for
 * a target since the last time that target's results were uploaded, the previous ones will
 * be re-uploaded the next time that {@link #uploadDiscoveredGroups()} is called.
 */
@ThreadSafe
public class DiscoveredGroupUploader {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Having this keyword in the group_name field of a GroupDTO coming from VCenter means
     * that the group is a folder.
     * <p>
     * We care about this because we DON'T want to support mapping folders for now (2017).
     */
    static String VC_FOLDER_KEYWORD = "Folder";

    private final DiscoveredGroupServiceBlockingStub uploadStub;

    private final DiscoveredGroupInterpreter discoveredGroupInterpreter;

    /**
     * A map from targetId to the list of the most recent {@link DiscoveredGroupInfo} for that
     * target.
     * <p>
     * This is for debugging purposes only - to support easily viewing the latest discovered
     * groups.
     */
    private final Map<Long, List<InterpretedGroup>> latestGroupByTarget = new ConcurrentHashMap<>();
    private final Map<Long, List<DiscoveredPolicyInfo>> latestPoliciesByTarget = new ConcurrentHashMap<>();

    @VisibleForTesting
    DiscoveredGroupUploader(@Nonnull final Channel groupChannel,
            @Nonnull final DiscoveredGroupInterpreter discoveredGroupInterpreter) {
        this.uploadStub =
            DiscoveredGroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.discoveredGroupInterpreter = discoveredGroupInterpreter;

        // Do not start the GroupUploader in this constructor.
    }

    public DiscoveredGroupUploader(@Nonnull final Channel groupChannel,
                                   @Nonnull final EntityStore entityStore) {
        this(groupChannel, new DiscoveredGroupInterpreter(entityStore));

    }

    /**
     * Set the discovered groups for a target. This overwrites any existing discovered
     * group information for the target.
     *
     * @param targetId The id of the target whose groups were discovered.
     * @param groups The discovered groups for the target.
     */
    public void setTargetDiscoveredGroups(final long targetId,
                                          @Nonnull final List<CommonDTO.GroupDTO> groups) {
        final List<InterpretedGroup> interpretedDtos =
                discoveredGroupInterpreter.interpretSdkGroupList(groups, targetId);
        synchronized (latestGroupByTarget) {
            latestGroupByTarget.put(targetId, interpretedDtos);
            final DiscoveredPolicyInfoParser parser = new DiscoveredPolicyInfoParser(groups);
            latestPoliciesByTarget.put(targetId, parser.parsePoliciesOfGroups());
        }
    }

    /**
     * Get the latest {@link DiscoveredGroupInfo} for each target ID.
     *
     * @return A map, with the ID of the target as the key.
     */
    @Nonnull
    public Map<Long, List<DiscoveredGroupInfo>> getDiscoveredGroupInfoByTarget() {
        return latestGroupByTarget.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                entry -> entry.getValue().stream()
                    .map(InterpretedGroup::createDiscoveredGroupInfo)
                    .collect(Collectors.toList())));
    }

    /**
     * Upload discovered groups, policies, and settings to the component responsible for managing
     * these items.
     *
     * Uploading discovered groups does NOT clear the latest groups, policies, and settings known
     * to the group uploader.
     */
    public void uploadDiscoveredGroups() {
        final List<StoreDiscoveredGroupsRequest> requests = new ArrayList<>();

        // Create requests in a synchronized block to guard against changes to discovered groups/settings/policies
        // while an upload is in progress so that the data structures for each type cannot be made to be
        // out of synch with each other.
        synchronized (latestGroupByTarget) {
            latestGroupByTarget.forEach((targetId, groups) -> {
                final StoreDiscoveredGroupsRequest.Builder req =
                    StoreDiscoveredGroupsRequest.newBuilder()
                        .setTargetId(targetId);
                groups.forEach(interpretedDto -> {
                    interpretedDto.getDtoAsCluster().ifPresent(req::addDiscoveredCluster);
                    interpretedDto.getDtoAsGroup().ifPresent(req::addDiscoveredGroup);
                });
                List<DiscoveredPolicyInfo> policiesByTarget = latestPoliciesByTarget.get(targetId);
                if (policiesByTarget != null) {
                    req.addAllDiscoveredPolicyInfos(policiesByTarget);
                }

                requests.add(req.build());
            });
        }

        // Upload the groups/policies/settings.
        // TODO: (DavidBlinn 1/29/18) upload these as a gRPC stream rather than in multiple requests.
        requests.forEach(uploadStub::storeDiscoveredGroups);
    }

    /**
     * Called when a target has been removed. Queue an empty Group list for that target.
     * This should effectively delete all previously discovered Groups and Clusters for the given
     * target.
     *
     * @param targetId ID of the target that was removed.
     */
    public void targetRemoved(long targetId) {
        setTargetDiscoveredGroups(targetId, Collections.emptyList());
    }
}
