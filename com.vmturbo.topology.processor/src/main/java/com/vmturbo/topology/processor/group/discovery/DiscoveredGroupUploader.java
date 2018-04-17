package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc;
import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.entity.EntityStore;

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
 *
 * TODO: (DavidBlinn 1/31/2018) There is a problem with how we presently handle
 * TODO: discovered groups/policies/settings/templates/deployment profiles etc.
 * TODO: These data are tied with a specific discovery and topology but because they are stored
 * TODO: independently from each other, a discovery that completes in the middle of broadcast may
 * TODO: result in publishing these data from a different discovery than some other part of the
 * TODO: topology (ie the entities in the broadcast for a target may be from discovery A but the
 * TODO: discovered groups in the same broadcast may be from discovery B). These data should all be
 * TODO: stored together and copied together at the the first stage in the broadcast pipeline so
 * TODO: that we can guarantee the topology we publish is internally consistent.
 */
@ThreadSafe
public class DiscoveredGroupUploader {

    /**
     * Having this keyword in the group_name field of a GroupDTO coming from VCenter means
     * that the group is a folder.
     * <p>
     * We care about this because we DON'T want to support mapping folders for now (2017).
     */
    static String VC_FOLDER_KEYWORD = "Folder";

    private final DiscoveredGroupServiceBlockingStub uploadStub;

    private final DiscoveredGroupInterpreter discoveredGroupInterpreter;

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

    /**
     * A map from targetId to the list of the most recent {@link DiscoveredGroupInfo} for that
     * target.
     * <p>
     * This is for debugging purposes only - to support easily viewing the latest discovered
     * groups.
     */
    private final Map<Long, List<InterpretedGroup>> latestGroupByTarget = new HashMap<>();
    private final Map<Long, List<DiscoveredPolicyInfo>> latestPoliciesByTarget = new HashMap<>();
    private final Multimap<Long, DiscoveredSettingPolicyInfo> latestSettingPoliciesByTarget =
        HashMultimap.create();

    private final Map<Long, List<InterpretedGroup>> groupsToUploadByTarget = new HashMap<>();

    @VisibleForTesting
    DiscoveredGroupUploader(@Nonnull final Channel groupChannel,
                            @Nonnull final DiscoveredGroupInterpreter discoveredGroupInterpreter,
                            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache) {
        this.uploadStub =
            DiscoveredGroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.discoveredGroupInterpreter = Objects.requireNonNull(discoveredGroupInterpreter);
        this.discoveredClusterConstraintCache = discoveredClusterConstraintCache;
    }

    public DiscoveredGroupUploader(
            @Nonnull final Channel groupChannel,
            @Nonnull final EntityStore entityStore,
            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache) {
        this(groupChannel, new DiscoveredGroupInterpreter(entityStore), discoveredClusterConstraintCache);
    }

    /**
     * Set the discovered groups for a target. This overwrites any existing discovered
     * group information for the target.
     *
     * This also clears any previously discovered setting policies for this target.
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
            final List<DiscoveredPolicyInfo> discoveredPolicyInfos = parser.parsePoliciesOfGroups();
            latestPoliciesByTarget.put(targetId, discoveredPolicyInfos);
            discoveredClusterConstraintCache.storeDiscoveredClusterConstraint(targetId, groups);
            latestSettingPoliciesByTarget.get(targetId).clear();
        }
    }

    /**
     * Set the discovered setting policies for a target. This overwrites any existing discovered
     * setting policies for that target.
     *
     * @param targetId the id of the target whose settings policies were discovered
     * @param settings the discovered setting policies for the target
     */
    public void setTargetDiscoveredSettingPolicies(final long targetId,
                                    @Nonnull final List<DiscoveredSettingPolicyInfo> settings) {
        synchronized (latestGroupByTarget) {
            latestSettingPoliciesByTarget.get(targetId).clear();
            latestSettingPoliciesByTarget.putAll(targetId, settings);
        }

    }

    /**
     * Insert discovered groups and setting policies for a target in an additive manner. Does not
     * overwrite previous discovered groups and setting policies, instead it appends the provided
     * {@link InterpretedGroup}s and {@link DiscoveredSettingPolicyInfo}s to the existing ones.
     *
     * @param targetId The id of the target that discovered these groups and setting policies.
     * @param interpretedGroups The discovered groups to be added to the existing collection for this target.
     * @param settingPolicies The discovered setting policies to be added to the existing collection
     *                        for this target.
     */
    public void addDiscoveredGroupsAndPolicies(final long targetId,
                                               @Nonnull final List<InterpretedGroup> interpretedGroups,
                                               @Nonnull final List<DiscoveredSettingPolicyInfo> settingPolicies) {
        synchronized (latestGroupByTarget) {
            final List<InterpretedGroup> targetGroups =
                latestGroupByTarget.computeIfAbsent(targetId, id -> new ArrayList<>());
            targetGroups.addAll(interpretedGroups);

            latestSettingPoliciesByTarget.putAll(targetId, settingPolicies);
        }
    }

    /**
     * Get the latest {@link DiscoveredGroupInfo} for each target ID.
     *
     * @return A map, with the ID of the target as the key.
     */
    @Nonnull
    public Map<Long, List<DiscoveredGroupInfo>> getDiscoveredGroupInfoByTarget() {
        synchronized (latestGroupByTarget) {
            return latestGroupByTarget.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey,
                    entry -> entry.getValue().stream()
                        .map(InterpretedGroup::createDiscoveredGroupInfo)
                        .collect(Collectors.toList())));
        }
    }

    /**
     * Get a {@link DiscoveredGroupMemberCache} associated with the {@link InterpretedGroup}s contained
     * in the {@link DiscoveredGroupUploader} at this time.
     *
     * @return a {@link DiscoveredGroupMemberCache} for this {@link DiscoveredGroupUploader}.
     */
    @Nonnull
    public DiscoveredGroupMemberCache buildMemberCache() {
        synchronized (latestGroupByTarget) {
            return new DiscoveredGroupMemberCache(latestGroupByTarget);
        }
    }

    public Map<Long, List<InterpretedGroup>> createDeepCopiesOfGroups() {
        synchronized (latestGroupByTarget) {
            groupsToUploadByTarget.clear();
            latestGroupByTarget.forEach((targetId, targetGroups) -> groupsToUploadByTarget
                            .put(targetId, targetGroups.stream().map(InterpretedGroup::deepCopy)
                                            .collect(Collectors.toList())));
        }
        return groupsToUploadByTarget;
    }

    /**
     * Get a copy of the setting policies for a target.
     *
     * @return a copy of the setting policies for a target. If the target is unknown,
     *         returns {@link Optional#empty()}.
     */
    @Nonnull
    public Optional<List<DiscoveredSettingPolicyInfo>> getDiscoveredSettingPolicyInfoForTarget(
        final long targetId) {
        synchronized (latestGroupByTarget) {
            final Collection<DiscoveredSettingPolicyInfo> targetDiscoveredSettingPolicies =
                latestSettingPoliciesByTarget.get(targetId);
            return Optional.ofNullable(targetDiscoveredSettingPolicies).map(ImmutableList::copyOf);
        }
    }

    /**
     * Get a copy of the setting policies for all targets.
     *
     * @return a copy of the setting policies for all targets.
     */
    @Nonnull
    public Multimap<Long, DiscoveredSettingPolicyInfo> getDiscoveredSettingPolicyInfoByTarget() {
        synchronized (latestGroupByTarget) {
            return ImmutableMultimap.copyOf(latestSettingPoliciesByTarget);
        }
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
            if (groupsToUploadByTarget.isEmpty()) {
                createDeepCopiesOfGroups();
            }
            groupsToUploadByTarget.forEach((targetId, groups) -> {
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
                Collection<DiscoveredSettingPolicyInfo> settingPolicies = latestSettingPoliciesByTarget.get(targetId);
                if (settingPolicies != null) {
                    req.addAllDiscoveredSettingPolicies(settingPolicies);
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
