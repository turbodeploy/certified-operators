package com.vmturbo.action.orchestrator.execution;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Class to manage creation of tier exclusion policies for VMs that failed to resize.
 */
public class FailedCloudResizeTierExcluder {
    /**
     * Logger.
     */
    public static final Logger logger = LogManager.getLogger(FailedCloudResizeTierExcluder.class);

    private final Object groupLock = new Object();
    private final Object policyLock = new Object();
    private final GroupServiceBlockingStub groupServiceClient;
    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final RepositoryServiceBlockingStub repositoryService;
    private final List<Pattern> failedActionPatterns;

    /**
     * Create a FailedCloudResizeTierExcluder.
     *
     * @param groupServiceClient group service gRPC stub
     * @param settingPolicyService settings policy service gRPC stub
     * @param repositoryService repository service gRPC stub
     * @param failedActionPatterns list of action error message strings that will trigger the
     */
    public FailedCloudResizeTierExcluder(final GroupServiceBlockingStub groupServiceClient,
            final SettingPolicyServiceBlockingStub settingPolicyService,
            final RepositoryServiceBlockingStub repositoryService,
            final String failedActionPatterns) {
        this.groupServiceClient = groupServiceClient;
        this.settingPolicyService = settingPolicyService;
        this.repositoryService = repositoryService;
        // Pre-compile the patterns for efficiency.
        this.failedActionPatterns = Arrays.stream(failedActionPatterns.split("\n"))
                .map(pattern -> {
                    try {
                        return Pattern.compile(pattern);
                    } catch (PatternSyntaxException e) {
                        logger.error(
                                "Regular expression syntax error in failedActionPatterns list: '{}'",
                                pattern);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private String regionLabel(@Nonnull RegionInfo regionInfo) {
        return (EntityType.AVAILABILITY_ZONE == regionInfo.entityType
                ? "Availability Zone " : "Region ") + regionInfo.name;
    }

    /**
     * Create a name for the associated tier exclusion policy.
     *
     * @param regionInfo name and type of region
     * @param accountId account ID
     * @return name of family exclusion policy
     */
    private String makePolicyName(@Nonnull RegionInfo regionInfo,
            @Nonnull Long accountId) {
        return String.format(
                "%s - Failed execution Tier Family Exclusion Policy (account %d)",
                regionLabel(regionInfo), accountId);
    }

    /**
     * Create a group name for the associated tier exclusion policy.  This is only used for
     * single VM groups.
     *
     * @param accountId account ID
     * @param originInfo name and vendor's ID of the VM in the group
     * @return name of family exclusion group
     */
    private String makeGroupName(@Nonnull Long accountId, @Nonnull VMOriginInfo originInfo) {
        return String.format("%s%s - Failed execution Tier Family Exclusion Group (account %d)",
                originInfo.getDisplayName(),
                originInfo.getVmVendorId() == null ? "" : "/" + originInfo.getVmVendorId(),
                accountId);
    }

    /**
     * Create or update a tier exclusion policy for the failed action.  The policy will exclude all
     * compute tiers in the family of the attempted resize.
     *
     * @param actionView the action details.
     * @param actionFailure The progress notification for an action, which contains the reason for
     *                      the failure.
     */
    public void updateTierExclusionPolicy(@Nonnull ActionView actionView,
                                          @Nonnull ActionFailure actionFailure) {
        // Only add a tier exclusion if the execution failure matches a pattern in the configuration.
        Action action = actionView.getRecommendation();
        if (!shouldHandleFailure(actionFailure)) {
            logger.debug("Not creating tier exclusion policy for action {}: Action is successful",
                    action.getId());
            return;
        }

        ActionInfo actionInfo = action.getInfo();
        if (!actionInfo.hasMove()) {
            logger.warn("Not creating tier exclusion policy for action {}: Missing move info",
                    action.getId());
            return;
        }

        Move move = actionInfo.getMove();
        logger.info("Creating exclusion policy for failed action ID {}",
                action.hasId() ? action.getId() : "[UNKNOWN]");
        // Get the detail for VM
        Long targetOid = move.getTarget().getId();
        TopologyEntityDTO vm = getVM(targetOid);
        if (vm == null) {
            logger.warn("Not creating tier exclusion policy for action {}: Cannot find VM ID {}",
                    action.getId(), targetOid);
            return;
        }
        // Locate the family name and Tier OIDs for the policy
        Optional<PeerSkuInfo> peerInfo = findPeerSKUs(vm, move.getChangesList());
        if (!peerInfo.isPresent()) {
            logger.warn("Not creating tier exclusion policy for action {}: Cannot find peer SKUs",
                    action.getId());
            return;
        }
        VMOriginInfo originInfo = new VMOriginInfo(vm);
        Long accountId = originInfo.getAccountId();
        if (accountId == null) {
            logger.warn("Not creating tier exclusion policy for action {}: Cannot find account ID",
                    action.getId());
            return;
        }
        // Create the policy and potential group names
        RegionInfo regionInfo = getRegionInfo(vm);
        if (regionInfo == null) {
            logger.warn("Not creating tier exclusion policy for action {}: Cannot find region",
                    action.getId());
            return;
        }
        // Locate the group to apply the exclusion policy to. If the group doesn't exist, it will
        // be created.
        String groupName = makeGroupName(accountId, originInfo);
        logger.info("Locating or creating group {} for exclusion policy", groupName);
        GroupResult result = getTierExclusionGroup(targetOid, groupName);
        if (result == null) {
            logger.warn("Not creating tier exclusion policy for action {}: "
                    + "Cannot find or create group {}", action.getId(), groupName);
            return;
        }
        // Locate an existing policy.  If it exists, then add peerInfo to that policy, else create
        // a new policy.
        String policyName = makePolicyName(regionInfo, accountId);
        logger.info("Locating or creating policy {} for failed resize action", policyName);
        Long policyOid = createOrUpdateExclusionPolicy(vm, peerInfo.get(), result.oid, policyName);
        if (policyOid == null) {
            logger.error("Failed to create tier exclusion policy '{}'", policyName);
            // Failed to create the exclusion policy, so delete the group if it was newly-created.
            if (result.isNewGroup) {
                logger.debug("Deleting newly-created group '{}' due to failure to create "
                                + " associated tier exclusion policy", groupName);
                deleteGroup(result.oid);
            }
        }
    }

    /**
     * Create a template exclusion policy.  If a tier family exclusion policy exists for the group,
     * add the new set of excluded tiers to the existing policy.
     *
     * @param vm VM that the policy will affect.  This is used for debug logging only.
     * @param peerInfo SKU peer information containing list of SKUs to exclude
     * @param groupOid OID of group to attach policy to.
     * @param policyName name of policy to create/reuse.
     * @return OID of the existing or newly-created policy. Returns null if a policy could not be
     *         found or created.
     */
    @Nullable
    private Long createOrUpdateExclusionPolicy(TopologyEntityDTO vm, PeerSkuInfo peerInfo,
            Long groupOid, String policyName) {
        Long oid;
        GetSettingPolicyRequest getReq = GetSettingPolicyRequest.newBuilder()
                .setName(policyName).build();
        synchronized (policyLock) {
            try {
                GetSettingPolicyResponse getRsp = settingPolicyService.getSettingPolicy(getReq);

                if (getRsp.hasSettingPolicy()) {
                    logger.debug("Using existing tier exclusion policy: {}", policyName);
                    SettingPolicy settingPolicy = getRsp.getSettingPolicy();
                    // Add the scope and new SKUs to exclude
                    SettingPolicyInfo existingInfo = settingPolicy.getInfo();
                    Set<Long> updatedScope = new HashSet<>(existingInfo.getScope().getGroupsList());
                    Set<Long> updatedSKUs = new HashSet<>(existingInfo.getSettings(0)
                            .getSortedSetOfOidSettingValue()
                            .getOidsList());
                    updatedSKUs.addAll(peerInfo.peerSKUs);
                    List<Long> updatedSKUList = Lists.newArrayList(updatedSKUs);
                    Collections.sort(updatedSKUList);
                    updatedScope.add(groupOid);
                    SettingPolicyInfo.Builder info =
                            createSettingPolicyInfo(existingInfo.getDisplayName(),
                                    Lists.newArrayList(updatedScope), updatedSKUList);
                    UpdateSettingPolicyRequest req = UpdateSettingPolicyRequest.newBuilder()
                            .setId(settingPolicy.getId()).setNewInfo(info).build();
                    UpdateSettingPolicyResponse rsp = settingPolicyService.updateSettingPolicy(req);
                    oid = rsp.hasSettingPolicy() ? rsp.getSettingPolicy().getId() : null;
                    logger.debug("Updated policy response = {}", rsp);
                } else {
                    // New policy, so create a new policy with the SKUs to exclude. Note that unlike
                    // other automatically-generated policies, this is a read/write policy, to allow
                    // the user to delete them in case a family becomes available for the given region.
                    logger.debug("Creating new family exclusion policy for VM {} with name '{}'",
                            vm.getDisplayName(), policyName);
                    SettingPolicyInfo.Builder info = createSettingPolicyInfo(policyName,
                            ImmutableList.of(groupOid), peerInfo.peerSKUs);
                    CreateSettingPolicyRequest createReq =
                            CreateSettingPolicyRequest.newBuilder().setSettingPolicyInfo(info).build();
                    CreateSettingPolicyResponse rsp =
                            settingPolicyService.createSettingPolicy(createReq);
                    oid = rsp.hasSettingPolicy() ? rsp.getSettingPolicy().getId() : null;
                    logger.debug("Create policy response for VM {} = {}", vm.getDisplayName(), rsp);
                }
            } catch (StatusRuntimeException e) {
                logger.error("Could not create or update tier exclusion policy '{}': {}", policyName,
                        e.getMessage());
                return null;
            }
        }
        return oid;
    }

    @Nullable
    private GroupResult getTierExclusionGroup(Long initialMemberOid, String groupName) {
        long groupOid;
        boolean newGroup = false;
        try {
            synchronized (groupLock) {
                Optional<Grouping> existingGroup = findGroup(groupName);
                if (existingGroup.isPresent()) {
                    groupOid = existingGroup.get().getId();
                } else {
                    Grouping group = createGroup(groupName, groupName, initialMemberOid);
                    if (!group.hasId()) {
                        logger.error("Could not create tier family exclusion group '{}'.", groupName);
                        return null;
                    }
                    newGroup = true;
                    groupOid = group.getId();
                }
            }
            if (newGroup) {
                logger.info("Created group {} for family exclusion policy", groupName);
            } else {
                logger.debug("Using existing group {} for family exclusion policy", groupName);
            }
        } catch (StatusRuntimeException e) {
            logger.error("Could not create tier family exclusion group '{}': {}", groupName,
                    e.getMessage());
            return null;
        }
        return new GroupResult(groupOid, newGroup);
    }

    /**
     * Return the VM group with the indicated name.
     *
     * @param groupName name of group to find
     * @return optional of existing group
     */
    private Optional<Grouping> findGroup(String groupName) {
        Iterator<Grouping> groupResponse = groupServiceClient.getGroups(
                GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.REGULAR)
                                .addPropertyFilters(PropertyFilter.newBuilder()
                                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex(Pattern.quote(groupName)))))
                        .build());

        while (groupResponse.hasNext()) {
            Grouping group = groupResponse.next();
            if (GroupProtoUtil.getEntityTypes(group).equals(
                    Collections.singleton(ApiEntityType.VIRTUAL_MACHINE))) {
                return Optional.of(group);
            }
        }
        return Optional.empty();
    }

    /**
     * Create a VM group with an initial member.
     *
     * @param groupName name of group to create
     * @param description description of group
     * @param initialMemberOid initial VM oid to add to group
     * @return the created group
     */
    private Grouping createGroup(String groupName, String description, Long initialMemberOid) {
        final CreateGroupRequest request = CreateGroupRequest.newBuilder()
                .setGroupDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(groupName)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder().setEntity(
                                                EntityType.VIRTUAL_MACHINE.getValue()))
                                        .addMembers(initialMemberOid))))
                .setOrigin(Origin.newBuilder().setSystem(Origin.System.newBuilder()
                                .setDescription(description)))
                .build();
        return groupServiceClient.createGroup(request).getGroup();
    }

    /**
     * Delete a group.
     *
     * @param oid OID of group to delete.
     */
    private void deleteGroup(Long oid) {
        groupServiceClient.deleteGroup(GroupID.newBuilder().setId(oid).build());
    }

    @Nullable
    private TopologyEntityDTO getVM(Long targetOid) {
        final List<TopologyEntityDTO> allResults = getEntities(EntityType.VIRTUAL_MACHINE, targetOid);
        if (allResults.isEmpty()) {
            logger.warn("Cannot find VM with OID {} - cannot create exclusion policy", targetOid);
            return null;
        }
        if (allResults.size() > 1) {
            logger.warn("Multiple entities with OID {} - cannot create exclusion policy", targetOid);
            return null;
        }
        return allResults.get(0);
    }

    @VisibleForTesting
    static SettingPolicyInfo.Builder createSettingPolicyInfo(String policyName, List<Long> scope,
            List<Long> skus) {
        Setting.Builder setting = Setting.newBuilder().setSettingSpecName(
                "excludedTemplatesOids").setSortedSetOfOidSettingValue(
                SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(skus));
        return SettingPolicyInfo.newBuilder()
                .setName(policyName)
                .setDisplayName(policyName)
                .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                .setScope(Scope.newBuilder().addAllGroups(scope))
                .addSettings(setting);
    }

    /**
     * Return whether the action failure requires adding a template exclusion based on the
     * failed action's error description.
     *
     * @param actionFailure failed action to check
     * @return true if the action failure should trigger generation of a tier exclusion policy to
     * prevent further resize recommendations to the tier family.
     */
    private boolean shouldHandleFailure(ActionFailure actionFailure) {
        if (actionFailure == null) {
            return false;
        }
        String failureMessage = actionFailure.getErrorDescription();
        return failedActionPatterns.stream()
                .anyMatch(pattern -> pattern.matcher(failureMessage).find());
    }

    /**
     * Helper function to query the repository for entities.
     *
     * @param entityType type of entity to search for.
     * @param entityOid if present, the specific OID to search for, else return all entities of
     *              the specified type.
     * @return the list of requested entities, or an empty list if none found.
     */
    private @Nonnull
    List<TopologyEntityDTO> getEntities(EntityType entityType, Long entityOid) {
        Builder builder = RetrieveTopologyEntitiesRequest.newBuilder()
                .setReturnType(Type.FULL)
                .addEntityType(entityType.getValue());
        if (entityOid != null) {
            builder.addEntityOids(entityOid);
        }
        return RepositoryDTOUtil.topologyEntityStream(
                repositoryService.retrieveTopologyEntities(builder.build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
    }

    /**
     * Get the region information related to this VM.  For Azure, only the region is available, and
     * for others, the availability zone or both are available.  In the event that both are present,
     * we will favor the more specific availability zone information.
     *
     * @param vm The VM {@link TopologyEntityDTO} to check
     * @return If the region or availability zone can be determined, a @{link RegionInfo} containing
     *         AZ/region and type, else null.
     */
    @Nullable
    private RegionInfo getRegionInfo(TopologyEntityDTO vm) {
        // Get the current region so that we can build the group name
        Long oid = null;
        EntityType entityType = EntityType.REGION;
        // If both AZ and region are present, prefer the AZ
        for (ConnectedEntity ce : vm.getConnectedEntityListList()) {
            if (ce.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE.getValue()) {
                oid = ce.getConnectedEntityId();
                entityType = EntityType.AVAILABILITY_ZONE;
                break;
            } else if (ce.getConnectedEntityType() == EntityType.REGION.getValue()) {
                oid = ce.getConnectedEntityId();
            }
        }
        if (oid == null) {
            logger.error("Cannot determine region for VM {} - skipping creating family "
                    + "exclusion policy", vm.getDisplayName());
            return null;
        }

        List<TopologyEntityDTO> regionEntity = getEntities(entityType, oid);
        if (regionEntity.isEmpty()) {
            logger.error("Cannot determine region name for VM {} - skipping creating family "
                    + "exclusion policy", vm.getDisplayName());
            return null;
        }
        return new RegionInfo(regionEntity.get(0).getDisplayName(), entityType);
    }

    /**
     * Locate the OIDs of all SKUs in the family in which this OID resides.  For example, if this
     * OID is currently on Standard_D1, then the OIDs for Standard_D1, D2, D3, and D4 will be
     * returned.
     * @param vm Target VM in action
     * @param changes list of changes in the action
     * @return list of all SKUs in this VM's tier. Return an empty list if a result isn't available.
     */
    private Optional<PeerSkuInfo> findPeerSKUs(@Nonnull TopologyEntityDTO vm,
            @Nonnull List<ChangeProvider> changes) {
        // Get details of the failed SKU. Find the change with COMPUTE_TIER as the destination.
        Optional<Long> failedSkuOid = changes.stream()
                .map(ChangeProvider::getDestination)
                .filter(dest -> dest.getType() == EntityType.COMPUTE_TIER.getValue())
                .map(ActionEntity::getId)
                .findFirst();
        if (!failedSkuOid.isPresent()) {
            logger.debug("Cannot create template exclusion: missing destination compute tier in action");
            return Optional.empty();
        }
        // Now get the peer SKUs for this compute tier
        final List<TopologyEntityDTO> tierResults = getEntities(EntityType.COMPUTE_TIER, null);
        // If this is an Azure VM, the SKU family is provided in its entity property map. For other
        // VMs, we need to take a brute-force approach.
        String skuFamily = vm.getEntityPropertyMapMap().get("azureQuotaFamily");

        // Map from SKU family to list of SKU OID/names.  The names are only used for debug logging.
        ListMultimap<String, Pair<Long, String>> familyToTier = ArrayListMultimap.create();
        for (TopologyEntityDTO tier : tierResults) {
            TypeSpecificInfo info = tier.getTypeSpecificInfo();
            if (info.hasComputeTier()) {
                ComputeTierInfo computeTier = info.getComputeTier();
                String key = computeTier.hasQuotaFamily() && !computeTier.getQuotaFamily().isEmpty()
                        ? computeTier.getQuotaFamily()
                        : computeTier.getFamily();
                familyToTier.put(key, new Pair<>(tier.getOid(), tier.getDisplayName()));
                if (skuFamily == null && tier.getOid() == failedSkuOid.get()) {
                    // Make a note of the family that the failed SKU is in.
                    skuFamily = key;
                }
            }
        }
        if (skuFamily == null) {
            logger.warn("Cannot find tier family for tier ID {}", failedSkuOid.get());
            return Optional.empty();
        }
        // Now that we have the family, we can now return the OIDs of all SKUs
        // in it.
        List<Pair<Long, String>> peerTiers = familyToTier.get(skuFamily);
        logger.debug("Peer tiers in {} = {}", failedSkuOid.get(), peerTiers.stream()
                .map(p -> p.second).collect(Collectors.toList()));
        return Optional.of(new PeerSkuInfo(skuFamily, peerTiers.stream()
                .map(p -> p.first)
                .sorted()  // OIDs need to be sorted for policy creation
                .collect(Collectors.toList())));
    }

    /** Multiple return value alias for for SKU family information.
     */
    static class PeerSkuInfo {
        public final String familyName;
        public final List<Long> peerSKUs;

        PeerSkuInfo(String familyName, List<Long> peerSKUs) {
            this.familyName = familyName;
            this.peerSKUs = peerSKUs;
        }
    }

    /**
     * Multiple return value alias for creating groups.
     */
    static class GroupResult {
        public final Long oid;
        public final boolean isNewGroup;

        GroupResult(Long oid, boolean newGroup) {
            this.oid = oid;
            this.isNewGroup = newGroup;
        }
    }

    /**
     * Multiple return value alias for region search results.
     */
    private static class RegionInfo {
        public final String name;
        public final EntityType entityType;

        RegionInfo(String name, EntityType entityType) {
            this.name = name;
            this.entityType = entityType;
        }
    }

    /**
     * Helper class to parse and return VM origin information.
     */
    private static class VMOriginInfo {
        private Long accountId = null;
        private String vmVendorId = null;
        private final String displayName;

        VMOriginInfo(@Nonnull TopologyEntityDTO vm) {
            displayName = vm.getDisplayName();
            // Ensure that the VM has account information associated with this VM
            if (vm.hasOrigin() && vm.getOrigin().hasDiscoveryOrigin()) {
                Map<Long, PerTargetEntityInformation> discoveryDataMap =
                        vm.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap();
                if (!discoveryDataMap.isEmpty()) {
                    // Get a target identifier, which is the account ID for cloud VMs.
                    Map.Entry<Long, PerTargetEntityInformation> entry =
                            discoveryDataMap.entrySet().iterator().next();
                    accountId = entry.getKey();
                    vmVendorId = entry.getValue().getVendorId();
                }
            }
            if (accountId == null) {
                logger.error("Cannot find account ID in VM {}", vm.getDisplayName());
            }
            if (vmVendorId == null) {
                logger.error("Cannot find vendor ID in VM {}", vm.getDisplayName());
            }
        }

        public Long getAccountId() {
            return accountId;
        }

        public String getVmVendorId() {
            return vmVendorId;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    @VisibleForTesting
    public List<Pattern> getFailedActionPatterns() {
        return failedActionPatterns;
    }
}
