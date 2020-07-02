package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * The {@link DiscoveredGroupUploader} is the interface for the discovery operation to upload
 * discovered {@link CommonDTO.GroupDTO}s, Policies, and Settings to the Group component.
 *
 * <p>The uploader should is thread safe. Discovered groups, policies, and settings may be
 * set while an upload is in progress. Upload happens during a
 * {@link com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline} stage in the
 * broadcast pipeline, while discoveries happen asynchronously with this pipeline.
 *
 * <p>Uploading discovered groups does NOT clear the latest discovered groups, policies, and settings
 * for targets known to the uploader. Thus, if no new groups, policies, or settings are set for
 * a target since the last time that target's results were uploaded, the previous ones will
 * be re-uploaded the next time that {@link #uploadDiscoveredGroups(Map, ConsistentScalingManager)} is called.
 *
 * <p>TODO: (DavidBlinn 1/31/2018) There is a problem with how we presently handle
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

    private static final String PROBE_TYPE_OTHER = "OTHER";

    private final Logger logger = LogManager.getLogger();

    private final GroupServiceStub groupServiceStub;

    private final DiscoveredGroupInterpreter discoveredGroupInterpreter;

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

    private final TargetStore targetStore;

    /**
     * A map from targetId to the list of the most recent {@link DiscoveredGroupInfo} for that
     * target.
     *
     * <p>This is for debugging purposes only - to support easily viewing the latest discovered
     * groups.
     */
    private final Map<Long, TargetDiscoveredData> dataByTarget = new HashMap<>();

    private Map<Long, Long> hypervisorPM2datacenterOIDs;

    @VisibleForTesting
    DiscoveredGroupUploader(
            @Nonnull final GroupServiceStub groupServiceStub,
            @Nonnull final DiscoveredGroupInterpreter discoveredGroupInterpreter,
            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache,
            @Nonnull final TargetStore targetStore) {
        this.groupServiceStub = Objects.requireNonNull(groupServiceStub);
        this.discoveredGroupInterpreter = Objects.requireNonNull(discoveredGroupInterpreter);
        this.discoveredClusterConstraintCache =  Objects.requireNonNull(discoveredClusterConstraintCache);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Constructor for use from the Spring context.
     *
     * @param groupServiceStub To access the group service.
     * @param entityStore {@link EntityStore} to use to get target-specific entity information.
     * @param discoveredClusterConstraintCache See {@link DiscoveredClusterConstraintCache}.
     * @param targetStore {@link TargetStore} for target-related information.
     */
    public DiscoveredGroupUploader(
            @Nonnull final GroupServiceStub groupServiceStub,
            @Nonnull final EntityStore entityStore,
            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache,
            @Nonnull final TargetStore targetStore) {
        this(groupServiceStub, new DiscoveredGroupInterpreter(entityStore),
                discoveredClusterConstraintCache, targetStore);
    }


    /**
     * Set the discovered groups for a target. This overwrites any existing discovered
     * group information for the target.
     *
     * <p>This also clears any previously discovered setting policies for this target.
     *
     * @param targetId The id of the target whose groups were discovered.
     * @param groups The discovered groups for the target.
     */
    public void setTargetDiscoveredGroups(final long targetId,
                                          @Nonnull final List<CommonDTO.GroupDTO> groups) {
        final List<InterpretedGroup> interpretedDtos =
                discoveredGroupInterpreter.interpretSdkGroupList(groups, targetId);
        final DiscoveredPolicyInfoParser parser = new DiscoveredPolicyInfoParser(groups, targetId);
        final List<DiscoveredPolicyInfo> discoveredPolicyInfos = parser.parsePoliciesOfGroups();
        final List<DiscoveredSettingPolicyInfo> discoveredSettingPolicyInfos = new ArrayList<>();
        discoveredSettingPolicyInfos
                    .addAll(convertTemplateExclusionGroupsToPolicies(targetId, groups));
        discoveredSettingPolicyInfos
                    .addAll(convertConsistentScalingGroupsToPolicies(targetId, groups));

        final TargetDiscoveredData discoveredData = new TargetDiscoveredData(interpretedDtos,
            discoveredPolicyInfos, discoveredSettingPolicyInfos);

        synchronized (dataByTarget) {
            dataByTarget.put(targetId, discoveredData);
            discoveredClusterConstraintCache.storeDiscoveredClusterConstraint(targetId, groups);
        }
    }

    /**
     * Create policies to enable consistent scaling on groups that have it enabled via mediation.
     *
     * @param targetId The id of the target whose groups were discovered
     * @param groups GroupDTOs to process
     * @return list of DiscoveredSettingPolicyInfo created for consistent scaling
     */
    private List<DiscoveredSettingPolicyInfo> convertConsistentScalingGroupsToPolicies(
                    final long targetId,
                    final List<GroupDTO> groups) {
        List<DiscoveredSettingPolicyInfo> discoveredPolicyInfos = new ArrayList<>();
        for (GroupDTO group : groups) {
            if (!group.getIsConsistentResizing()) {
                continue;
            }
            DiscoveredSettingPolicyInfo.Builder policy = DiscoveredSettingPolicyInfo.newBuilder();
            Setting.Builder setting = Setting.newBuilder();
            setting.setSettingSpecName(EntitySettingSpecs.EnableConsistentResizing.getSettingName());
            setting.setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true));
            policy.setEntityType(group.getEntityType().getNumber());
            policy.addDiscoveredGroupNames(GroupProtoUtil.createIdentifyingKey(group));
            String targetName = getTargetDisplayName(targetId);
            String displayName = String.format("%s - %s - Consistent Scaling Policy (account %d)",
                                        targetName, group.getDisplayName(), targetId);
            policy.setDisplayName(displayName);
            policy.setName(createPolicyName(group, targetId, "CSP"));
            policy.addSettings(setting);
            discoveredPolicyInfos.add(policy.build());
        }
        return discoveredPolicyInfos;
    }

    /**
     * Convert template exclusion GroupDTOs to DiscoveredSettingPolicyInfos.
     *
     * @param targetId The id of the target whose groups were discovered
     * @param groups GroupDTOs to process
     * @return the list of DiscoveredSettingPolicyInfos
     */
    @Nonnull
    private List<DiscoveredSettingPolicyInfo> convertTemplateExclusionGroupsToPolicies(
            long targetId, @Nonnull List<CommonDTO.GroupDTO> groups) {
        List<DiscoveredSettingPolicyInfo> result = new ArrayList<>();
        String targetName = getTargetDisplayName(targetId);

        for (GroupDTO group : groups) {
            if (group.getConstraintInfo()
                    .getConstraintType() != ConstraintType.TEMPLATE_EXCLUSION) {
                continue;
            }

            SortedSetOfOidSettingValue.Builder oids = SortedSetOfOidSettingValue.newBuilder();
            oids.addAllOids(discoveredGroupInterpreter.convertTemplateNamesToOids(group, targetId,
                    targetName));

            Setting.Builder setting = Setting.newBuilder();
            setting.setSettingSpecName(EntitySettingSpecs.ExcludedTemplates.getSettingName());
            setting.setSortedSetOfOidSettingValue(oids);

            DiscoveredSettingPolicyInfo.Builder policy = DiscoveredSettingPolicyInfo.newBuilder();
            policy.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
            policy.addDiscoveredGroupNames(GroupProtoUtil.createIdentifyingKey(group));
            String name = String.format("%s - %s - %s (account %d)", targetName,
                    group.getDisplayName(), "Cloud Compute Tier Exclusion Policy", targetId);
            policy.setDisplayName(name);
            policy.setName(createPolicyName(group, targetId, "EXP"));
            policy.addSettings(setting);

            result.add(policy.build());
        }

        return result;
    }

    /**
     * Create a policy name based on group name and discovering target oid. It will make the name
     * stays under 255 character.
     *
     * @param group the group which has the policy.
     * @param targetId the id for target discovering the group.
     * @param prefix the prefix distinguishing different type of policies.
     * @return the name created for policy.
     */
    private String createPolicyName(GroupDTO group, long targetId, String prefix) {
        // It is expected that the group name is unique in the scope of that target.
        // Therefore, the concatenation of group name and target id should be unqiue.
        final String name = String.join(":", prefix, group.hasGroupName()
            ? group.getGroupName() : group.getConstraintInfo().getConstraintId(),
            String.valueOf(targetId));
        // The name of policy should not be longer than character limit. Fix if that is the case
        return SDKUtil.fixUuid(name, 255, 200);
    }

    @Nonnull
    private String getTargetDisplayName(long targetId) {
        Optional<String> name = targetStore.getTargetDisplayName(targetId);
        return name.orElseGet(() -> String.valueOf(targetId));
    }

    /**
     * Insert scanned groups and setting policies for a target. This overrides previously set
     * scanned groups and setting policies for this target, so it is the caller's responsibility
     * to do a single call per-target per-broadcast.
     *
     * @param targetId The id of the target.
     * @param interpretedGroups The scanned groups. These will override the current scanned groups for this target.
     * @param settingPolicies The scanned setting policies. These will override the current scanned setting policies for this target.
     */
    public void setScannedGroupsAndPolicies(final long targetId,
                                            @Nonnull final Collection<InterpretedGroup> interpretedGroups,
                                            @Nonnull final Collection<DiscoveredSettingPolicyInfo> settingPolicies) {
        synchronized (dataByTarget) {
            final TargetDiscoveredData curDiscoveredData =
                dataByTarget.computeIfAbsent(targetId, k -> TargetDiscoveredData.empty());
            curDiscoveredData.setScannedData(interpretedGroups, settingPolicies);
        }
    }

    /**
     * Get the current {@link TargetDiscoveredData} of each target. Subsequent target discoveries
     * will not affect the returned results - though subsequent broadcasts may add scanned groups
     * and setting policies!
     *
     * @return Map of (target id) -> (current {@link TargetDiscoveredData} for target).
     */
    @Nonnull
    public Map<Long, TargetDiscoveredData> getDataByTarget() {
        synchronized (dataByTarget) {
            return ImmutableMap.copyOf(dataByTarget);
        }
    }

    /**
     * Restore setting policies from diags, overwriting all current discovered setting policies for
     * that target. All current scanned setting policies for that target get dropped. This method
     * is only intended to be used when restoring diags!
     *
     * @param targetId The ID of the target.
     * @param discovered The discovered setting policies to restore. These will completely overwrite
     *                   the current discovered setting policies for this target.
     */
    public void restoreDiscoveredSettingPolicies(final long targetId,
                                                 @Nonnull final List<DiscoveredSettingPolicyInfo> discovered) {
        synchronized (dataByTarget) {
            TargetDiscoveredData oldData = dataByTarget.computeIfAbsent(targetId,
                k -> TargetDiscoveredData.empty());
            final TargetDiscoveredData newData = new TargetDiscoveredData(
                oldData.getDiscoveredGroups().collect(Collectors.toList()),
                oldData.getPolicies().collect(Collectors.toList()),
                discovered);
            dataByTarget.put(targetId, newData);
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
        synchronized (dataByTarget) {
            final Map<Long, List<InterpretedGroup>> groupsByTarget = new HashMap<>();
            dataByTarget.forEach((targetId, discoveredData) -> {
                groupsByTarget.put(targetId, discoveredData.getGroups()
                    .collect(Collectors.toList()));
            });
            return new DiscoveredGroupMemberCache(groupsByTarget);
        }
    }

    /**
     * Upload discovered groups, policies, and settings to the component responsible for managing
     * these items.
     *
     * <p>Uploading discovered groups does NOT clear the latest groups, policies, and settings known
     * to the group uploader.
     *
     * @param input topology entities indexed by oid
     * @param consistentScalingManager consistent scaling manager
     */
    public void uploadDiscoveredGroups(@Nonnull Map<Long, TopologyEntity.Builder> input,
                                       @Nonnull ConsistentScalingManager consistentScalingManager) {
        final List<DiscoveredGroupsPoliciesSettings> requests = new ArrayList<>();

        // Create requests in a synchronized block to guard against changes to discovered groups/settings/policies
        // while an upload is in progress so that the data structures for each type cannot be made to be
        // out of sync with each other.
        synchronized (dataByTarget) {
            dataByTarget.forEach((targetId, targetData) -> {
                final List<InterpretedGroup> groupsCopy = targetData.copyGroups();

                // then we are adding the datacenter prefix to the group name
                // note: we are doing this modification only if the cluster is a compute cluster
                // this is because we cannot easily associate a storage cluster to a datacenter
                groupsCopy.stream()
                    .map(InterpretedGroup::getGroupDefinition)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(group -> GroupType.COMPUTE_HOST_CLUSTER == group.getType())
                    .forEach(cluster -> addDatacenterPrefixToComputeClusterName(input, cluster));

                // OM-52323: Probe types are not necessarily in the SDKProbeType enum. For instance,
                // the kubernetes probe creates a unique probe type like Kubernetes-2411221677 so
                // that XL is able to route Kubernetes rediscoveries to the correct probe instance.
                // Kubernetes will have an instance of the kubernetes probe per kubernetes cluster
                // since it acts as an agent discovery for that cluster.
                // Routing to probe instance only uses probe type. targetId is not used for routing
                // to probe instance.
                // A second use case is third party probes. When a third party implements their own
                // SDK probe, their probe type will not be in the SDKProbeType enum.
                final String probeType = targetStore.getProbeTypeForTarget(targetId)
                    .map(SDKProbeType::toString)
                    .orElseGet(() -> PROBE_TYPE_OTHER);

                final DiscoveredGroupsPoliciesSettings.Builder req =
                    DiscoveredGroupsPoliciesSettings.newBuilder()
                        .setTargetId(targetId)
                        .setProbeType(probeType);
                groupsCopy.forEach(interpretedDto -> {
                    interpretedDto.convertToUploadedGroup().ifPresent(req::addUploadedGroups);
                });

                targetData.getPolicies().forEach(req::addDiscoveredPolicyInfos);
                targetData.getSettingPolicies().forEach(req::addDiscoveredSettingPolicies);
                requests.add(req.build());
            });


            final Collection<Long> emptyTargets = new HashSet<>(
                    targetStore.getAll().stream().map(Target::getId).collect(Collectors.toSet()));
            emptyTargets.removeAll(dataByTarget.keySet());
            for (Long targetId : emptyTargets) {
                requests.add(DiscoveredGroupsPoliciesSettings.newBuilder()
                        .setTargetId(targetId)
                        .setDataAvailable(false)
                        .build());
            }
        }

        // Upload the groups/policies/settings to group component.
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                new StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse>() {
            @Override
            public void onNext(StoreDiscoveredGroupsPoliciesSettingsResponse response) {
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                logger.error("Error uploading discovered groups, settings and policies due to: {}", status);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.debug("Finished uploading the discovered groups, settings and policies");
                finishLatch.countDown();
            }

        };

        StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                groupServiceStub.storeDiscoveredGroupsPoliciesSettings(responseObserver);
        for (DiscoveredGroupsPoliciesSettings record : requests) {
            try {
                requestObserver.onNext(record);
            } catch (RuntimeException e) {
                logger.error("Error uploading groups");
                requestObserver.onError(e);
                throw e;
            }
        }

        requestObserver.onCompleted();
        try {
            // block until we get a response or an exception occurs.
            finishLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // set interrupt flag
            logger.error("Interrupted while waiting for response", e);
        }
    }

    /**
     * As all hosts of cluster are located at the same datacenter, we are getting any host of cluster,
     * and looking for any commodity which it buys from datacenter to find datacenter by oid and
     * add name of datacenter to name of cluster.
     *
     * @param topologyMap to search host and datacenter entity
     * @param cluster to change name of it
     */
    private void addDatacenterPrefixToComputeClusterName(
            @Nonnull Map<Long, TopologyEntity.Builder> topologyMap,
            @Nonnull GroupDefinition.Builder cluster) {
        final Optional<StaticMembersByType> hosts = cluster.getStaticGroupMembers()
                .getMembersByTypeList().stream()
                .filter(staticMembersByType -> EntityType.PHYSICAL_MACHINE_VALUE ==
                        staticMembersByType.getType().getEntity())
                .findAny();

        if (!hosts.isPresent() || hosts.get().getMembersList().isEmpty()) {
            logger.warn("Cannot add the datacenter prefix to the cluster. Empty cluster provided {}",
                    cluster.getDisplayName());
            return;
        }
        // here the assumption is that all the cluster members are hosts.
        // cluster members from the SDK can be also VDC sometimes, but those are filtered before
        // reaching this point
        final TopologyEntity.Builder host = topologyMap.get(hosts.get().getMembers(0));
        if (host == null) {
            logger.error("Topology map doesn't contain host {}", hosts.get().getMembers(0));
            return;
        }
        Long datacenterOid = getDatacenterOID(host);

        if (datacenterOid == null) {
            final Optional<TopologyEntityDTO.CommoditiesBoughtFromProvider> chassisCommodity =
                getChassisCommodityOfHost(host);
            if (!chassisCommodity.isPresent()) {
                logger.error("Host (oid:{},displayName:{}) has no commodities bought from " +
                        "datacenter or chassis",
                    host.getOid(), host.getDisplayName());
                return;
            }
            final TopologyEntity.Builder chassis =
                topologyMap.get(chassisCommodity.get().getProviderId());
            if (chassis == null) {
                logger.error("Topology map doesn't contain chassis with OID {} for host (oid:{}," +
                        "displayName:{})",
                    chassisCommodity.get().getProviderId(), host.getOid(), host.getDisplayName());
                return;
            }
            cluster.setDisplayName(chassis.getDisplayName() + "/" + cluster.getDisplayName());
            return;
        }

        final TopologyEntity.Builder datacenter = topologyMap.get(datacenterOid);
        if (datacenter == null) {
            logger.error("Topology map doesn't contain datacenter with OID {} for host (oid:{},displayName:{})",
                            datacenterOid, host.getOid(), host.getDisplayName());
            return;
        }
        cluster.setDisplayName(datacenter.getDisplayName() + "/" + cluster.getDisplayName());
    }

    private Optional<TopologyEntityDTO.CommoditiesBoughtFromProvider> getDatacenterCommodityOfHost(
            @Nonnull TopologyEntity.Builder host) {
        return host.getEntityBuilder().getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(commodityBundle -> commodityBundle.getProviderEntityType() == EntityType.DATACENTER_VALUE)
                .findFirst();
    }

    private Optional<TopologyEntityDTO.CommoditiesBoughtFromProvider> getChassisCommodityOfHost(
        @Nonnull TopologyEntity.Builder host) {
        return host.getEntityBuilder().getCommoditiesBoughtFromProvidersList()
            .stream()
            .filter(commodityBundle -> commodityBundle.getProviderEntityType() == EntityType.CHASSIS_VALUE)
            .findFirst();
    }

    /**
     * Get the datacenter OID for host.
     * @param host  the host whose datacenter OID we want to get.
     * @return datacenter OID
     */
    private Long getDatacenterOID(TopologyEntity.Builder host) {
        final Optional<CommoditiesBoughtFromProvider> datacenterCommodities =
                        getDatacenterCommodityOfHost(host);
        if (datacenterCommodities.isPresent())  {
            return datacenterCommodities.get().getProviderId();
        }
        //We try to get datacenter OID from the commodities bought by host, but they can be absent
        //if the host got stitched with a chassis (the commodities bought from DC are removed then).
        //In this case we use hypervisorPM2datacenterOIDs. We can't just rely on it as the single
        //source either as it is filled only when both hypervisor and UCS targets are discovered.
        return (hypervisorPM2datacenterOIDs == null) ? null
                        : hypervisorPM2datacenterOIDs.get(host.getOid());
    }

    public void setPM2DCMap(Map<Long, Long> pm2dcMap)   {
        this.hypervisorPM2datacenterOIDs = pm2dcMap;
    }

    /**
     * Checks if a UCS target is present in the current {@link TargetStore} instance.
     * @return true if a UCS target is present.
     */
    public boolean isUCSTargetPresent()  {
        for (Target target : targetStore.getAll())  {
            if (target.getProbeInfo().hasProbeType()
                            && target.getProbeInfo().getProbeType().equals(
                                            SDKProbeType.UCS.getProbeType()))   {
                return true;
            }
        }
        return false;
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

    /**
     * A utility class that keeps the per-target discovered (and scanned) data before it's uploaded
     * to the group component. The kept data includes groups, policies, and setting policies.
     *
     * <p>Discovered groups/policies/setting policies come in directly from the probe. The discovered
     * data is immutable - new discoveries lead to new instances of {@link TargetDiscoveredData} for
     * the target.
     *
     * <p>Scanned groups and setting policies are created by the topology processor based on
     * utilization thresholds of certain entities (see {@link DiscoveredSettingPolicyScanner}).
     * Scanned data is mutable - it is set (via the {@link DiscoveredSettingPolicyScanner} at
     * broadcast.
     */
    public static class TargetDiscoveredData {

        private final List<InterpretedGroup> discoveredGroups;
        private final List<DiscoveredPolicyInfo> discoveredPolicies;
        private final List<DiscoveredSettingPolicyInfo> discoveredSettingPolicies;

        private List<InterpretedGroup> scannedGroups = Collections.emptyList();
        private List<DiscoveredSettingPolicyInfo> scannedSettingPolicies = Collections.emptyList();

        private TargetDiscoveredData(@Nonnull final List<InterpretedGroup> discoveredGroups,
                                     @Nonnull final List<DiscoveredPolicyInfo> discoveredPolicies,
                                     @Nonnull final List<DiscoveredSettingPolicyInfo> discoveredSettingPolicies) {
            this.discoveredGroups = discoveredGroups;
            this.discoveredPolicies = discoveredPolicies;
            this.discoveredSettingPolicies = discoveredSettingPolicies;
        }

        private void setScannedData(@Nonnull final Collection<InterpretedGroup> scannedGroups,
                                    @Nonnull final Collection<DiscoveredSettingPolicyInfo> scannedSettingPolicies) {
            this.scannedGroups = new ArrayList<>(scannedGroups);
            this.scannedSettingPolicies = new ArrayList<>(scannedSettingPolicies);
        }

        /**
         * Utility factory method to create an empty instance of {@link TargetDiscoveredData}.
         *
         * @return The {@link TargetDiscoveredData}.
         */
        public static TargetDiscoveredData empty() {
            return new TargetDiscoveredData(Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
        }

        /**
         * Get the groups for this target. This returns both scanned and discovered groups.
         *
         * @return Scanned and discovered groups for the target.
         */
        public Stream<InterpretedGroup> getGroups() {
            return Stream.concat(discoveredGroups.stream(), scannedGroups.stream());

        }

        /**
         * Get the discovered policies for this target.
         *
         * @return The discovered policies for the target.
         */
        public Stream<DiscoveredPolicyInfo> getPolicies() {
            return discoveredPolicies.stream();
        }

        /**
         * Get the setting policies for this target. Returns both scanned and discovered setting
         * policies.
         *
         * @return Scanned and discovered setting policies for the target.
         */
        public Stream<DiscoveredSettingPolicyInfo> getSettingPolicies() {
            return Stream.concat(discoveredSettingPolicies.stream(), scannedSettingPolicies.stream());
        }

        /**
         * Get ONLY the discovered setting policies for this target. This does NOT return the
         * scanned setting policies. The primary use case for this is diagnostic collection - for
         * diagnostics we do not save the scanned setting policies, because those are implicitly
         * contained in the entity diagnostics.
         *
         * @return The discovered setting policies for this target.
         */
        public Stream<DiscoveredSettingPolicyInfo> getDiscoveredSettingPolicies() {
            return discoveredSettingPolicies.stream();
        }

        /**
         * Get ONLY the discovered groups for this target. This does NOT return the scanned groups.
         * See {@link TargetDiscoveredData#getDiscoveredSettingPolicies()}.
         *
         * @return The discovered groups for this target.
         */
        public Stream<InterpretedGroup> getDiscoveredGroups() {
            return discoveredGroups.stream();
        }

        /**
         * Return deep copy of the discovered groups, which can be modifies without altering
         * the source discovered data.
         *
         * @return List of {@link InterpretedGroup}s that can be further manipulated by the caller.
         */
        List<InterpretedGroup> copyGroups() {
            return getGroups()
                .map(InterpretedGroup::deepCopy)
                .collect(Collectors.toList());
        }
    }
}
