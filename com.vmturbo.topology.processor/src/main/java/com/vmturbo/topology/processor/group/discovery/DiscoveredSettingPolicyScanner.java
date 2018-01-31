package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This scanner scans entities for utilization thresholds that require a utilization threshold to be created.
 * If discover a host entity that has a memory or CPU utilization threshold percentage set, we create
 * a setting policy to enforce the threshold.
 *
 * Ideally there would be a better way to surface utilization thresholds in the UI as opposed to hacking
 * settings to do so. If and when we rethink how to surface this sort of information, we should
 * delete this scanner and the related pieces of code.
 *
 * Only VCenter and VMM hosts should be scanned for these thresholds.
 * {@see https://vmturbo.atlassian.net/browse/OM-28320 and
 * https://vmturbo.atlassian.net/wiki/spaces/Home/pages/356646963/Discovered+Settings}
 *
 * Currently scans physical machines associated with VCenter and VMM probe types. Any utilization
 * thresholds set on CPU and Mem commodities result in the creation of {@link DiscoveredSettingPolicyInfo}s.
 *
 * We attempt to minimize the number of setting policies created by grouping groups with the same
 * values for CPU and Mem utilization thresholds into a single setting policy.
 */
public class DiscoveredSettingPolicyScanner {

    private static final Logger logger = LogManager.getLogger();

    public static final double DEFAULT_UTILIZATION_THRESHOLD = 100.0;

    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    public DiscoveredSettingPolicyScanner(@Nonnull final ProbeStore probeStore,
                                          @Nonnull final TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Scan the hosts in the stitching context for utilization threshold values for which we need
     * to create discovered groups and setting policies.
     *
     * @param stitchingContext The {@link StitchingContext} containing the entities that should be
     *                         scanned for utilization thresholds requiring discovered setting policies.
     * @param groupUploader The group uploader to be used to upload the discovered groups and setting
     *                      policies discovered during the scan.
     */
    public void scanForDiscoveredSettingPolicies(@Nonnull final StitchingContext stitchingContext,
                                                 @Nonnull final DiscoveredGroupUploader groupUploader) {
        final Scanner scanner = new Scanner(groupUploader);

        scanner.scanVCenterHosts(hostsForProbe(SDKProbeType.VCENTER, stitchingContext));
        scanner.scanVmmHosts(hostsForProbe(SDKProbeType.VMM, stitchingContext));

        scanner.getTargetIdToSettingPoliciesMap().forEach((targetId, targetUtilizationThresholds) -> {
            final List<InterpretedGroup> groups = new ArrayList<>();
            final List<DiscoveredSettingPolicyInfo> settingPolicies = new ArrayList<>();

            final Optional<String> targetName = targetStore.getTargetAddress(targetId);
            if (targetName.isPresent()) {
                targetUtilizationThresholds.getSettingPolicyBuilders().stream()
                    .forEach(builder -> builder.addGroupsAndSettingPolicies(
                        groups, settingPolicies, targetName.get()));
                groupUploader.addDiscoveredGroupsAndPolicies(targetId, groups, settingPolicies);
            } else {
                logger.error("Unable to find targetName for target {}. Skipping " +
                    "setting policy creation for {} setting policies.", targetId,
                    targetUtilizationThresholds.getSettingPolicyBuilders().size());
            }
        });
    }

    /**
     * Get a stream of all the hosts discovered by a given probe.
     *
     * @param probeType The type of the probe whose hosts (physical machines) should be retrieved.
     * @param stitchingContext The stitching context containing entities that can be used to look up hosts.
     * @return A stream of all the hosts discovered by a given probe.
     */
    private Stream<TopologyStitchingEntity> hostsForProbe(@Nonnull final SDKProbeType probeType,
                                                          @Nonnull final StitchingContext stitchingContext) {
        return probeStore.getProbeIdForType(probeType.getProbeType())
            .map(probeId -> targetStore.getProbeTargets(probeId).stream()
                .map(Target::getId)
                .flatMap(targetId -> stitchingContext.internalEntities(EntityType.PHYSICAL_MACHINE, targetId)))
            .orElse(Stream.empty());
    }

    /**
     * A helper class that scans physical machine (host) entities for CPU and Mem utilization thresholds
     * on their respective commodities.
     *
     * When hosts are scanned and a utilization threshold requiring a setting policy is found, automatically
     * creates a setting policy that mandates the found utilization levels. If a setting policy with those
     * values already exists, it reuses that setting policy by adding the cluster/group for that host
     * to the setting policy.
     */
    private static class Scanner {
        private final ComputeClusterMemberCache clusterMemberCache;
        private final Map<Long, TargetSettingPolicies> targetIdToSettingPoliciesMap;

        public Scanner(@Nonnull final DiscoveredGroupUploader groupUploader) {
            this.clusterMemberCache =
                new ComputeClusterMemberCache(groupUploader.getDiscoveredGroupInfoByTarget());
            targetIdToSettingPoliciesMap = new HashMap<>();
        }

        /**
         * Get the map of targetIds to {@link TargetSettingPolicies}.
         *
         * @return the map of targetIds to {@link TargetSettingPolicies}.
         */
        public Map<Long, TargetSettingPolicies> getTargetIdToSettingPoliciesMap() {
            return targetIdToSettingPoliciesMap;
        }

        /**
         * Scan VCenter-discovered hosts for hosts requiring Mem/CPU utilization threshold settings. All
         * VC hosts are required to be associated with a cluster. If a VC host has commodities that require
         * utilization threshold settings, we create a setting policy and associate that hosts cluster
         * with the policy.
         *
         * @param vcHosts The list of hosts discovered by VCenter to scan for utilization settings.
         */
        public void scanVCenterHosts(@Nonnull final Stream<TopologyStitchingEntity> vcHosts) {
            vcHosts
                // Find all hosts with at least one commodity sold requiring a utilization setting.
                .filter(host -> host.getCommoditiesSold().filter(this::requiresUtilizationSetting).count() > 0)
                .forEach(host -> {
                    final Optional<String> clusterName = clusterMemberCache.groupComponentClusterNameForHost(host);
                    if (clusterName.isPresent()) {
                        setupSettingPolicy(host, clusterName, host.getCommoditiesSold()
                            .filter(this::requiresUtilizationSetting)
                            .collect(Collectors.toList()));
                    } else {
                        logger.error("Unable to handle memory or cpu utilization threshold for host {}. " +
                            "Unable to find an associated compute cluster.", host.getOid());
                    }
                });
        }

        /**
         * Scan VMM-discovered hosts for hosts requiring Mem/CPU utilization threshold settings. All VMM
         * hosts are required to be associated with a cluster. If a VC host has commodities that require
         * utilization threshold settings, we create a setting policy and associate that hosts cluster
         * with the policy.
         *
         * @param vmmHosts The list of hosts discovered by VCenter to scan for utilization settings.
         */
        public void scanVmmHosts(@Nonnull final Stream<TopologyStitchingEntity> vmmHosts) {
            vmmHosts.filter(host ->
                host.getCommoditiesSold().filter(this::requiresUtilizationSetting).count() > 0)
                .forEach(host -> setupSettingPolicy(host, Optional.empty(), host.getCommoditiesSold()
                    .filter(this::requiresUtilizationSetting)
                    .collect(Collectors.toList())));
        }

        /**
         * Set up a setting policy for the given host and clusterName with values determined
         * by the list of commodities containing thresholds. This results in adding an entry
         * in the targetIdToSettingPoliciesMap which will be used to construct the corresponding
         * {@link DiscoveredSettingPolicyInfo}.
         *
         * @param host The host for which the setting policy should be setup to include.
         * @param clusterName The name of the compute cluster containing the host. If none is provided,
         *                    a new group will be created containing the host.
         * @param commoditiesWithThresholds The commodities from the host containing mem/cpu utilization
         *                                  threhsolds.
         */
        private void setupSettingPolicy(@Nonnull final TopologyStitchingEntity host,
                                        @Nonnull final Optional<String> clusterName,
                                        @Nonnull final List<CommodityDTO.Builder> commoditiesWithThresholds) {
            // Create the setting policy.
            // Note that we differ from legacy here in that we always create the setting regardless of
            // whether there is a setting to ignore HA. IgnoreHA will be examined in conjunction with
            // this setting when settings are applied.
            final TargetSettingPolicies targetSettingPolicies =
                targetIdToSettingPoliciesMap.computeIfAbsent(host.getTargetId(), targetId ->
                    new TargetSettingPolicies());
            final DiscoveredSettingPolicyCreator settingPolicyBuilder =
                targetSettingPolicies.builderFor(commoditiesWithThresholds);
            settingPolicyBuilder.applyToHost(host.getOid(), clusterName);
        }

        /**
         * Determine if a commodity requires a utilization setting.
         *
         * @param commoditySold The commodity to check.
         * @return True if the commodity requires a utilization setting, false otherwise.
         */
        private boolean requiresUtilizationSetting(@Nonnull final CommodityDTO.Builder commoditySold) {
            return (commoditySold.getCommodityType() == CommodityType.MEM ||
                commoditySold.getCommodityType() == CommodityType.CPU)
                && commoditySold.hasUtilizationThresholdPct()
                && commoditySold.getUtilizationThresholdPct() != DEFAULT_UTILIZATION_THRESHOLD;
        }
    }

    /**
     * Contains a map of {@link DiscoveredSettingPolicyCreator} organized by
     * {@link UtilizationThresholdValues} for a given target.
     */
    private static class TargetSettingPolicies {
        final Map<UtilizationThresholdValues, DiscoveredSettingPolicyCreator> thresholdValuesToBuilders =
            new HashMap<>();

        /**
         * Get the {@link DiscoveredSettingPolicyCreator} for the utilization threshold values in the given list
         * of commodities sold. If no {@link DiscoveredSettingPolicyCreator} exists for the given target with
         * the desired {@link UtilizationThresholdValues}, a new {@link DiscoveredSettingPolicyCreator} will
         * be created, stored for later use, and returned.
         *
         * @param commoditiesSold The Memory and/or CPU commodities potentially containing utilization threshold values.
         *                        At least one of these commodities must contain a utilization threshold value.
         * @return A {@link DiscoveredSettingPolicyCreator} for the Mem/CPU utilization thresholds on
         *         the input commodities.
         */
        public DiscoveredSettingPolicyCreator builderFor(@Nonnull final List<CommodityDTO.Builder> commoditiesSold) {
            Preconditions.checkArgument(commoditiesSold.size() <= 2); // Max size is 2 (MEM + CPU)
            Preconditions.checkArgument(!commoditiesSold.isEmpty());

            final UtilizationThresholdValues values = new UtilizationThresholdValues(
                utilizationThresholdFor(commoditiesSold, CommodityType.MEM),
                utilizationThresholdFor(commoditiesSold, CommodityType.CPU)
            );

            return thresholdValuesToBuilders.computeIfAbsent(values,
                v -> new DiscoveredSettingPolicyCreator(values));
        }

        /**
         * Get all the {@link DiscoveredSettingPolicyCreator}s for the given target.
         *
         * @return the {@link DiscoveredSettingPolicyCreator}s for the given target.
         */
        public Collection<DiscoveredSettingPolicyCreator> getSettingPolicyBuilders() {
            return thresholdValuesToBuilders.values();
        }

        /**
         * Get the utilization threshold percentage value for a given commodity with a given type.
         *
         * @param commoditiesSold The list of commodities to be scanned for a commodity of the given type
         *                        with a utilization threshold percentage set.
         * @param targetType The type of the commodity that should be searched for.
         * @return The utilization threshold percentage value for a given commodity with a given type.
         *         If no such commodity is found, returns {@link Optional#empty()}.
         */
        private Optional<Double> utilizationThresholdFor(@Nonnull final List<CommodityDTO.Builder> commoditiesSold,
                                                         final CommodityType targetType) {
            return commoditiesSold.stream()
                .filter(Builder::hasUtilizationThresholdPct)
                .filter(commodity -> commodity.getCommodityType() == targetType)
                .map(Builder::getUtilizationThresholdPct)
                .findFirst();
        }
    }

    /**
     * Wraps optional mem and cpu utilization thresholds.
     *
     * Overrides {@link #equals(Object)} and {@link #hashCode()} so that these objects can be
     * compared and used as keys in {@link HashMap}s.
     */
    private static class UtilizationThresholdValues {
        private final Optional<Float> memUtilizationThresholdPercentage;
        private final Optional<Float> cpuUtilizationThresholdPercentage;

        /**
         * Create a new {@link UtilizationThresholdValues}. At least one of the mem or cpu utilization
         * values must be present.
         *
         * @param memUtilizationThresholdPercentage The memory utilization percentage.
         * @param cpuUtilizationThresholdPercentage The CPU utilization percentage.
         */
        public UtilizationThresholdValues(@Nonnull final Optional<Double> memUtilizationThresholdPercentage,
                                          @Nonnull final Optional<Double> cpuUtilizationThresholdPercentage) {
            // At least one of mem or CPU utilization threshold must be non-null.
            Preconditions.checkArgument(memUtilizationThresholdPercentage.isPresent() ||
                cpuUtilizationThresholdPercentage.isPresent());

            this.memUtilizationThresholdPercentage = memUtilizationThresholdPercentage
                .map(Double::floatValue);
            this.cpuUtilizationThresholdPercentage = cpuUtilizationThresholdPercentage
                .map(Double::floatValue);
        }

        /**
         * Get the mem utilization threshold percentage.
         *
         * @return the mem utilization threshold percentage.
         */
        @Nonnull
        public Optional<Float> getMemUtilizationThresholdPercentage() {
            return memUtilizationThresholdPercentage;
        }

        /**
         * Get the CPU utilization threshold percentage.
         *
         * @return the CPU utilization threshold percentage.
         */
        @Nonnull
        public Optional<Float> getCpuUtilizationThresholdPercentage() {
            return cpuUtilizationThresholdPercentage;
        }

        /**
         * Get a setting for the mem utilization threshold.
         *
         * @return a setting for the mem utilization threshold. Returns {@link Optional#empty()} if the
         *         mem utilization is not present.
         */
        @Nonnull
        public Optional<Setting.Builder> getMemUtilizationSetting() {
            return getMemUtilizationThresholdPercentage()
                .map(value -> Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.MemoryUtilization.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value)));
        }

        /**
         * Get a setting for the CPU utilization threshold.
         *
         * @return a setting for the CPU utilization threshold. Returns {@link Optional#empty()} if the
         *         CPU utilization is not present.
         */
        @Nonnull
        public Optional<Setting.Builder> getCpuUtilizationSetting() {
            return getCpuUtilizationThresholdPercentage()
                .map(value -> Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.CpuUtilization.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value)));
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(memUtilizationThresholdPercentage,
                cpuUtilizationThresholdPercentage);
        }

        @Override
        public boolean equals(@Nullable Object other) {
            if (!(other instanceof UtilizationThresholdValues)) {
                return false;
            }

            final UtilizationThresholdValues v = (UtilizationThresholdValues)other;
            return com.google.common.base.Objects.equal(memUtilizationThresholdPercentage,
                v.memUtilizationThresholdPercentage) &&
                com.google.common.base.Objects.equal(cpuUtilizationThresholdPercentage,
                    v.cpuUtilizationThresholdPercentage);
        }
    }

    /**
     * A class that associates discovered groups and discovered setting policies.
     */
    private static class DiscoveredSettingPolicyCreator {
        private final UtilizationThresholdValues utilizationThresholdValues;
        private final List<Long> hostOids = new ArrayList<>();
        private final Set<String> clusterNames = new HashSet<>();

        public DiscoveredSettingPolicyCreator(@Nonnull final UtilizationThresholdValues utilizationThresholdValues) {
            this.utilizationThresholdValues = Objects.requireNonNull(utilizationThresholdValues);
        }

        /**
         * Apply the setting policy associated with this {@link DiscoveredSettingPolicyCreator} to a given host.
         * Does not check if the host was already added. If the clusterName is provided, will also associate
         * this {@link DiscoveredSettingPolicyCreator} with the cluster with that name. If no such cluster
         * is provided, a new group will be created consisting of all the hosts added to this
         * {@link DiscoveredSettingPolicyCreator} when asked to
         * {@link #addGroupsAndSettingPolicies(List, List, String)}.
         *
         * @param hostOid The oid of the host.
         * @param clusterName The optional cluster containing this host.
         */
        public void applyToHost(final long hostOid, @Nonnull final Optional<String> clusterName) {
            if (clusterName.isPresent()) {
                clusterNames.add(clusterName.get());
            } else {
                hostOids.add(hostOid);
            }
        }

        /**
         * Add the setting policy and (if necessary) associated group for this
         * {@link DiscoveredSettingPolicyCreator} to the input lists of groups and setting policies.
         *
         * @param groups If this {@link DiscoveredSettingPolicyCreator} has hosts unassociated with clusters
         *               to it, create a group for those hosts and associate that group with the setting policy.
         *               Finally, add this group to the list of groups.
         * @param settingPolicies The list of setting policies that this {@link DiscoveredSettingPolicyCreator}
         *                        will add a setting policy to. This setting policy is always added,
         *                        not conditionally added.
         */
        public void addGroupsAndSettingPolicies(@Nonnull final List<InterpretedGroup> groups,
                                                @Nonnull final List<DiscoveredSettingPolicyInfo> settingPolicies,
                                                @Nonnull final String targetName) {
            DiscoveredSettingPolicyInfo.Builder settingBuilder = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);

            if (!hostOids.isEmpty()) {
                final String groupName = composeGroupName(targetName);
                // Associate the policy with the group.
                settingBuilder.addDiscoveredGroupNames(groupName);

                final CommonDTO.GroupDTO groupDTO = CommonDTO.GroupDTO.newBuilder()
                    .setDisplayName(groupName)
                    .setGroupName(groupName)
                    .setEntityType(EntityType.PHYSICAL_MACHINE)
                    .setMemberList(MembersList.newBuilder()
                            .addAllMember(hostOids.stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()))
                    ).build();

                final GroupInfo groupInfo = GroupInfo.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setName(groupName)
                    .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addAllStaticMemberOids(hostOids))
                    .build();

                groups.add(new InterpretedGroup(groupDTO, Optional.of(groupInfo), Optional.empty()));
            }

            // Create a setting policy with settings for the utilization thresholds
            settingBuilder.addAllDiscoveredGroupNames(clusterNames);
            settingBuilder.setName(composeName(targetName));
            utilizationThresholdValues.getMemUtilizationSetting().ifPresent(settingBuilder::addSettings);
            utilizationThresholdValues.getCpuUtilizationSetting().ifPresent(settingBuilder::addSettings);

            settingPolicies.add(settingBuilder.build());
        }

        /**
         * Returns a name in the form of "memUtilzation-0.5-cpuUtilization-0.2/targetName"
         *
         * @return the name.
         */
        private String composeName(@Nonnull final String targetName) {
            String name = utilizationThresholdValues.getMemUtilizationThresholdPercentage()
                .map(value -> "memUtilization-" + value).orElse("");
            name += (name.isEmpty() ? "" : "-") +
                utilizationThresholdValues.getCpuUtilizationThresholdPercentage()
                .map(value -> "cpuUtilization-" + value).orElse("");
            name += "/" + targetName;

            return name;
        }

        /**
         * Returns the name of the group associated with the setting policy.
         *
         * @return the name of the group associated with the setting policy.
         */
        private String composeGroupName(@Nonnull final String targetName) {
            return composeName(targetName) + "-group";
        }
    }
}
