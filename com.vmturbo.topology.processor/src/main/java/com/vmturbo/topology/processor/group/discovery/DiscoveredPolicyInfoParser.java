package com.vmturbo.topology.processor.group.discovery;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class for parsing spec of DRS rules from GroupDTOs.
 * These specs then will be send to group component to create Policies.
 */
@NotThreadSafe
public class DiscoveredPolicyInfoParser {

    /**
     * Size of set of groups which are engaged by the certain DRS rule.
     */
    private static final int GROUPS_IN_POLICY = 2;
    /**
     * Name consists of parts which are separated by '/'. One of parts is cluster name.
     */
    private static final int CLUSTER_PART_OF_NAME = 1;

    private static final Logger logger = LogManager.getLogger();

    private final Map<String, CommonDTO.GroupDTO> foundClusters = new HashMap<>();

    private final List<CommonDTO.GroupDTO> allGroups;

    public DiscoveredPolicyInfoParser(@Nonnull List<CommonDTO.GroupDTO> groups) {
        Objects.requireNonNull(groups);
        allGroups = groups;
    }

    /**
     * Searches pairs of groups which are engaged by the same DRS rule.
     * Then creates these pairs DRS rule specs.
     *
     * @return Created DRS rule Specs
     */
    @Nonnull
    public List<DiscoveredPolicyInfo> parsePoliciesOfGroups() {

        final List<DiscoveredPolicyInfo> parsedInfos = new LinkedList<>();
        collectPolicyGroups()
                .forEach(policyGroups ->  {
                    parsePolicyFromGroups(policyGroups.getValue())
                            .ifPresent(parsedInfos::add);
                });
        return parsedInfos;
    }

    /**
     * Assorts groupDTOs by constraintId for finding pairs of GroupDTOs which are engaged by
     * DRS rule and parses DRS rule spec from pairs.
     *
     * @return Entries of constraintId and list of groups engaged by this constraint
     */
    private Stream<Entry<String, List<CommonDTO.GroupDTO>>> collectPolicyGroups() {
        return allGroups.stream()
                .collect(
                        Collectors.groupingBy(group -> group.getConstraintInfo().getConstraintId()))
                .entrySet()
                .stream()
                .filter(entry -> isPolicyEntry(entry.getValue()));
    }

    /**
     * Returns true if entry has groups engaged by DRS rule, or if entry has
     * VM-group, which engaged by DRS rule with some Cluster
     *
     * @param groups Entry of ConstraintId and List of Policies whith this Constraint id
     * @return True if it's possible to parse policy from this entry
     */
    private boolean isPolicyEntry(@Nonnull List<CommonDTO.GroupDTO> groups) {
        return groups.size() == GROUPS_IN_POLICY || isVmClusterPolicy(groups);
    }

    /**
     * Returnes true if VM group is engaged with another VM group
     * by VM_VM Affinity of Antiaffinity.
     *
     * @param groups Groups engaged by policy
     * @return
     */
    private boolean isVmClusterPolicy(@Nonnull List<CommonDTO.GroupDTO> groups) {
        ConstraintType constraintType = groups.get(0).getConstraintInfo().getConstraintType();
        return (constraintType == ConstraintType.BUYER_BUYER_AFFINITY ||
                constraintType == ConstraintType.BUYER_BUYER_ANTI_AFFINITY) &&
                groups.size() == 1;
    }

    /**
     * Parses policy from policyGroups, which are pair of DRS-rule groups or
     * only VM-group which may be engaged with some Cluster by Policy
     * and add parsed policy to list
     *
     * @param policyGroups Groups for parsing (Pair or just VM-group).
     * @return Created policy if everything was ok and it was created.
     */
    private Optional<DiscoveredPolicyInfo> parsePolicyFromGroups(@Nonnull List<CommonDTO.GroupDTO>
            policyGroups) {

        if (isVmClusterPolicy(policyGroups)) {
            return parseVmClusterPolicy(policyGroups);
        } else {
            return Optional.of(parsePolicyFromPairOfGroups(policyGroups.get(0),
                    policyGroups.get(1)));
        }
    }

    /**
     * Searches cluster and creates policy for VM-Cluster groups.
     * @param policyGroups Has one VM group
     * @return Created policy if everything was ok and it was created.
     */
    private Optional<DiscoveredPolicyInfo> parseVmClusterPolicy(@Nonnull List<CommonDTO.GroupDTO>
            policyGroups) {

        CommonDTO.GroupDTO vmGroup = policyGroups.get(0);
        Optional<String> policyClusterName = getPolicyClusterName(vmGroup);
        if (!policyClusterName.isPresent()) {
            logger.warn("Can't parse a cluster name from VM-group name.");
            return Optional.empty();
        }
        Optional<CommonDTO.GroupDTO> cluster = findPolicyCluster(policyClusterName.get());
        if (cluster.isPresent()) {
            return Optional.of(parsePolicy(vmGroup, cluster.get()));
        } else {
            logger.warn("Can't find the cluster with such name.");
            return Optional.empty();
        }
    }

    /**
     * Parses name of policy group and gets name of cluster from this.
     *
     * @param policyGroup Discovered group. It has name of cluster as a part of name.
     * @return Name of policy cluster if it was found.
     */
    private Optional<String> getPolicyClusterName(@Nonnull CommonDTO.GroupDTO policyGroup) {
        /*
        * There is not the most reliable way to figure out the related cluster.
        * We suppose that it should be changed in future.
        */
        final String[] nameParts = policyGroup.getDisplayName().split("/");
        if (nameParts.length > CLUSTER_PART_OF_NAME) {
            return Optional.of(nameParts[CLUSTER_PART_OF_NAME]);
        }
        return Optional.empty();
    }

    /**
     * Searches in allGroups cluster with the certain name.
     *
     * @param clusterName Name of cluster which is parsed from VM-group name.
     * @return Cluster with the same name if it was found.
     */
    private Optional<CommonDTO.GroupDTO> findPolicyCluster(@Nonnull String clusterName) {

        CommonDTO.GroupDTO clusterInMap = foundClusters.get(clusterName);
        if (clusterInMap != null) {
            return Optional.of(clusterInMap);
        }
        Optional <CommonDTO.GroupDTO> clusterOpt = allGroups.stream()
                .filter(group -> group.getDisplayName().equals(clusterName) &&
                        group.getConstraintInfo().getConstraintType() ==
                                CommonDTO.GroupDTO.ConstraintType.CLUSTER)
                .findFirst();
        clusterOpt.ifPresent(cluster -> foundClusters.put(cluster.getDisplayName(), cluster));
        return clusterOpt;
    }

    /**
     * Identifies which group is seller-group and which is buyer and creates policy spec.
     * The order of the params is not important.
     *
     * @param firstGroup One of the groups of the Policy. May be buyers or sellers group.
     * @param secondGroup One of the groups of the Policy. May be buyers or sellers group.
     * @return Created policy.
     */
    private DiscoveredPolicyInfo parsePolicyFromPairOfGroups(@Nonnull CommonDTO.GroupDTO firstGroup,
                                                             @Nonnull CommonDTO.GroupDTO secondGroup) {

        if (firstGroup.getConstraintInfo().getIsBuyer()) {
            return parsePolicy(firstGroup, secondGroup);
        } else {
            return parsePolicy(secondGroup, firstGroup);
        }
    }

    /**
     * Parses new DRS rule spec from two groups, which are engaged by the same policy.
     * Order of input parameters is important.
     *
     * @param buyers Group of Policy which is Buyers group.
     * @param sellers Group of Policy which is Sellers group.
     * @return Created policy.
     */
    private DiscoveredPolicyInfo parsePolicy(@Nonnull CommonDTO.GroupDTO buyers,
                                             @Nonnull CommonDTO.GroupDTO sellers) {

        final CommonDTO.GroupDTO.ConstraintInfo constraintInfo = buyers.getConstraintInfo();
        return DiscoveredPolicyInfo.newBuilder()
                .setPolicyName(constraintInfo.getConstraintName())
                .setBuyersGroupStringId(createStringId(buyers))
                .setSellersGroupStringId(createStringId(sellers))
                .setConstraintType(buyers.getConstraintInfo().getConstraintType().getNumber())
                .build();
    }

    public static String createStringId(CommonDTO.GroupDTO group) {
        return group.getDisplayName() + "-" + group.getEntityType().toString();
    }
}
