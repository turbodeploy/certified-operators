package com.vmturbo.topology.processor.group.discovery;

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

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;

/**
 * Class for parsing specs of DRS rules from GroupDTOs.
 * These specs are then sent to the group component to create Policies.
 */
@NotThreadSafe
public class DiscoveredPolicyInfoParser {

    private static final Logger logger = LogManager.getLogger();

    private final Map<String, CommonDTO.GroupDTO> foundClusters = new HashMap<>();

    private final List<CommonDTO.GroupDTO> allGroups;

    public DiscoveredPolicyInfoParser(@Nonnull List<CommonDTO.GroupDTO> groups) {
        Objects.requireNonNull(groups);
        allGroups = groups;
    }

    /**
     * Search pairs of groups which are engaged by the same DRS rule.
     * Then create these pairs DRS rule specs.
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
                .collect(Collectors.groupingBy(
                    group -> group.getConstraintInfo().getConstraintId()))
                .entrySet()
                .stream()
                .filter(entry -> isPolicyEntry(entry.getValue()));
    }

    /**
     * Returns true if entry has groups engaged by DRS rule, or if entry has
     * VM-group, which engaged by DRS rule with some Cluster.
     *
     * @param groups Entry of ConstraintId and List of Policies with this Constraint id
     * @return True if it's possible to parse policy from this entry
     */
    private boolean isPolicyEntry(@Nonnull List<CommonDTO.GroupDTO> groups) {
        return isBuyerSellerPolicy(groups) || isBuyerBuyerPolicy(groups);
    }

    /**
     * A buyer/seller policy has exactly two groups. If any of the groups
     * is marked as deleted then don't create the policy.
     *
     * @param groups list of groups associated with the same policy identifier
     * @return whether this is a buyer/seller policy
     */
    private boolean isBuyerSellerPolicy(List<CommonDTO.GroupDTO> groups) {
        return groups.size() == 2
            && groupNotDeleted(groups.get(0))
            && groupNotDeleted(groups.get(1));
    }

    /**
     * When a list of groups is of size 1 and associated with
     * a buyer/buyer affinity or anti-affinity rule - it is a buyer/buyer policy.
     *
     * @param groups list of groups associated with the same policy identifier
     * @return whether this is a buyer/buyer policy
     */
    private boolean isBuyerBuyerPolicy(@Nonnull List<CommonDTO.GroupDTO> groups) {
        // A buyer/buyer policy is associated with exactly one group
        if (groups.size() != 1) {
            return false;
        }
        CommonDTO.GroupDTO group = groups.get(0);
        // If the group was deleted then don't process the policy
        if (!groupNotDeleted(group)) {
            return false;
        }
        ConstraintType constraintType = group.getConstraintInfo().getConstraintType();
        return constraintType == ConstraintType.BUYER_BUYER_AFFINITY
            || constraintType == ConstraintType.BUYER_BUYER_ANTI_AFFINITY;
    }

    /**
     * When a policy is disabled in the target, the discovery
     * message marks at least one of the groups associated with the policy
     * with updateType = DELETED.
     *
     * @param group a discovered group
     * @return whether the group is marked by the probe as deleted
     */
    private boolean groupNotDeleted(CommonDTO.GroupDTO group) {
        return group.getUpdateType() != UpdateType.DELETED;
    }

    /**
     * Parses policy from policyGroups, which are pair of DRS-rule groups or
     * only VM-group which may be engaged with some Cluster by Policy
     * and add parsed policy to list.
     *
     * @param policyGroups Groups for parsing (Pair or just VM-group).
     * @return Created policy if everything was ok and it was created.
     */
    private Optional<DiscoveredPolicyInfo> parsePolicyFromGroups(@Nonnull List<CommonDTO.GroupDTO>
            policyGroups) {

        if (isBuyerBuyerPolicy(policyGroups)) {
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
     * Extract the cluster name from a policy name. For example:
     * <p>"GROUP-DRS-testRule-rule/Physical Hosts_Cluster2/vsphere-dc13.corp.vmturbo.com"
     * <p>Here the name of the cluster is "Physical Hosts_Cluster2".
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
        return nameParts.length > 1
                ? Optional.of(nameParts[1])
                : Optional.empty();
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
        Optional<CommonDTO.GroupDTO> clusterOpt = allGroups.stream()
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
