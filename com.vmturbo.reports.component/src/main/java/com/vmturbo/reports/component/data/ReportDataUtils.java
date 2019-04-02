package com.vmturbo.reports.component.data;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import javaslang.Tuple2;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Utility class for report data generation.
 * Most methods in this class are copied from API component.
 * TODO: consider extract the common codes to com.vmturbo.components.common project.
 */
public class ReportDataUtils {


    @Nonnull
    public static Set<Long> expandOids(@Nonnull final Set<Long> oidSet,
                                @Nonnull GroupServiceBlockingStub groupServiceGrpc) {
        Set<Long> answer = Sets.newHashSet();

        boolean isEntity = false;
        for (final Long oid : oidSet) {
            // Assume it's group type at the beginning
            // For subsequent items, if it's group type, we continue the RPC call to Group component.
            // If not, we know it's entity type, and will just add oids to return set.
            if (!isEntity) {
                // If we know it's group, we can further optimize the codes to do multi olds RPC call.
                GetMembersRequest getGroupMembersReq = GetMembersRequest.newBuilder()
                    .setId(oid)
                    .setExpectPresent(false)
                    .build();
                GetMembersResponse groupMembersResp = groupServiceGrpc.getMembers(getGroupMembersReq);

                if (groupMembersResp.hasMembers()) {
                    answer.addAll(groupMembersResp.getMembers().getIdsList());
                } else {
                    answer.add(oid);
                    isEntity = true;
                }
            } else {
                answer.add(oid);
            }
        }
        return answer;
    }
    /**
     * Convert from {@link ActionSpec} to {@link RightSizingInfo}
     *
     * @param actionSpec aciton specification
     * @return right size info
     */
    static RightSizingInfo getRightSizingInfo(final ActionSpec actionSpec) {
        Resize resize = actionSpec.getRecommendation().getInfo().getResize();
        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
            resize.getCommodityType().getType());
        final RightSizingInfo rightSizingInfo = new RightSizingInfo();
        // TODO populate saving when available
        rightSizingInfo.setSavings("0");
        rightSizingInfo.setTargetName("dummy name");
        rightSizingInfo.setExplanation(getResizeExplanation(actionSpec));
        rightSizingInfo.setCloudAction(false);
        rightSizingInfo.setFrom(formatResizeActionCommodityValue(commodityType, resize.getOldCapacity()));
        rightSizingInfo.setTo(formatResizeActionCommodityValue(commodityType, resize.getNewCapacity()));
        return rightSizingInfo;
    }

    private static String getResizeExplanation(final ActionSpec action) {
        final Resize resize = action.getRecommendation().getInfo().getResize();
        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
            resize.getCommodityType().getType());
        final String msg = MessageFormat.format("Resize {0} on the entity from {1} to {2}",
            readableCommodityTypes(Collections.singletonList(resize.getCommodityType())),
            formatResizeActionCommodityValue(commodityType, resize.getOldCapacity()),
            formatResizeActionCommodityValue(commodityType, resize.getNewCapacity()));
        return msg;
    }

    private static String readableCommodityTypes(@Nonnull final List<CommodityType> commodityTypes) {
        return commodityTypes.stream()
            .map(commodityType -> ActionDTOUtil.getCommodityDisplayName(commodityType))
            .collect(Collectors.joining(", "));
    }

    /**
     * Format resize actions commodity capacity value to more readable format. If it is vMem commodity,
     * format it from default KB to GB unit. Otherwise, it will keep its original format.
     *
     * @param commodityType commodity type.
     * @param capacity      commodity capacity which needs to format.
     * @return a string after format.
     */
    private static String formatResizeActionCommodityValue(@Nonnull final CommodityDTO.CommodityType commodityType,
                                                           final double capacity) {
        if (commodityType.equals(CommodityDTO.CommodityType.VMEM)) {
            // if it is vMem commodity, it needs to convert to GB units. And its default capacity unit is KB.
            return MessageFormat.format("{0} GB", capacity / (Units.GBYTE / Units.KBYTE));
        } else {
            return MessageFormat.format("{0}", capacity);
        }
    }

    // helper class to serialize RightSize action to Json
    static class RightSizingInfo {
        @JsonProperty("Savings")
        private String savings;
        @JsonProperty("Target name")
        private String targetName;
        @JsonProperty("Explanation")
        private String explanation;
        @JsonProperty("CloudAction")
        private Boolean cloudAction;
        @JsonProperty("From")
        private String from;
        @JsonProperty("To")
        private String to;

        public String getTargetName() {
            return targetName;
        }

        public void setTargetName(final String targetName) {
            this.targetName = targetName;
        }

        public String getSavings() {
            return savings;
        }

        public void setSavings(final String savings) {
            this.savings = savings;
        }

        public String getExplanation() {
            return explanation;
        }

        public void setExplanation(final String explanation) {
            this.explanation = explanation;
        }

        public Boolean getCloudAction() {
            return cloudAction;
        }

        public void setCloudAction(final Boolean cloudAction) {
            this.cloudAction = cloudAction;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(final String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(final String to) {
            this.to = to;
        }
    }

    // wrapper class to return default VM group (PK, UUID) and VM groups' (PK, UUID)
    static class GroupResults {
        private Map<Group, Tuple2<Long, String>> groupToPK;
        private Tuple2<Long, String> defaultGroupPK;

        public GroupResults(@Nonnull Tuple2<Long, String> defaultVmGroupPK,
                       @Nonnull Map<Group, Tuple2<Long, String>> groupToPK) {
            this.groupToPK = groupToPK;
            this.defaultGroupPK = defaultVmGroupPK;
        }

        public Map<Group, Tuple2<Long, String>> getGroupToPK() {
            return groupToPK;
        }

        public Tuple2<Long, String> getDefaultGroupPK() {
            return defaultGroupPK;
        }
    }

    /**
     * Store auto incremented ids for newly generated groups in entities table.
     */
    static class EntitiesTableGeneratedId {
        // Group -> new generated group id in entities table.
        private Map<Group, Long> groupToPK;
        // default group, e.g. all VM on-premise
        private Long defaultGroupPK;

        public EntitiesTableGeneratedId(@Nonnull final Long defaultVmGroupPK,
                                        @Nonnull final Map<Group, Long> groupToPK) {
            this.groupToPK = groupToPK;
            this.defaultGroupPK = defaultVmGroupPK;
        }

        public Map<Group, Long> getGroupToPK() {
            return groupToPK;
        }

        public Long getDefaultGroupPK() {
            return defaultGroupPK;
        }
    }

    public enum MetaGroup {
        VMs("VMs", "GROUP-VMsByCluster"),
        PMs("PMs", "GROUP-PMsByCluster"),
        Storages("Storage", "GROUP-STsByCluster");

        private final String groupName;
        private final String groupPrefix;
        MetaGroup(@Nonnull final String groupName,
                  @Nonnull final String groupPrefix) {
            this.groupName = groupName;
            this.groupPrefix = groupPrefix;
        }

        public String getGroupName() {
            return groupName;
        }

        public String getGroupPrefix() {
            return groupPrefix;
        }
    }
}
