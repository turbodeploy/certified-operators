package com.vmturbo.reports.component.data;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Utility class for report data generation.
 * Most methods in this class are copied from API component.
 * TODO: consider extract the common codes to com.vmturbo.components.common project.
 */
public class ReportDataUtils {

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

    // wrapper class to return default VM group PK and VM groups' PK
    static class Results {
        private Map<Group, Long> groupToPK;
        private Long defaultGroupPK;

        public Results(@Nonnull Long defaultVmGroupPK,
                       @Nonnull Map<Group, Long> groupToPK) {
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
