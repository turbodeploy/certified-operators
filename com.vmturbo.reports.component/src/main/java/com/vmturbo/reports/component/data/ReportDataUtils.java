package com.vmturbo.reports.component.data;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import javaslang.Tuple2;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.reports.component.data.pm.Daily_cluster_30_days_avg_stats_vs_thresholds_grid;
import com.vmturbo.reports.component.data.pm.NullReportTemplate;
import com.vmturbo.reports.component.data.pm.PM_group_daily_pm_vm_utilization_heatmap_grid;
import com.vmturbo.reports.component.data.pm.PM_group_hosting;
import com.vmturbo.reports.component.data.pm.PM_group_profile;
import com.vmturbo.reports.component.data.pm.PM_profile;
import com.vmturbo.reports.component.data.vm.Daily_vm_over_under_prov_grid;
import com.vmturbo.reports.component.data.vm.Daily_vm_over_under_prov_grid_30_days;
import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;
import com.vmturbo.reports.component.data.vm.VM_group_30_days_vm_top_bottom_capacity_grid;
import com.vmturbo.reports.component.data.vm.VM_group_daily_over_under_prov_grid_30_days;
import com.vmturbo.reports.component.data.vm.VM_group_individual_monthly_summary;
import com.vmturbo.reports.component.data.vm.VM_group_profile;
import com.vmturbo.reports.component.data.vm.VM_group_profile_physical_resources;
import com.vmturbo.reports.component.data.vm.VM_group_rightsizing_advice_grid;
import com.vmturbo.reports.component.data.vm.VM_profile;

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
        private Map<Grouping, Tuple2<Long, String>> groupToPK;
        private Tuple2<Long, String> defaultGroupPK;

        public GroupResults(@Nonnull Tuple2<Long, String> defaultVmGroupPK,
                       @Nonnull Map<Grouping, Tuple2<Long, String>> groupToPK) {
            this.groupToPK = groupToPK;
            this.defaultGroupPK = defaultVmGroupPK;
        }

        public Map<Grouping, Tuple2<Long, String>> getGroupToPK() {
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
        private Map<Grouping, Long> groupToPK;
        // default group, e.g. all VM on-premise
        private Long defaultGroupPK;

        public EntitiesTableGeneratedId(@Nonnull final Long defaultVmGroupPK,
                                        @Nonnull final Map<Grouping, Long> groupToPK) {
            this.groupToPK = groupToPK;
            this.defaultGroupPK = defaultVmGroupPK;
        }

        public Map<Grouping, Long> getGroupToPK() {
            return groupToPK;
        }

        public Long getDefaultGroupPK() {
            return defaultGroupPK;
        }
    }

    public enum MetaGroup {
        VMs("VMs", "GROUP-VMsByCluster"),
        PMs("PMs", "GROUP-PMsByCluster"),
        Storages("Storage", "GROUP-STsByCluster"),
        // it has "\\\\" as "groupPrefix" value because two are for escape, and other two are for SQL
        // query: like GROUP-VMsByCluster_hostname\\fake_vm_group%
        FAKE_VM_GROUP("FAKE_VMS", "GROUP-VMsByCluster_hostname\\\\fake_vm_group");

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

    public static Map<Long, ReportTemplate> getReportMap(final long maxConnectRetryCount,
                                                         final long retryDelayInMilliSec) {
        final GroupGeneratorDelegate delegate =
            new GroupGeneratorDelegate(maxConnectRetryCount, retryDelayInMilliSec);
        return ImmutableMap.<Long, ReportTemplate>builder().
            // standard reports
            // daily_infra_30_days_avg_stats_vs_thresholds_grid
                put(140L, new NullReportTemplate(delegate)).
            // require stats in cluster_members table
            //    put(146L, new Monthly_cluster_summary(delegate)).
            //    put(147L, new Monthly_summary(delegate)).
                put(148L, new Daily_vm_over_under_prov_grid(delegate)).
                put(150L, new Daily_vm_rightsizing_advice_grid(delegate)).
            // daily_pm_vm_top_bottom_utilized_grid
                put(155L, new NullReportTemplate(delegate)).
            // daily_pm_vm_utilization_heatmap_grid
                put(156L, new NullReportTemplate(delegate)).
            // daily_pm_top_bottom_capacity_grid
                put(165L, new NullReportTemplate(delegate)).
            // daily_vm_top_bottom_capacity_grid
                put(166L, new NullReportTemplate(delegate)).
            // daily_pm_top_cpu_ready_queue_grid
                put(168L, new NullReportTemplate(delegate)).
            // daily_pm_top_resource_utilization_bar
                put(169L, new NullReportTemplate(delegate)).
                put(170L, new Daily_cluster_30_days_avg_stats_vs_thresholds_grid(delegate)).
            // daily_potential_storage_waste_grid, according to PT it show recently added VM, hide it for now
            //  put(172L, new NullReportTemplate(delegate)).
            // monthly_individual_vm_summary
                put(181L, new NullReportTemplate(delegate)).
            // monthly_30_days_pm_top_bottom_capacity_grid
                put(182L, new NullReportTemplate(delegate)).
            // monthly_30_days_vm_top_bottom_capacity_grid
                put(183L, new NullReportTemplate(delegate)).
                put(184L, new Daily_vm_over_under_prov_grid_30_days(delegate)).
            // weekly_socket_audit_report
                put(189L, new NullReportTemplate(delegate)).

            // on demand report
                put(1L, new PM_group_profile(delegate)).
                put(2L, new PM_profile(delegate)).
                put(5L, new VM_group_profile(delegate)).
                put(6L, new VM_profile(delegate)).
                put(8L, new PM_group_hosting(delegate)).
                put(9L, new VM_group_profile_physical_resources(delegate)).
                put(10L, new VM_group_individual_monthly_summary(delegate)).
                put(11L, new VM_group_daily_over_under_prov_grid_30_days(delegate)).
                put(12L, new VM_group_30_days_vm_top_bottom_capacity_grid(delegate)).
            // vm_group_daily_over_under_prov_grid_given_days
                put(13L, new NullReportTemplate(delegate)).
            // require stats in cluster_members table
            //    put(14L, new PM_group_monthly_individual_cluster_summary(delegate)).
            //    put(15L, new PM_group_pm_top_bottom_capacity_grid_per_cluster(delegate)).
                put(16L, new PM_group_daily_pm_vm_utilization_heatmap_grid(delegate)).
                put(17L, new VM_group_rightsizing_advice_grid(delegate))
            .build();
    }
}
