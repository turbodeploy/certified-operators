package com.vmturbo.topology.processor.group.discovery;

import java.util.Optional;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;

/**
 * Shared constants for testing the package.
 */
class DiscoveredGroupConstants {

    static final long TARGET_ID = 1L;

    static final String DISPLAY_NAME = "Freedom is slavery.";

    static final String PLACEHOLDER_PROP_NAME = "prop";

    static final PropertyFilter PLACEHOLDER_FILTER = PropertyFilter.newBuilder()
            .setPropertyName(PLACEHOLDER_PROP_NAME)
            .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("test"))
            .build();

    static final CommonDTO.GroupDTO CLUSTER_DTO = CommonDTO.GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                    .setConstraintType(ConstraintType.CLUSTER)
                    .setConstraintId("constraint")
                    .setConstraintName("name"))
            .setMemberList(MembersList.newBuilder()
                    .addMember("1").build())
            .build();

    static final GroupInfo PLACEHOLDER_GROUP_INFO = GroupInfo.newBuilder()
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(10L))
            .build();

    static final ClusterInfo PLACEHOLDER_CLUSTER_INFO = ClusterInfo.newBuilder()
            .setClusterType(Type.COMPUTE)
            .setName("cluster")
            .build();

    static final DiscoveredSettingPolicyInfo DISCOVERED_SETTING_POLICY_INFO = DiscoveredSettingPolicyInfo.newBuilder()
        .addDiscoveredGroupNames(CLUSTER_DTO.getGroupName())
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setName("discovered-setting-policy")
        .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.CpuUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(55.0f))
        ).build();

    static final CommonDTO.GroupDTO SELECTION_DTO = CommonDTO.GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("group")
            .setSelectionSpecList(SelectionSpecList.newBuilder()
                    .addSelectionSpec(SelectionSpec.newBuilder()
                            .setProperty("prop1")
                            .setExpressionType(ExpressionType.EQUAL_TO)
                            .setPropertyValueDouble(10.0))
                    .addSelectionSpec(SelectionSpec.newBuilder()
                            .setProperty("prop2")
                            .setExpressionType(ExpressionType.EQUAL_TO)
                            .setPropertyValueDouble(10.0)))
            .build();

    static final CommonDTO.GroupDTO STATIC_MEMBER_DTO = CommonDTO.GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("group")
            .setMemberList(MembersList.newBuilder()
                    .addMember("1"))
            .build();

    static final InterpretedGroup PLACEHOLDER_INTERPRETED_GROUP =
            new InterpretedGroup(STATIC_MEMBER_DTO,
                    Optional.of(PLACEHOLDER_GROUP_INFO), Optional.empty());

    static final InterpretedGroup PLACEHOLDER_INTERPRETED_CLUSTER =
            new InterpretedGroup(STATIC_MEMBER_DTO, Optional.empty(),
                    Optional.of(PLACEHOLDER_CLUSTER_INFO));
}
