package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * Shared constants for testing the package.
 */
class DiscoveredGroupConstants {

    static final long TARGET_ID = 1L;

    public static final String GROUP_NAME = "group";

    public static final String DISPLAY_NAME = "Freedom is slavery.";

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

    static final long PLACEHOLDER_GROUP_MEMBER = 10L;
    static final long PLACEHOLDER_CLUSTER_MEMBER = 11L;

    static final GroupInfo PLACEHOLDER_GROUP_INFO = GroupInfo.newBuilder()
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(PLACEHOLDER_GROUP_MEMBER))
            .build();

    static final ClusterInfo PLACEHOLDER_CLUSTER_INFO = ClusterInfo.newBuilder()
            .setClusterType(Type.COMPUTE)
            .setName("cluster")
            .setMembers(StaticGroupMembers.newBuilder()
                .addStaticMemberOids(PLACEHOLDER_CLUSTER_MEMBER))
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
            .setGroupName(GROUP_NAME)
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
            .setGroupName(GROUP_NAME)
            .setMemberList(MembersList.newBuilder()
                    .addMember("1"))
            .build();

    static final InterpretedGroup PLACEHOLDER_INTERPRETED_GROUP =
            new InterpretedGroup(STATIC_MEMBER_DTO,
                    Optional.of(PLACEHOLDER_GROUP_INFO.toBuilder()), Optional.empty());

    static final InterpretedGroup PLACEHOLDER_INTERPRETED_CLUSTER =
            new InterpretedGroup(STATIC_MEMBER_DTO, Optional.empty(),
                    Optional.of(PLACEHOLDER_CLUSTER_INFO.toBuilder()));


    // create a DATACENTER to test the cluster name prefix addition
    static final String DC_NAME = "DC1";
    static final TopologyEntityDTO.Builder DATACENTER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.DATACENTER_VALUE)
            .setDisplayName(DC_NAME)
            .setOid(100L);

    // create an host inside that DATACENTER
    static final CommoditiesBoughtFromProvider COMM_BOUGHT_BY_PM_FROM_DC =
            CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(DATACENTER.getOid())
                    .setProviderEntityType(DATACENTER.getEntityType())
                    .addCommodityBought(
                            CommodityBoughtDTO.newBuilder()
                                    .setCommodityType(
                                            CommodityType.newBuilder()
                                                    .setType(CommodityDTO.CommodityType.SPACE_VALUE)
                                                    .build()
                                    )
                    )
                    .build();

    static final String PM_NAME = "PM1";
    static final TopologyEntityDTO.Builder HOST_IN_DATACENTER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setDisplayName(PM_NAME)
            .setOid(101L)
            .addCommoditiesBoughtFromProviders(COMM_BOUGHT_BY_PM_FROM_DC);

    // create a storage
    static final String ST_NAME = "ST1";
    static final TopologyEntityDTO.Builder storage = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setDisplayName(ST_NAME)
            .setOid(102L);

    // create a topology for those entities
    static final Map<Long, Builder> TOPOLOGY = ImmutableMap.of(
            DATACENTER.getOid(), topologyEntityBuilder(DATACENTER),
            HOST_IN_DATACENTER.getOid(), topologyEntityBuilder(HOST_IN_DATACENTER),
            storage.getOid(), topologyEntityBuilder(storage)
    );

    // compute cluster containing the host
    static final String COMPUTE_CLUSTER_NAME = "compute-cluster";
    static final ClusterInfo COMPUTE_CLUSTER_INFO = ClusterInfo.newBuilder()
            .setClusterType(Type.COMPUTE)
            .setName(COMPUTE_CLUSTER_NAME)
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(HOST_IN_DATACENTER.getOid()))
            .build();

    // storage  cluster containing the storage
    static final String STORAGE_CLUSTER_NAME = "storage-cluster";
    static final ClusterInfo STORAGE_CLUSTER_INFO = ClusterInfo.newBuilder()
            .setClusterType(Type.STORAGE)
            .setName(STORAGE_CLUSTER_NAME)
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(storage.getOid()))
            .build();

    // in those interpreted clusters, I don't really care that the STATIC_MEMBER_DTO is not 100%
    // matching, because in the tests I am going to use the clusterInfo directly.
    static final InterpretedGroup COMPUTE_INTERPRETED_CLUSTER =
            new InterpretedGroup(STATIC_MEMBER_DTO, Optional.empty(),
                    Optional.of(COMPUTE_CLUSTER_INFO.toBuilder()));

    static final InterpretedGroup STORAGE_INTERPRETED_CLUSTER =
            new InterpretedGroup(STATIC_MEMBER_DTO, Optional.empty(),
                    Optional.of(STORAGE_CLUSTER_INFO.toBuilder()));


    private DiscoveredGroupConstants() {}
}
