package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.math.BigDecimal;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class provides constants for other tests.
 * Create
 *   historical data indices: multiple hours and days
 *   accounts: two master accounts with different number of subaccounts
 *   locations: two regions with different number of availability zones
 *   compute tiers: two families with different number of instance types
 *
 */
public class ReservedInstanceAnalyzerConstantsTest {

    private ReservedInstanceAnalyzerConstantsTest() {}

    // Byte hour, Byte day, Long accountId, Long computeTierId, Long zoneId, Byte platform, Byte tenancy, BigDecimal allocation, BigDecimal consumption)
    // hour
    static final Byte HOUR1 = new Byte((byte)1);
    static final Byte HOUR2 = new Byte((byte)2);
    static final Byte HOUR3 = new Byte((byte)3);
    // day
    static final Byte DAY1 = new Byte((byte)1);
    static final Byte DAY4 = new Byte((byte)4);

    // billing family 1
    static final long MASTER_ACCOUNT_1_OID = 300L;
    static final long BUSINESS_ACCOUNT_11_OID = 301L;
    static final long BUSINESS_ACCOUNT_12_OID = 302L;
    // billing family 2
    static final long MASTER_ACCOUNT_2_OID = 310L;
    static final long BUSINESS_ACCOUNT_21_OID = 311L;

    static final long AWS_TARGET_ID = 77777L;
    static final Origin AWS_ORIGIN = Origin.newBuilder()
        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
            .addDiscoveringTargetIds(AWS_TARGET_ID))
        .build();

    /*
     * location
     */
    static final long REGION_AWS_OHIO_OID = 500L;
    static final long ZONE_AWS_OHIO_1_OID = 501L;
    static final long ZONE_AWS_OHIO_2_OID = 502L;
    static final long REGION_AWS_OREGON_OID = 5100L;
    static final long ZONE_AWS_OREGON_1_OID = 511L;
    static final long ZONE_AWS_OREGON_2_OID = 512L;

    static final TopologyEntityDTO ZONE_AWS_OHIO_1 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OHIO_1_OID)
        .setDisplayName("aws_ohio_zone_1")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    static final TopologyEntityDTO ZONE_AWS_OHIO_2 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OHIO_2_OID)
        .setDisplayName("aws_ohio_zone_2")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    static final TopologyEntityDTO REGION_AWS_OHIO = TopologyEntityDTO.newBuilder()
        .setOid(REGION_AWS_OHIO_OID)
        .setDisplayName("aws-ohio")
        .setEntityType(EntityType.REGION_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OHIO_1.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OHIO_1_OID)
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OHIO_2.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OHIO_2_OID)
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    static final TopologyEntityDTO ZONE_AWS_OREGON_1 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OREGON_1_OID)
        .setDisplayName("aws-oregon_zone_1")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    static final TopologyEntityDTO ZONE_AWS_OREGON_2 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OREGON_2_OID)
        .setDisplayName("aws-oregon_zone_2")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    static final TopologyEntityDTO REGION_AWS_OREGON = TopologyEntityDTO.newBuilder()
        .setOid(REGION_AWS_OREGON_OID)
        .setDisplayName("aws-oregon")
        .setEntityType(EntityType.REGION_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OREGON_1.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OREGON_1.getOid())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OREGON_2.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OREGON_2.getOid())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    /*
     * compute tiers
     */
    static final long COMPUTE_TIER_M5_LARGE_OID = 404;
    static final String COMPUTE_TIER_M5_FAMILY = "m5";
    static final long COMPUTE_TIER_T2_NANO_OID = 410;
    static final long COMPUTE_TIER_T2_MICRO_OID = 411;
    static final String COMPUTE_TIER_T2_FAMILY = "t2";

    static final ComputeTierInfo t2ComputeTierInfo = ComputeTierInfo.newBuilder().setFamily(COMPUTE_TIER_T2_FAMILY).build();
    static final TypeSpecificInfo t2TypeSpecificInfo = TypeSpecificInfo.newBuilder().setComputeTier(t2ComputeTierInfo).build();

    static final ComputeTierInfo m5ComputeTierInfo = ComputeTierInfo.newBuilder().setFamily(COMPUTE_TIER_M5_FAMILY).build();
    static final TypeSpecificInfo m5TypeSpecificInfo = TypeSpecificInfo.newBuilder().setComputeTier(m5ComputeTierInfo).build();
    static final TopologyEntityDTO COMPUTE_TIER_M5_LARGE = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_M5_LARGE_OID)
        .setDisplayName("m5.large")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .setTypeSpecificInfo(m5TypeSpecificInfo)
        .build();

    static final TopologyEntityDTO COMPUTE_TIER_T2_NANO = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_T2_NANO_OID)
        .setDisplayName("t2.nano")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OREGON.getEntityType())
            .setConnectedEntityId(REGION_AWS_OREGON.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .setTypeSpecificInfo(t2TypeSpecificInfo)
        .build();

    static final TopologyEntityDTO COMPUTE_TIER_T2_MICRO = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_T2_MICRO_OID)
        .setDisplayName("t2.micro")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OREGON.getEntityType())
            .setConnectedEntityId(REGION_AWS_OREGON.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .setTypeSpecificInfo(t2TypeSpecificInfo)
        .build();
    static final BigDecimal VALUE_ALLOCATION = new BigDecimal(10);
    static final BigDecimal VALUE_CONSUMPTION = new BigDecimal(20);


}
