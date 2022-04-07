package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CronJobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CustomControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DaemonSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DeploymentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.JobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ReplicaSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ReplicationControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StatefulSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.WorkloadControllerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Workload Controller - i.e. {@link WorkloadControllerInfo}.
 **/
public class WorkloadControllerInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo
    mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                   @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasWorkloadControllerData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        WorkloadControllerData wcData = sdkEntity.getWorkloadControllerData();
        WorkloadControllerInfo.Builder infoBuilder = WorkloadControllerInfo.newBuilder();
        switch (wcData.getControllerTypeCase()) {
            case CRON_JOB_DATA:
                infoBuilder.setCronJobInfo(CronJobInfo.newBuilder());
                break;
            case CUSTOM_CONTROLLER_DATA:
                infoBuilder.setCustomControllerInfo(CustomControllerInfo.newBuilder()
                    .setCustomControllerType(wcData.getCustomControllerData().getCustomControllerType()));
                break;
            case DAEMON_SET_DATA:
                infoBuilder.setDaemonSetInfo(DaemonSetInfo.newBuilder());
                break;
            case DEPLOYMENT_DATA:
                infoBuilder.setDeploymentInfo(DeploymentInfo.newBuilder());
                break;
            case JOB_DATA:
                infoBuilder.setJobInfo(JobInfo.newBuilder());
                break;
            case REPLICA_SET_DATA:
                infoBuilder.setReplicaSetInfo(ReplicaSetInfo.newBuilder());
                break;
            case REPLICATION_CONTROLLER_DATA:
                infoBuilder.setReplicationControllerInfo(ReplicationControllerInfo.newBuilder());
                break;
            case STATEFUL_SET_DATA:
                infoBuilder.setStatefulSetInfo(StatefulSetInfo.newBuilder());
                break;
            default:
                logger.error("Unknown ControllerTypeCase: {}", wcData.getControllerTypeCase());
                return TypeSpecificInfo.getDefaultInstance();
        }

        return TypeSpecificInfo.newBuilder().setWorkloadController(infoBuilder)
                .build();
    }

}
