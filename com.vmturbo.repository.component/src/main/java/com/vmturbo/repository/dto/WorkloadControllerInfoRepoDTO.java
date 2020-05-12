package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo.ControllerTypeCase;

/**
 * Class that encapsulates the workload controller data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class WorkloadControllerInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private WorkloadControllerInfo.ControllerTypeCase controllerType;

    /**
     * Only set when the controllerType is CUSTOM_CONTROLLER_INFO. Null otherwise.
     */
    private String customControllerType;

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasWorkloadController()) {
            return;
        }
        final WorkloadControllerInfo workloadControllerInfo = typeSpecificInfo.getWorkloadController();
        this.controllerType = workloadControllerInfo.getControllerTypeCase();
        if (workloadControllerInfo.getControllerTypeCase() == ControllerTypeCase.CUSTOM_CONTROLLER_INFO) {
            this.customControllerType = workloadControllerInfo.getCustomControllerInfo()
                .getCustomControllerType();
        } else {
            this.customControllerType = null;
        }

        serviceEntityRepoDTO.setWorkloadControllerInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final WorkloadControllerInfo.Builder builder = WorkloadControllerInfo.newBuilder();
        if (controllerType != null) {
            switch (controllerType) {
                case CRON_JOB_INFO:
                    builder.setCronJobInfo(CronJobInfo.getDefaultInstance());
                    break;
                case CUSTOM_CONTROLLER_INFO:
                    builder.setCustomControllerInfo(CustomControllerInfo.newBuilder()
                        .setCustomControllerType(customControllerType));
                    break;
                case DAEMON_SET_INFO:
                    builder.setDaemonSetInfo(DaemonSetInfo.getDefaultInstance());
                    break;
                case DEPLOYMENT_INFO:
                    builder.setDeploymentInfo(DeploymentInfo.getDefaultInstance());
                    break;
                case JOB_INFO:
                    builder.setJobInfo(JobInfo.getDefaultInstance());
                    break;
                case REPLICA_SET_INFO:
                    builder.setReplicaSetInfo(ReplicaSetInfo.getDefaultInstance());
                    break;
                case REPLICATION_CONTROLLER_INFO:
                    builder.setReplicationControllerInfo(ReplicationControllerInfo.getDefaultInstance());
                    break;
                case STATEFUL_SET_INFO:
                    builder.setStatefulSetInfo(StatefulSetInfo.getDefaultInstance());
                    break;
                default:
                    logger.warn("createTypeSpecificInfo: controller type not set.");
                    break;
            }
        }

        return TypeSpecificInfo.newBuilder().setWorkloadController(builder).build();
    }

    /**
     * Get the controller type for this workload controller.
     *
     * @return the controller type for this workload controller. Returns NULL if not set.
     */
    @Nullable
    public ControllerTypeCase getControllerType() {
        return controllerType;
    }

    /**
     * Get the custom controller type for this workload controller.
     *
     * @return the custom controller type for this workload controller.
     *         Returns NULL if the controller type is not a Custom Controller.
     */
    @Nullable
    public String getCustomControllerType() {
        return customControllerType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WorkloadControllerInfoRepoDTO that = (WorkloadControllerInfoRepoDTO)o;
        return Objects.equals(controllerType, that.controllerType)
            && Objects.equals(customControllerType, that.customControllerType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(controllerType, customControllerType);
    }

    @Override
    public String toString() {
        return WorkloadControllerInfoRepoDTO.class.getSimpleName()
            + '{' + "controllerType=" + controllerType
            + ", customControllerType=" + customControllerType + '}';
    }
}
