package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo.ControllerTypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CronJobData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CustomControllerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DaemonSetData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DeploymentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.JobData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReplicaSetData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReplicationControllerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StatefulSetData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.WorkloadControllerData;

/**
 * Test {@link WorkloadControllerInfoMapper}.
 */
public class WorkloadControllerInfoMapperTest {
    private final WorkloadControllerData.Builder data = WorkloadControllerData.newBuilder();
    private final EntityDTO.Builder entity = EntityDTO.newBuilder()
        .setEntityType(EntityType.WORKLOAD_CONTROLLER)
        .setId("foo");
    private final WorkloadControllerInfoMapper mapper = new WorkloadControllerInfoMapper();

    /**
     * testCronJob.
     */
    @Test
    public void testCronJob() {
        entity.setWorkloadControllerData(
            data.setCronJobData(CronJobData.getDefaultInstance()));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ControllerTypeCase.CRON_JOB_INFO, info.getWorkloadController().getControllerTypeCase());
    }

    /**
     * testCustomController.
     */
    @Test
    public void testCustomController() {
        entity.setWorkloadControllerData(
            data.setCustomControllerData(CustomControllerData.newBuilder()
                .setCustomControllerType("custom-type")));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ControllerTypeCase.CUSTOM_CONTROLLER_INFO, info.getWorkloadController().getControllerTypeCase());
    }

    /**
     * testCronJob.
     */
    @Test
    public void testDaemonSet() {
        entity.setWorkloadControllerData(
            data.setDaemonSetData(DaemonSetData.getDefaultInstance()));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ControllerTypeCase.DAEMON_SET_INFO, info.getWorkloadController().getControllerTypeCase());
    }

    /**
     * testDeployment.
     */
    @Test
    public void testDeployment() {
        entity.setWorkloadControllerData(
            data.setDeploymentData(DeploymentData.getDefaultInstance()));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ControllerTypeCase.DEPLOYMENT_INFO, info.getWorkloadController().getControllerTypeCase());
    }

    /**
     * testJob.
     */
    @Test
    public void testJob() {
        entity.setWorkloadControllerData(
            data.setJobData(JobData.getDefaultInstance()));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ControllerTypeCase.JOB_INFO, info.getWorkloadController().getControllerTypeCase());
    }

    /**
     * testReplicaSet.
     */
    @Test
    public void testReplicaSet() {
        data.setReplicaSetData(ReplicaSetData.getDefaultInstance());
    }

    /**
     * testReplicationController.
     */
    @Test
    public void testReplicationController() {
        data.setReplicationControllerData(ReplicationControllerData.getDefaultInstance());
    }

    /**
     * testStatefulSet.
     */
    @Test
    public void testStatefulSet() {
        data.setStatefulSetData(StatefulSetData.getDefaultInstance());
    }
}