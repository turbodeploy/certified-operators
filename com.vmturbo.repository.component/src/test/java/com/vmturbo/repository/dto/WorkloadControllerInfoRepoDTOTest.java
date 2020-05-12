package com.vmturbo.repository.dto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

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
 * Class to test WorkloadControllerInfoRepoDTO conversion to and from TypeSpecificInfo.
 */
public class WorkloadControllerInfoRepoDTOTest {

    /**
     * Test filling a RepoDTO from a TypeSpecificInfo with data fields populated.
     */
    @Test
    public void testFillFromTypeSpecificInfo() {
        final ServiceEntityRepoDTO repoDTO = new ServiceEntityRepoDTO();
        final WorkloadControllerInfo.Builder builder = WorkloadControllerInfo.newBuilder();
        final TypeSpecificInfo.Builder tsiBuilder = TypeSpecificInfo.newBuilder();
        final WorkloadControllerInfoRepoDTO wcInfoDto = new WorkloadControllerInfoRepoDTO();

        builder.setCronJobInfo(CronJobInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.CRON_JOB_INFO, wcInfoDto.getControllerType());
        assertNull(wcInfoDto.getCustomControllerType());

        builder.setCustomControllerInfo(CustomControllerInfo.newBuilder()
            .setCustomControllerType("foo"));
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.CUSTOM_CONTROLLER_INFO, wcInfoDto.getControllerType());
        assertEquals("foo", wcInfoDto.getCustomControllerType());

        builder.setDaemonSetInfo(DaemonSetInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.DAEMON_SET_INFO, wcInfoDto.getControllerType());
        assertNull(wcInfoDto.getCustomControllerType());

        builder.setDeploymentInfo(DeploymentInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.DEPLOYMENT_INFO, wcInfoDto.getControllerType());

        builder.setJobInfo(JobInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.JOB_INFO, wcInfoDto.getControllerType());

        builder.setReplicaSetInfo(ReplicaSetInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.REPLICA_SET_INFO, wcInfoDto.getControllerType());

        builder.setReplicationControllerInfo(ReplicationControllerInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.REPLICATION_CONTROLLER_INFO, wcInfoDto.getControllerType());

        builder.setStatefulSetInfo(StatefulSetInfo.getDefaultInstance());
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.STATEFUL_SET_INFO, wcInfoDto.getControllerType());
    }

    /**
     * Test filling a RepoDTO from an empty TypeSpecificInfo.
     */
    @Test
    public void testFillFromEmptyTypeSpecificInfo() {
        // arrange
        TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .build();
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final WorkloadControllerInfoRepoDTO testBusinessAccountRepoDTO =
            new WorkloadControllerInfoRepoDTO();
        // act
        testBusinessAccountRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertNull(testBusinessAccountRepoDTO.getControllerType());
        assertNull(testBusinessAccountRepoDTO.getCustomControllerType());
    }

    /**
     * Test extracting a TypeSpecificInfo from a RepoDTO.
     */
    @Test
    public void testCreateFromRepoDTO() {
        final ServiceEntityRepoDTO repoDTO = new ServiceEntityRepoDTO();
        final WorkloadControllerInfo.Builder builder = WorkloadControllerInfo.newBuilder();
        final TypeSpecificInfo.Builder tsiBuilder = TypeSpecificInfo.newBuilder();
        final WorkloadControllerInfoRepoDTO wcInfoDto = new WorkloadControllerInfoRepoDTO();

        builder.setCustomControllerInfo(CustomControllerInfo.newBuilder()
            .setCustomControllerType("foo"));
        wcInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setWorkloadController(builder).build(), repoDTO);
        assertEquals(ControllerTypeCase.CUSTOM_CONTROLLER_INFO, wcInfoDto.getControllerType());
        assertEquals("foo", wcInfoDto.getCustomControllerType());

        assertEquals(ControllerTypeCase.CUSTOM_CONTROLLER_INFO,
            wcInfoDto.createTypeSpecificInfo().getWorkloadController().getControllerTypeCase());
        assertEquals("foo",
            wcInfoDto.createTypeSpecificInfo().getWorkloadController()
                .getCustomControllerInfo().getCustomControllerType());
    }

    /**
     * Test extracting a TypeSpecificInfo from an empty RepoDTO.
     */
    @Test
    public void testCreateFromEmptyRepoDTO() {
        // arrange
        final WorkloadControllerInfoRepoDTO repoDto =
            new WorkloadControllerInfoRepoDTO();
        WorkloadControllerInfo expected = WorkloadControllerInfo.newBuilder()
            .build();
        // act
        TypeSpecificInfo result = repoDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasWorkloadController());
        final WorkloadControllerInfo workloadControllerInfo = result.getWorkloadController();
        assertEquals(expected, workloadControllerInfo);
    }
}
