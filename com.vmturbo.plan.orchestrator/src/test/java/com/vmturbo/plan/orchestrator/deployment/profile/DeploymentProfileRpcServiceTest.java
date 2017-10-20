package com.vmturbo.plan.orchestrator.deployment.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.CreateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeleteDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EditDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfilesRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.DeploymentProfileServiceGrpc.DeploymentProfileServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.DiscoveredNotSupportedOperationException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

public class DeploymentProfileRpcServiceTest {
    private DeploymentProfileDaoImpl deploymentProfileDao =
            Mockito.mock(DeploymentProfileDaoImpl.class);

    private DeploymentProfileRpcService deploymentProfileRpcService =
            new DeploymentProfileRpcService(deploymentProfileDao);

    private DeploymentProfileServiceBlockingStub deploymentProfileServiceBlockingStub;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(deploymentProfileRpcService);

    @Before
    public void init() throws Exception {
        deploymentProfileServiceBlockingStub = DeploymentProfileServiceGrpc.newBlockingStub(
            grpcServer.getChannel()
        );
    }

    @Test
    public void testGetDeploymentProfiles() {
        final GetDeploymentProfilesRequest request = GetDeploymentProfilesRequest.newBuilder().build();
        Set<DeploymentProfile> deploymentProfileSet = Sets.newHashSet(DeploymentProfile.newBuilder()
            .setId(123)
            .setDeployInfo(DeploymentProfileInfo.newBuilder().setName("test"))
            .build());

        Mockito.when(deploymentProfileDao.getAllDeploymentProfiles()).thenReturn(deploymentProfileSet);
        Iterator<DeploymentProfile> result = deploymentProfileServiceBlockingStub.getDeploymentProfiles(request);
        assertTrue(result.hasNext());
        assertEquals(deploymentProfileSet, Sets.newHashSet(result));
    }

    @Test
    public void testGetDeploymentProfile() {
        final GetDeploymentProfileRequest request = GetDeploymentProfileRequest.newBuilder()
            .setDeploymentProfileId(123)
            .build();
        Optional<DeploymentProfile> deploymentProfile = Optional.of(DeploymentProfile.newBuilder()
            .setId(123)
            .setDeployInfo(DeploymentProfileInfo.newBuilder().setName("test"))
            .build());
        Mockito.when(deploymentProfileDao.getDeploymentProfile(123)).thenReturn(deploymentProfile);
        DeploymentProfile result = deploymentProfileServiceBlockingStub.getDeploymentProfile(request);
        assertEquals(result, deploymentProfile.get());
    }

    @Test
    public void createDeploymentProfile() {
        final DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test")
            .build();
        final CreateDeploymentProfileRequest request = CreateDeploymentProfileRequest.newBuilder()
            .setDeployInfo(deploymentProfileInfo)
            .build();
        final DeploymentProfile deploymentProfile = DeploymentProfile.newBuilder()
            .setId(123)
            .setDeployInfo(deploymentProfileInfo)
            .build();
        Mockito.when(deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo))
            .thenReturn(deploymentProfile);
        DeploymentProfile result = deploymentProfileServiceBlockingStub.createDeploymentProfile(request);
        assertEquals(result, deploymentProfile);
    }

    @Test
    public void testEditDeploymentProfile() throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        final DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("old")
            .build();
        final DeploymentProfileInfo newDeploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("new")
            .build();
        final EditDeploymentProfileRequest request = EditDeploymentProfileRequest.newBuilder()
            .setDeploymentProfileId(123)
            .setDeployInfo(deploymentProfileInfo)
            .build();
        final DeploymentProfile newDeploymentProfile = DeploymentProfile.newBuilder()
            .setId(123)
            .setDeployInfo(newDeploymentProfileInfo)
            .build();

        Mockito.when(deploymentProfileDao.editDeploymentProfile(123, deploymentProfileInfo))
            .thenReturn(newDeploymentProfile);
        DeploymentProfile result = deploymentProfileServiceBlockingStub.editDeploymentProfile(request);
        assertEquals(result, newDeploymentProfile);
    }

    @Test
    public void testDeleteDeploymentProfile() throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        final DeleteDeploymentProfileRequest request = DeleteDeploymentProfileRequest.newBuilder()
            .setDeploymentProfileId(123)
            .build();
        final DeploymentProfile deploymentProfile = DeploymentProfile.newBuilder()
            .setId(123)
            .setDeployInfo(DeploymentProfileInfo.newBuilder().setName("test"))
            .build();
        Mockito.when(deploymentProfileDao.deleteDeploymentProfile(123)).thenReturn(deploymentProfile);
        DeploymentProfile result = deploymentProfileServiceBlockingStub.deleteDeploymentProfile(request);
        assertEquals(result, deploymentProfile);
    }
}
