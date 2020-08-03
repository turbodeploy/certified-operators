package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateTargetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOMoles.DiscoveredTemplateDeploymentProfileServiceMole;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.conversions.typespecific.DesktopPoolInfoMapper;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit tests for DiscoveredTemplateDeploymentProfileNotifier.
 */
public class DiscoveredTemplateDeploymentProfileUploaderTest {

    private EntityStore entityStore = mock(EntityStore.class);

    private TargetStore targetStore = mock(TargetStore.class);

    private DiscoveredTemplateDeploymentProfileServiceMole backend = spy(DiscoveredTemplateDeploymentProfileServiceMole.class);

    /**
     * Emulates gRPC dependencies.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(backend);

    private DiscoveredTemplateDeploymentProfileUploader uploader;

    @Captor
    private ArgumentCaptor<List<UpdateTargetDiscoveredTemplateDeploymentProfileRequest>> uploadCaptor;

    /**
     * Common setup code before every test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        uploader = spy(new DiscoveredTemplateDeploymentProfileUploader(entityStore,
            targetStore,
            DiscoveredTemplateDeploymentProfileServiceGrpc.newStub(testServer.getChannel()),
            1, TimeUnit.SECONDS));
    }

    /**
     * Test the delayed stitching of template identifiers (referenced by desktop pool).
     */
    @Test
    public void testPatchTopology() {
        final TopologyEntityDTO.Builder pool = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                        .setOid(7L);
        String vendorId = "qqq";
        long id = 2131L;
        long target = 58674L;
        TopologyEntity.Builder builder = topologyEntityBuilder(pool);
        builder.getEntityBuilder()
                        .putEntityPropertyMap(DesktopPoolInfoMapper.DESKTOP_POOL_TEMPLATE_REFERENCE,
                                              vendorId);
        builder.getEntityBuilder().getOriginBuilder().getDiscoveryOriginBuilder()
                        .putDiscoveredTargetData(target,
                                                 PerTargetEntityInformation.getDefaultInstance());
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, builder);

        when(uploader.getProfileId(target, vendorId)).thenReturn(id);
        Mockito.doCallRealMethod().when(uploader).patchTopology(Mockito.any());
        uploader.patchTopology(topology);
        Assert.assertEquals(id, pool.getTypeSpecificInfoBuilder().getDesktopPoolBuilder()
                        .getTemplateReferenceId());
    }

    /**
     * Test that targets that haven't set their template discovery results yet get uploaded
     * with the "data_available" flag set to false.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUploadNoDataAvailable() throws Exception {
        final long targetId = 777;
        Target mockTarget = mock(Target.class);
        when(mockTarget.getId()).thenReturn(targetId);
        when(targetStore.getAll()).thenReturn(Collections.singletonList(mockTarget));
        when(targetStore.getProbeTypeForTarget(targetId)).thenReturn(Optional.of(SDKProbeType.VCENTER));
        uploader.sendTemplateDeploymentProfileData();

        verify(backend).updateDiscoveredTemplateDeploymentProfile(uploadCaptor.capture());
        List<UpdateTargetDiscoveredTemplateDeploymentProfileRequest> upload = uploadCaptor.getValue();
        assertThat(upload, containsInAnyOrder(UpdateTargetDiscoveredTemplateDeploymentProfileRequest.newBuilder()
            .setTargetId(targetId)
            .setDataAvailable(false)
            .build()));
    }
}
