package com.vmturbo.topology.processor.plan;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.conversions.typespecific.DesktopPoolInfoMapper;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * Unit tests for DiscoveredTemplateDeploymentProfileNotifier.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DiscoveredTemplateDeploymentProfileServiceStub.class})
@PowerMockIgnore("javax.management.*")
public class DiscoveredTemplateDeploymentProfileNotifierTest {

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
        final DiscoveredTemplateDeploymentProfileNotifier uploader =
                        Mockito.spy(new DiscoveredTemplateDeploymentProfileUploader(Mockito.mock(EntityStore.class),
                            PowerMockito.mock(DiscoveredTemplateDeploymentProfileServiceStub.class)));
        Mockito.when(uploader.getProfileId(target, vendorId)).thenReturn(id);
        Mockito.doCallRealMethod().when(uploader).patchTopology(Mockito.any());
        uploader.patchTopology(topology);
        Assert.assertEquals(id, pool.getTypeSpecificInfoBuilder().getDesktopPoolBuilder()
                        .getTemplateReferenceId());
    }

}
