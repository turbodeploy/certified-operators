package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.cloud.common.commitment.TopologyEntityCommitmentTopology.TopologyEntityCommitmentTopologyFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link TopologyEntityCommitmentTopology}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyEntityCommitmentTopologyTest {

    @Mock
    private CloudTopology<TopologyEntityDTO> cloudTopology;

    private TopologyEntityCommitmentTopologyFactory commitmentTopologyFactory = new TopologyEntityCommitmentTopologyFactory();

    private CloudCommitmentTopology commitmentTopology;

    /**
     *  Set up.
     */
    @Before
    public void setup() {
        commitmentTopology = commitmentTopologyFactory.createTopology(cloudTopology);
    }

    /**
     * Test for {@link CloudCommitmentTopology#getSupportedCoverageVectors(long)}.
     */
    @Test
    public void testGetSupportedCoverageVectors() {

        final TopologyEntityDTO targetEntity = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();

        final TopologyEntityDTO gcpServiceProvider = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .setDisplayName("gcp")
                .build();

        // set up cloud topology
        when(cloudTopology.getEntity(targetEntity.getOid())).thenReturn(Optional.of(targetEntity));
        when(cloudTopology.getServiceProvider(targetEntity.getOid())).thenReturn(Optional.of(gcpServiceProvider));

        // invoke commitment topology
        final Set<CloudCommitmentCoverageTypeInfo> actualCoverageVectors = commitmentTopology.getSupportedCoverageVectors(targetEntity.getOid());

        assertThat(actualCoverageVectors, hasSize(2));
    }
}
