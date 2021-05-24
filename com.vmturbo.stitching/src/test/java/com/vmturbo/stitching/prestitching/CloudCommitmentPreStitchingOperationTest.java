package com.vmturbo.stitching.prestitching;

import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.TestStitchingEntity;

/**
 * Unit tests for {@link CloudCommitmentPreStitchingOperation}.
 */
public class CloudCommitmentPreStitchingOperationTest {

    /**
     * Test that Cloud Commitment belonging to GovCloud is removed if there is no real (non-proxy)
     * GovCloud region in the topology.
     */
    @Test
    public void testCloudCommitmentRemoval() {
        // Standard region
        final String standardRegionId = "standardRegionId";
        final StitchingEntity standardRegion = new TestStitchingEntity(EntityDTO
                .newBuilder()
                .setEntityType(EntityType.REGION)
                .setId(standardRegionId));

        // Cloud Commitment in Standard region
        final String standardCloudCommitmentId = "standardCloudCommitmentId";
        final StitchingEntity standardCloudCommitment = new TestStitchingEntity(EntityDTO
                .newBuilder()
                .setEntityType(EntityType.CLOUD_COMMITMENT)
                .setId(standardCloudCommitmentId)
                .addConnectedEntities(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId(standardRegionId)
                        .build()));

        // GovCloud region
        final String govCloudRegionId = "govCloudRegionId";
        final StitchingEntity govCloudRegion = new TestStitchingEntity(EntityDTO
                .newBuilder()
                .setEntityType(EntityType.REGION)
                .setId(govCloudRegionId)
                .setOrigin(EntityOrigin.PROXY));

        // Cloud Commitment in GovCloud region
        final String govCloudCloudCommitmentId = "govCloudCloudCommitmentId";
        final StitchingEntity govCloudCloudCommitment = new TestStitchingEntity(EntityDTO
                .newBuilder()
                .setEntityType(EntityType.CLOUD_COMMITMENT)
                .setId(govCloudCloudCommitmentId)
                .addConnectedEntities(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId("govCloudRegionId")
                        .build()));

        final Stream<StitchingEntity> entities = Stream.of(
                standardRegion,
                standardCloudCommitment,
                govCloudRegion,
                govCloudCloudCommitment);

        @SuppressWarnings("unchecked")
        final StitchingChangesBuilder<StitchingEntity> resultBuilder = Mockito.mock(
                StitchingChangesBuilder.class);

        new CloudCommitmentPreStitchingOperation().performOperation(entities, resultBuilder);

        // Test that exactly one entity was removed and this entity is Cloud Commitment in GovCloud.
        Mockito.verify(resultBuilder, Mockito.times(1))
                .queueEntityRemoval(Mockito.any());
        Mockito.verify(resultBuilder, Mockito.times(1))
                .queueEntityRemoval(govCloudCloudCommitment);
    }
}
