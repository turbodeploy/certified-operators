package com.vmturbo.topology.processor.cost;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.targets.TargetStore;

public class TopologyProcessorCostTestUtils {

    private final String riLocalId1 = "aws::ap-south-1::RI::1";
    private final String riLocalId2 = "aws::ap-south-1::RI::2";

    private final StitchingEntityData ri1 = StitchingEntityData.newBuilder(
        EntityDTO.newBuilder()
            .setEntityType(EntityType.RESERVED_INSTANCE)
            .setId("aws::ap-south-1::RI::1"))
        .oid(1)
        .build();
    private final StitchingEntityData ri2 = StitchingEntityData.newBuilder(
        EntityDTO.newBuilder()
            .setEntityType(EntityType.RESERVED_INSTANCE)
            .setId("aws::ap-south-1::RI::2"))
        .oid(2)
        .build();

    private final Map<String, StitchingEntityData> localIdToEntityMap = ImmutableMap.of(
        riLocalId1, ri1,
        riLocalId2, ri2
    );

    public StitchingContext setupStitchingContext() {
        final TargetStore targetStore = mock(TargetStore.class);
        when(targetStore.getAll()).thenReturn(Collections.emptyList());

        StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(2, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class));
        stitchingContextBuilder.addEntity(ri1, localIdToEntityMap);
        stitchingContextBuilder.addEntity(ri2, localIdToEntityMap);
        return stitchingContextBuilder.build();
    }
}
