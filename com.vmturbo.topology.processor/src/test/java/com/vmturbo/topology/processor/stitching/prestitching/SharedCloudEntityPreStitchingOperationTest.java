package com.vmturbo.topology.processor.stitching.prestitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test functionality of SharedCloudEntityPreStitchingOperationTest by creating an instance of the
 * operation that combines identical REGIONs and making sure it works as intended.
 */
public class SharedCloudEntityPreStitchingOperationTest {
    private StitchingContext stitchingContext;

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity regionStitchingEntity1;

    private StitchingEntity regionStitchingEntity2;

    private StitchingEntity regionStitchingEntity3;

    private final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();

    private final String regionId1 = "region1";

    private final String regionId3 = "region3";

    private final long region1Oid = 1L;

    private final long region3Oid = 3L;

    private final long targetId1 = 11L;

    private final long targetId2 = 22L;

    private final SharedCloudEntityPreStitchingOperation
        operation = new SharedCloudEntityPreStitchingOperation(SDKProbeType.AWS.getProbeType(),
            EntityType.REGION);

    private final EntityDTO.Builder regionDto1 = EntityDTO.newBuilder()
        .setId(regionId1)
        .setEntityType(EntityType.REGION);

    private final EntityDTO.Builder regionDto2 = EntityDTO.newBuilder()
        .setId(regionId1)
        .setEntityType(EntityType.REGION);

    private final EntityDTO.Builder regionDto3 = EntityDTO.newBuilder()
        .setId(regionId3)
        .setEntityType(EntityType.REGION);

    private final StitchingEntityData regionEntity1 =
        StitchingEntityData.newBuilder(regionDto1)
            .oid(region1Oid)
            .targetId(targetId1)
            .build();

    private final StitchingEntityData regionEntity2 =
        StitchingEntityData.newBuilder(regionDto2)
            .oid(region1Oid)
            .targetId(targetId2)
            .build();

    private final StitchingEntityData regionEntity3 =
        StitchingEntityData.newBuilder(regionDto3)
            .oid(region3Oid)
            .targetId(targetId1)
            .build();

    /**
     * Set up the stitching context and resultBuilder and populate stitching object variables.
     */
    @Before
    public void setup() {
        StitchingContext.Builder stitchingContextBuider = StitchingContext.newBuilder(3)
            .setTargetStore(Mockito.mock(TargetStore.class))
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        stitchingContextBuider.addEntity(regionEntity1,
            ImmutableMap.of(regionEntity1.getLocalId(), regionEntity1));
        stitchingContextBuider.addEntity(regionEntity2,
            ImmutableMap.of(regionEntity2.getLocalId(), regionEntity2));
        stitchingContextBuider.addEntity(regionEntity3,
            ImmutableMap.of(regionEntity3.getLocalId(), regionEntity3));

        stitchingContext = stitchingContextBuider.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);

        regionStitchingEntity1 = stitchingContext.getEntity(regionDto1).get();
        regionStitchingEntity2 = stitchingContext.getEntity(regionDto2).get();
        regionStitchingEntity3 = stitchingContext.getEntity(regionDto3).get();
    }

    /**
     * Test that REGIONs that are duplicates get stitched into 1 REGION and REGIONs that are not
     * duplicates are left as is.
     */
    @Test
    public void testRegionsProperlyMerged() {
        // before pre-stitching
        assertEquals(3, stitchingContext.size());

        // perform operation
        operation.performOperation(Stream.of(regionStitchingEntity1, regionStitchingEntity2,
            regionStitchingEntity3), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));

        // Regions 1 and 2 were the same region and should have been merged.  Region3 was unique and
        // should remain.
        assertEquals(2, stitchingContext.size());
        assertTrue(stitchingContext.hasEntity(regionStitchingEntity1)
            || stitchingContext.hasEntity(regionStitchingEntity2));
        assertTrue(stitchingContext.hasEntity(regionStitchingEntity3));
    }
}
