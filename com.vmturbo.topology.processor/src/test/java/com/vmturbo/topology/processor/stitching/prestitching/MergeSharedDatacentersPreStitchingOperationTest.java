package com.vmturbo.topology.processor.stitching.prestitching;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.prestitching.MergeSharedDatacentersPreStitchingOperation;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;

/**
 * Integration test for shared storage.
 *
 * Loads a minified topology containing overlapping storages and runs preStitching on them to verify that
 * storages are merged as desired.
 */
public class MergeSharedDatacentersPreStitchingOperationTest {

    private StitchingContext stitchingContext;

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity datacenterA;
    private StitchingEntity datacenterB;
    private StitchingEntity datacenterC;

    private final MergeSharedDatacentersPreStitchingOperation operation =
        new MergeSharedDatacentersPreStitchingOperation();

    private final EntityDTO.Builder dcEntityA = EntityDTO.newBuilder()
        .setId("1")
        .setDisplayName("datacenter-a")
        .setEntityType(EntityType.DATACENTER);
    private final EntityDTO.Builder dcEntityB = EntityDTO.newBuilder()
        .setId("1")
        .setDisplayName("datacenter-b")
        .setEntityType(EntityType.DATACENTER);
    private final EntityDTO.Builder dcEntityC = EntityDTO.newBuilder()
        .setId("1")
        .setDisplayName("datacenter-c")
        .setEntityType(EntityType.DATACENTER);

    private final StitchingEntityData dcA =
        StitchingEntityData.newBuilder(dcEntityA)
            .oid(11)
            .targetId(111L)
            .lastUpdatedTime(100L)
            .build();
    private final StitchingEntityData dcB =
        StitchingEntityData.newBuilder(dcEntityB)
            .oid(11)
            .targetId(222L)
            .lastUpdatedTime(90L)
            .build();
    private final StitchingEntityData dcC =
        StitchingEntityData.newBuilder(dcEntityC)
            .oid(11)
            .targetId(333L)
            .lastUpdatedTime(110L)
            .build();

    final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();

    @Before
    public void setup() {
        StitchingContext.Builder stitchingContextBuider = StitchingContext.newBuilder(3);
        stitchingContextBuider.addEntity(dcA,
            ImmutableMap.of(dcEntityA.getId(), dcA));
        stitchingContextBuider.addEntity(dcB,
            ImmutableMap.of(dcEntityB.getId(), dcB));
        stitchingContextBuider.addEntity(dcC,
            ImmutableMap.of(dcEntityC.getId(), dcC));

        stitchingContext = stitchingContextBuider.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);
        datacenterA = stitchingContext.getEntity(dcEntityA).get();
        datacenterB = stitchingContext.getEntity(dcEntityB).get();
        datacenterC = stitchingContext.getEntity(dcEntityC).get();
    }

    @Test
    public void testEntitiesMerged() {
        assertEquals(3, stitchingContext.size());
        operation.performOperation(
            Stream.of(datacenterA, datacenterB, datacenterC), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(1L, stitchingContext.size());

        final StitchingEntity mergedDatacenter = stitchingContext
            .getEntitiesOfType(EntityType.DATACENTER)
            .findFirst()
            .get();
        final List<StitchingMergeInformation> mergeInfo = mergedDatacenter
            .getMergeInformation();

        // 2 datacenters should have been merged into the original.
        assertEquals(2, mergeInfo.size());
        // The displayname should be taken from the alphabetically first.
        assertEquals("datacenter-a", mergedDatacenter.getDisplayName());
    }
}
