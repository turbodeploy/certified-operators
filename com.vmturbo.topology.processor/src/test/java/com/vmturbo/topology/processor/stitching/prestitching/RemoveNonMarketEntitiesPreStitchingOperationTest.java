package com.vmturbo.topology.processor.stitching.prestitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.RemoveNonMarketEntitiesPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

public class RemoveNonMarketEntitiesPreStitchingOperationTest {

    private StitchingContext stitchingContext;

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity businessAccountStitchingEntity;

    private StitchingEntity vmStitchingEntity;

    final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();

    private final RemoveNonMarketEntitiesPreStitchingOperation operation =
            new RemoveNonMarketEntitiesPreStitchingOperation();

    private final EntityDTO.Builder businessAccountEntityDto = EntityDTO.newBuilder()
            .setId("1")
            .setDisplayName("business-account-entity")
            .setEntityType(EntityType.BUSINESS_ACCOUNT);

    private final EntityDTO.Builder vmEntityDto = EntityDTO.newBuilder()
            .setId("2")
            .setDisplayName("virtual-machine-entity")
            .setEntityType(EntityType.VIRTUAL_MACHINE);

    private final StitchingEntityData businessAccount =
            StitchingEntityData.newBuilder(businessAccountEntityDto)
                    .oid(11)
                    .targetId(111L)
                    .lastUpdatedTime(100L)
                    .build();

    private final StitchingEntityData vmEntity =
            StitchingEntityData.newBuilder(vmEntityDto)
                    .oid(22)
                    .targetId(112L)
                    .lastUpdatedTime(101L)
                    .build();

    @Before
    public void setup() {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        StitchingContext.Builder stitchingContextBuider = StitchingContext.newBuilder(2, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        stitchingContextBuider.addEntity(businessAccount,
                ImmutableMap.of(businessAccount.getLocalId(), businessAccount));
        stitchingContextBuider.addEntity(vmEntity,
                ImmutableMap.of(vmEntity.getLocalId(), vmEntity));
        stitchingContext = stitchingContextBuider.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);
        businessAccountStitchingEntity =
                stitchingContext.getEntity(businessAccountEntityDto).get();
        vmStitchingEntity = stitchingContext.getEntity(vmEntityDto).get();
    }

    @Test
    public void testEntityRemoved() {
        assertEquals(2, stitchingContext.size());
        operation.performOperation(
                Stream.of(businessAccountStitchingEntity), resultBuilder)
                .getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(1L, stitchingContext.size());
        assertTrue(stitchingContext.hasEntity(vmStitchingEntity));
    }
}
