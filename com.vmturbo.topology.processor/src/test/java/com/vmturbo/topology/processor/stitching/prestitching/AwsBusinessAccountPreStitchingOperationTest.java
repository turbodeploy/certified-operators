package com.vmturbo.topology.processor.stitching.prestitching;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.AwsBusinessAccountPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedEntityDefaultPreStitchingOperation;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.ResoldCommodityCache;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test that we pre-stitch AWS Business Accounts properly. We should associate the merged
 * business account with the target that has dataDiscovered == true and we should merge
 * consistsOf and entity properties.
 */
public class AwsBusinessAccountPreStitchingOperationTest {
    private static final long SHARED_BUSINESS_ACCT_OID_1 = 1L;
    private static final long TARGET_ID_1 = 1L;
    private static final long TARGET_ID_2 = 2L;
    private static final String DISPLAY_NAME_1 = "DisplayName1";
    private static final String DISPLAY_NAME_2 = "DisplayName2";
    private static final String PROPERTY_1 = "prop1";
    private static final String PROPERTY_2 = "prop2";

    private StitchingContext stitchingContext;
    private StitchingResultBuilder stitchingResultBuilder;
    private final StitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
    private final PreStitchingOperation preStitchingOperation =
            new AwsBusinessAccountPreStitchingOperation();

    private EntityDTO.Builder sharedBusinessAcctDTO1;
    private EntityDTO.Builder sharedBusinessAcctDTO2;
    private EntityDTO.Builder virtualMachineDTO1;
    private EntityDTO.Builder virtualMachineDTO2;

    /**
     * Setup.
     *
     * @throws IOException if failed
     */
    @Before
    public void setup() throws IOException {
        virtualMachineDTO1 = EntityDTO.newBuilder()
                .setId("7b2ed5998e3c5b93c87b20a77f4406c710f7aad3")
                .setEntityType(EntityType.VIRTUAL_MACHINE);
        virtualMachineDTO2 = EntityDTO.newBuilder()
                .setId("123ed5998e3c5b93123b20123f4401231237a123")
                .setEntityType(EntityType.VIRTUAL_MACHINE);
        sharedBusinessAcctDTO1 = EntityDTO.newBuilder().setEntityType(EntityType.BUSINESS_ACCOUNT)
                .setBusinessAccountData(BusinessAccountData.newBuilder()
                        .setDataDiscovered(true).build())
                .addConsistsOf(virtualMachineDTO1.getId())
                .setDisplayName(DISPLAY_NAME_1)
                .addEntityProperties(EntityProperty.newBuilder()
                        .setName(PROPERTY_1)
                        .setValue(PROPERTY_1)
                        .setNamespace("DEFAULT")
                        .build());
        sharedBusinessAcctDTO2 = EntityDTO.newBuilder().setEntityType(EntityType.BUSINESS_ACCOUNT)
                .setBusinessAccountData(BusinessAccountData.newBuilder()
                        .setDataDiscovered(false).build())
                .addConsistsOf(virtualMachineDTO2.getId())
                .setDisplayName(DISPLAY_NAME_2)
                .addEntityProperties(EntityProperty.newBuilder()
                    .setName(PROPERTY_2)
                    .setValue(PROPERTY_2)
                    .setNamespace("DEFAULT")
                    .build());


        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(3, targetStore)
                .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        final StitchingEntityData stitchingEntityDataVm1 =
                StitchingEntityData.newBuilder(virtualMachineDTO1)
                        .oid(10)
                        .targetId(TARGET_ID_1)
                        .build();
        final StitchingEntityData stitchingEntityDataVm2 =
                StitchingEntityData.newBuilder(virtualMachineDTO2)
                        .oid(11)
                        .targetId(TARGET_ID_2)
                        .build();
        final StitchingEntityData stitchingEntityDataBa1Target1 =
                StitchingEntityData.newBuilder(sharedBusinessAcctDTO1)
                        .oid(SHARED_BUSINESS_ACCT_OID_1)
                        .targetId(TARGET_ID_1)
                        .build();
        final StitchingEntityData stitchingEntityDataBa1Target2 =
                StitchingEntityData.newBuilder(sharedBusinessAcctDTO2)
                        .oid(SHARED_BUSINESS_ACCT_OID_1)
                        .targetId(TARGET_ID_2)
                        .build();
        stitchingContextBuilder.addEntity(stitchingEntityDataVm1,
                ImmutableMap.of(stitchingEntityDataVm1.getLocalId(), stitchingEntityDataVm1));
        stitchingContextBuilder.addEntity(stitchingEntityDataVm2,
                ImmutableMap.of(stitchingEntityDataVm2.getLocalId(), stitchingEntityDataVm2));
        stitchingContextBuilder.addEntity(stitchingEntityDataBa1Target1,
                ImmutableMap.of(stitchingEntityDataVm1.getLocalId(), stitchingEntityDataVm1,
                        stitchingEntityDataBa1Target1.getLocalId(), stitchingEntityDataBa1Target1));
        stitchingContextBuilder.addEntity(stitchingEntityDataBa1Target2,
                ImmutableMap.of(stitchingEntityDataVm2.getLocalId(), stitchingEntityDataVm2,
                        stitchingEntityDataBa1Target2.getLocalId(), stitchingEntityDataBa1Target2));
        stitchingContext = stitchingContextBuilder.build();
        stitchingResultBuilder = new StitchingResultBuilder(stitchingContext);
    }

    /**
     * Test for {@link SharedEntityDefaultPreStitchingOperation#performOperation}.
     */
    @Test
    public void testStitchingSharedEntities() {
        final ResoldCommodityCache resoldCommodityCache = Mockito.mock(ResoldCommodityCache.class);
        Mockito.when(resoldCommodityCache.getIsResold(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyInt())).thenReturn(Optional.empty());

        Assert.assertEquals(4, stitchingContext.size());
        final TopologyStitchingEntity sharedBusinessAcctStitchingEntity1target1 =
                stitchingContext.getEntity(sharedBusinessAcctDTO1).get();
        sharedBusinessAcctStitchingEntity1target1.updateLastUpdatedTime(1);
        final TopologyStitchingEntity sharedBusinessAcctStitchingEntity1target2 =
                stitchingContext.getEntity(sharedBusinessAcctDTO2).get();
        sharedBusinessAcctStitchingEntity1target2.updateLastUpdatedTime(2);
        preStitchingOperation.performOperation(Stream.of(sharedBusinessAcctStitchingEntity1target1,
                sharedBusinessAcctStitchingEntity1target2), stitchingResultBuilder)
                .getChanges()
                .forEach(change -> change.applyChange(stitchingJournal));
        Assert.assertEquals(3, stitchingContext.size());
        final List<StitchingEntity> stitchingEntities =
                Stream.of(sharedBusinessAcctStitchingEntity1target1,
                        sharedBusinessAcctStitchingEntity1target2)
                        .filter(stitchingContext::hasEntity)
                        .collect(Collectors.toList());

        Assert.assertEquals(1, stitchingEntities.size());
        final StitchingEntity mergedBusinessAcctStitchingEntity = stitchingEntities.get(0);
        final TopologyDTO.TopologyEntityDTO.Builder actualMergedBusinessAcctDTO =
                SdkToTopologyEntityConverter.newTopologyEntityDTO(
                        (TopologyStitchingEntity)mergedBusinessAcctStitchingEntity, resoldCommodityCache);
        Assert.assertEquals(2, mergedBusinessAcctStitchingEntity.getEntityBuilder()
                .getConsistsOfCount());
        Assert.assertEquals(DISPLAY_NAME_1, actualMergedBusinessAcctDTO.getDisplayName());
        final Map<String, String> propertyMap =
                actualMergedBusinessAcctDTO.getEntityPropertyMapMap();
        Assert.assertEquals(3, propertyMap.size());
        Assert.assertEquals(PROPERTY_1, propertyMap.get(PROPERTY_1));
        Assert.assertEquals(PROPERTY_2, propertyMap.get(PROPERTY_2));
        Assert.assertEquals(String.valueOf(TARGET_ID_1), propertyMap.get("discoveringTargetId"));
    }
}
