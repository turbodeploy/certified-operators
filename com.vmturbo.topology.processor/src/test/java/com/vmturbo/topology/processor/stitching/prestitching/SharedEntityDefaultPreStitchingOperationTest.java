package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.loadEntityDTO;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
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
 * Unit test for {@link SharedEntityDefaultPreStitchingOperation}.
 */
public class SharedEntityDefaultPreStitchingOperationTest {
    private static final long SHARED_BUSINESS_USER_OID_1 = 1L;
    private static final long TARGET_ID_1 = 1L;
    private static final long TARGET_ID_2 = 2L;

    private StitchingContext stitchingContext;
    private StitchingResultBuilder stitchingResultBuilder;
    private final StitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
    private final PreStitchingOperation preStitchingOperation =
            new SharedEntityDefaultPreStitchingOperation(
                    stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                            SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType(),
                            EntityType.BUSINESS_USER),
                    Collections.singletonMap("common_dto.EntityDTO.BusinessUserData.sessionData",
                            Comparator.comparing(lhs -> ((SessionData)lhs).getVirtualMachine())));

    private EntityDTO.Builder sharedBusinessUserDTO1;
    private EntityDTO.Builder sharedBusinessUserDTO2;
    private TopologyDTO.TopologyEntityDTO.Builder expectedMergedBusinessUserDTO;
    private EntityDTO.Builder desktopPoolDTO1;
    private EntityDTO.Builder desktopPoolDTO2;
    private EntityDTO.Builder virtualMachineDTO1;
    private EntityDTO.Builder virtualMachineDTO2;

    /**
     * Setup.
     *
     * @throws IOException if failed
     */
    @Before
    public void setup() throws IOException {
        expectedMergedBusinessUserDTO = TopologyDTO.TopologyEntityDTO.newBuilder();
        JsonFormat.parser()
                .merge(new InputStreamReader(
                        SharedEntityDefaultPreStitchingOperationTest.class.getClassLoader()
                                .getResources(
                                        "protobuf/messages/shared_entity_default_pre_stitching_operation/shared_business_user_1_merged.json")
                                .nextElement()
                                .openStream()), expectedMergedBusinessUserDTO);

        sharedBusinessUserDTO1 = loadEntityDTO(
                "shared_entity_default_pre_stitching_operation/shared_business_user_1_from_target_1.json")
                .toBuilder();
        sharedBusinessUserDTO2 = loadEntityDTO(
                "shared_entity_default_pre_stitching_operation/shared_business_user_1_from_target_2.json")
                .toBuilder();
        virtualMachineDTO1 = EntityDTO.newBuilder()
                .setId("7b2ed5998e3c5b93c87b20a77f4406c710f7aad3")
                .setEntityType(EntityType.VIRTUAL_MACHINE);
        virtualMachineDTO2 = EntityDTO.newBuilder()
                .setId("123ed5998e3c5b93123b20123f4401231237a123")
                .setEntityType(EntityType.VIRTUAL_MACHINE);
        desktopPoolDTO1 = EntityDTO.newBuilder()
                .setId("1ccbaa36cd0962aef6ca3dd3b21452d11742c355")
                .setEntityType(EntityType.DESKTOP_POOL);
        desktopPoolDTO2 = EntityDTO.newBuilder()
                .setId("e806ea83d05a9d8ff24cf977d8f794797891fd21")
                .setEntityType(EntityType.DESKTOP_POOL);

        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(3, targetStore)
                .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        final StitchingEntityData stitchingEntityDataVm1 =
                StitchingEntityData.newBuilder(virtualMachineDTO1)
                        .oid(10)
                        .targetId(TARGET_ID_1)
                        .build();
        final StitchingEntityData stitchingEntityDataDp1 =
                StitchingEntityData.newBuilder(desktopPoolDTO1)
                        .oid(14)
                        .targetId(TARGET_ID_1)
                        .build();
        final StitchingEntityData stitchingEntityDataDp2 =
                StitchingEntityData.newBuilder(desktopPoolDTO2)
                        .oid(15)
                        .targetId(TARGET_ID_2)
                        .build();
        final StitchingEntityData stitchingEntityDataVm2 =
                StitchingEntityData.newBuilder(virtualMachineDTO2)
                        .oid(11)
                        .targetId(TARGET_ID_2)
                        .build();
        final StitchingEntityData stitchingEntityDataBu1Target1 =
                StitchingEntityData.newBuilder(sharedBusinessUserDTO1)
                        .oid(SHARED_BUSINESS_USER_OID_1)
                        .targetId(TARGET_ID_1)
                        .build();
        final StitchingEntityData stitchingEntityDataBu1Target2 =
                StitchingEntityData.newBuilder(sharedBusinessUserDTO2)
                        .oid(SHARED_BUSINESS_USER_OID_1)
                        .targetId(TARGET_ID_2)
                        .build();
        stitchingContextBuilder.addEntity(stitchingEntityDataBu1Target1,
                ImmutableMap.of(stitchingEntityDataVm1.getLocalId(), stitchingEntityDataVm1,
                        stitchingEntityDataBu1Target1.getLocalId(), stitchingEntityDataBu1Target1,
                        stitchingEntityDataDp1.getLocalId(), stitchingEntityDataDp1));
        stitchingContextBuilder.addEntity(stitchingEntityDataBu1Target2,
                ImmutableMap.of(stitchingEntityDataVm2.getLocalId(), stitchingEntityDataVm2,
                        stitchingEntityDataBu1Target2.getLocalId(), stitchingEntityDataBu1Target2,
                        stitchingEntityDataDp1.getLocalId(), stitchingEntityDataDp1,
                        stitchingEntityDataDp2.getLocalId(), stitchingEntityDataDp2));
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

        Assert.assertEquals(6, stitchingContext.size());
        final TopologyStitchingEntity sharedBusinessUserStitchingEntity1target1 =
                stitchingContext.getEntity(sharedBusinessUserDTO1).get();
        sharedBusinessUserStitchingEntity1target1.updateLastUpdatedTime(1);
        final TopologyStitchingEntity sharedBusinessUserStitchingEntity1target2 =
                stitchingContext.getEntity(sharedBusinessUserDTO2).get();
        sharedBusinessUserStitchingEntity1target2.updateLastUpdatedTime(2);
        preStitchingOperation.performOperation(Stream.of(sharedBusinessUserStitchingEntity1target1,
                sharedBusinessUserStitchingEntity1target2), stitchingResultBuilder)
                .getChanges()
                .forEach(change -> change.applyChange(stitchingJournal));
        Assert.assertEquals(5, stitchingContext.size());
        final List<StitchingEntity> stitchingEntities =
                Stream.of(sharedBusinessUserStitchingEntity1target1,
                        sharedBusinessUserStitchingEntity1target2)
                        .filter(stitchingContext::hasEntity)
                        .collect(Collectors.toList());

        Assert.assertEquals(1, stitchingEntities.size());
        final StitchingEntity mergedBusinessUserStitchingEntity = stitchingEntities.get(0);
        final TopologyDTO.TopologyEntityDTO.Builder actualMergedBusinessUserDTO =
                SdkToTopologyEntityConverter.newTopologyEntityDTO(
                    (TopologyStitchingEntity)mergedBusinessUserStitchingEntity, resoldCommodityCache);
        Assert.assertEquals(2, mergedBusinessUserStitchingEntity.getEntityBuilder()
                .getBusinessUserData()
                .getSessionDataCount());
        Assert.assertEquals(expectedMergedBusinessUserDTO.getCommoditySoldListList().size(),
                actualMergedBusinessUserDTO.getCommoditySoldListList().size());
        Assert.assertEquals(
                expectedMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size(),
                actualMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size());
        Assert.assertEquals(
                expectedMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size(),
                actualMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size());
        Assert.assertEquals(
                expectedMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size(),
                actualMergedBusinessUserDTO.getCommoditiesBoughtFromProvidersList().size());
    }
}
