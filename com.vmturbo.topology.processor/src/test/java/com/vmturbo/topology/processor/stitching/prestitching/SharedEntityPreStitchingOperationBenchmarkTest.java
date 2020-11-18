package com.vmturbo.topology.processor.stitching.prestitching;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedEntityDefaultPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Benchmark for shared entity pre stitching operations.
 */
@Ignore("Time-consuming test")
public class SharedEntityPreStitchingOperationBenchmarkTest {

    private static final int TARGETS = 100;
    private static final int SHARED_ENTITIES_PER_TARGET = 1000;
    private static final int BOUGHT_COMMODITIES = 3;
    private static final int SOLD_COMMODITIES = 3;

    /**
     * Benchmark for {@link SharedCloudEntityPreStitchingOperation}.
     */
    @Ignore("Time-consuming test")
    @Test
    public void testBenchmarkCloud() {
        testBenchmark(new SharedCloudEntityPreStitchingOperation(
                stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                        SDKProbeType.AZURE.getProbeType(), EntityType.AVAILABILITY_ZONE), false));
    }

    /**
     * Benchmark for {@link SharedEntityDefaultPreStitchingOperation}.
     */
    @Ignore("Time-consuming test")
    @Test
    public void testBenchmarkDefault() {
        testBenchmark(new SharedEntityDefaultPreStitchingOperation(
                stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                        SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType(), EntityType.BUSINESS_USER),
                Collections.singletonMap("common_dto.EntityDTO.VirtualVolumeData.file",
                        Comparator.comparing(
                                lhs -> ((VirtualVolumeFileDescriptor)lhs).getPath()))));
    }

    private static void testBenchmark(final PreStitchingOperation preStitchingOperation) {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final StitchingContext.Builder stitchingContextBuilder =
                StitchingContext.newBuilder(TARGETS * SHARED_ENTITIES_PER_TARGET, targetStore)
                        .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        final StitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();

        Builder entityDtoProviderBuilder =
                EntityDTO.newBuilder().setId("2").setEntityType(EntityType.VIRTUAL_MACHINE);
        IntStream.range(1, BOUGHT_COMMODITIES + 1)
                .boxed()
                .map(soldN -> CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_ACCESS)
                        .setKey("CommodityKeyBought" + soldN))
                .forEach(entityDtoProviderBuilder::addCommoditiesSold);

        final StitchingEntityData stitchingEntityDataProvider =
                StitchingEntityData.newBuilder(entityDtoProviderBuilder).oid(2).targetId(0).build();
        for (int targetId = 0; targetId < TARGETS; ++targetId) {
            for (int entityId = 0; entityId < SHARED_ENTITIES_PER_TARGET; ++entityId) {
                final Builder entityDtoBuilder = EntityDTO.newBuilder()
                        .setId("1")
                        .setEntityType(EntityType.VIRTUAL_VOLUME)
                        .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                                .addFile(VirtualVolumeFileDescriptor.newBuilder().setPath("Path")));
                IntStream.range(1, SOLD_COMMODITIES + 1)
                        .boxed()
                        .map(soldN -> CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.CPU)
                                .setKey("CommodityKeySold" + soldN))
                        .forEach(entityDtoBuilder::addCommoditiesSold);
                final CommodityBought.Builder commoditiesBought = CommodityBought.newBuilder()
                        .setProviderType(EntityType.VIRTUAL_MACHINE)
                        .setProviderId("2");
                IntStream.range(1, BOUGHT_COMMODITIES + 1)
                        .boxed()
                        .map(boughtN -> CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.STORAGE_ACCESS)
                                .setKey("CommodityKeyBought" + boughtN))
                        .forEach(commoditiesBought::addBought);
                entityDtoBuilder.addCommoditiesBought(commoditiesBought);
                final StitchingEntityData stitchingEntityData =
                        StitchingEntityData.newBuilder(entityDtoBuilder)
                                .oid(entityId)
                                .targetId(targetId)
                                .build();
                stitchingContextBuilder.addEntity(stitchingEntityData,
                        ImmutableMap.of(stitchingEntityData.getLocalId(), stitchingEntityData,
                                stitchingEntityDataProvider.getLocalId(),
                                stitchingEntityDataProvider));
            }
        }

        final StitchingContext stitchingContext = stitchingContextBuilder.build();
        final StitchingResultBuilder stitchingResultBuilder =
                new StitchingResultBuilder(stitchingContext);

        final List<StitchingEntity> sharedEntities =
                stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_VOLUME)
                        .map(StitchingEntity.class::cast)
                        .collect(Collectors.toList());
        final Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        preStitchingOperation.performOperation(sharedEntities.stream(), stitchingResultBuilder)
                .getChanges()
                .forEach(change -> change.applyChange(stitchingJournal));
        stopwatch.stop();
        System.out.println(String.format("Time: %s", stopwatch.toString()));
    }
}
