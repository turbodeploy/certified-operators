package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAccessIOPS;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAmount;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageLatencyMillis;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageProvisioned;
import static com.vmturbo.platform.common.builders.EntityBuilders.storage;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.SharedStoragePreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

public class SharedStorageTest {
    private static double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private final EntityDTO.Builder oldStorageDto = storage("old")
        .displayName("old")
        .selling(storageAmount().capacity(100.0).used(50.0))
        .selling(storageProvisioned().capacity(1000.0).used(500.0))
        .selling(storageLatencyMillis().capacity(100.0).used(100.0))
        .selling(storageAccessIOPS().capacity(100.0).used(20.0))
        .powerState(PowerState.POWERED_ON)
        .build()
        .toBuilder();

    private final EntityDTO.Builder newStorageDto = storage("new")
        .displayName("new")
        .selling(storageAmount().capacity(100.0).used(25.0))
        .selling(storageProvisioned().capacity(1000.0).used(250.0))
        .selling(storageLatencyMillis().capacity(100.0).used(50.0))
        .selling(storageAccessIOPS().capacity(100.0).used(30.0))
        .powerState(PowerState.POWERSTATE_UNKNOWN)
        .build()
        .toBuilder();

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity oldStorage;
    private StitchingEntity newStorage;
    private StitchingContext stitchingContext;

    final SharedStoragePreStitchingOperation operation = new SharedStoragePreStitchingOperation();

    @Before
    public void setup() {
        setupEntities(oldStorageDto, newStorageDto);

        oldStorage = stitchingContext.getEntity(oldStorageDto).get();
        newStorage = stitchingContext.getEntity(newStorageDto).get();
    }

    @Test
    public void testStorageAmountKeepsNewer() {
        assertEquals(2, stitchingContext.size());
        operation.performOperation(Stream.of(newStorage, oldStorage), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(1, stitchingContext.size());
        assertTrue(stitchingContext.hasEntity(newStorage));
        assertFalse(stitchingContext.hasEntity(oldStorage));

        assertEquals(25.0, newStorage.getCommoditiesSold()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_AMOUNT)
            .findFirst()
            .get().getUsed(), epsilon);
    }

    @Test
    public void testStorageProvisionedCalculation() {
        operation.performOperation(Stream.of(newStorage, oldStorage), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(700.0, newStorage.getCommoditiesSold()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_PROVISIONED)
            .findFirst()
            .get().getUsed(), epsilon);
    }

    @Test
    public void testStorageLatencyCalculationIsIopsWeightedAverage() {
        operation.performOperation(Stream.of(newStorage, oldStorage), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        // (100*20 + 50*30) / 50 = 70
        assertEquals(70.0, newStorage.getCommoditiesSold()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_LATENCY)
            .findFirst()
            .get().getUsed(), epsilon);
    }

    @Test
    public void testStorageLatencyCalculationWithoutLatency() {
        final EntityDTO.Builder a = storage("a")
            .selling(storageLatencyMillis().capacity(100.0).used(100.0))
            .build()
            .toBuilder();

        final EntityDTO.Builder b = storage("b")
            .selling(storageAccessIOPS().capacity(100.0).used(30.0))
            .build()
            .toBuilder();

        operation.performOperation(entityStream(a, b), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        final StitchingEntity result = stitchingContext.getStitchingGraph().entities().findFirst().get();

        assertEquals(100.0, result.getCommoditiesSold()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_LATENCY)
            .findFirst()
            .get().getUsed(), epsilon);
    }

    @Test
    public void testStorageAccessCalculationIsSum() {
        operation.performOperation(Stream.of(newStorage, oldStorage), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(50.0, newStorage.getCommoditiesSold()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_ACCESS)
            .findFirst()
            .get().getUsed(), epsilon);
    }

    @Test
    public void testStoragePowerState() {
        operation.performOperation(Stream.of(newStorage, oldStorage), resultBuilder)
                .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(PowerState.POWERED_ON, newStorage.getEntityBuilder().getPowerState());
    }

    private void setupEntities(@Nonnull final EntityDTO.Builder... entities) {
        final long targetIncrement = 111L;
        final long lastUpdatedIncrement = 100L;

        long oid = 1L;
        long targetId = targetIncrement;
        long lastUpdated = lastUpdatedIncrement;

        final List<StitchingEntityData> entityDataList = new ArrayList<>();
        for (EntityDTO.Builder dto : entities) {
            final StitchingEntityData stitchingData = StitchingEntityData.newBuilder(dto)
                .oid(oid)
                .targetId(targetId += targetIncrement)
                .lastUpdatedTime(lastUpdated += lastUpdatedIncrement)
                .build();

            entityDataList.add(stitchingData);
        }

        final StitchingContext.Builder builder = StitchingContext.newBuilder(entities.length)
            .setTargetStore(Mockito.mock(TargetStore.class))
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        entityDataList.forEach(entity -> builder.addEntity(entity, ImmutableMap.of(entity.getLocalId(), entity)));

        stitchingContext = builder.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);
    }

    private Stream<StitchingEntity> entityStream(@Nonnull final EntityDTO.Builder... entities) {
        setupEntities(entities);

        return stitchingContext.getStitchingGraph().entities()
            .map(Function.identity());
    }
}
