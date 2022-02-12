package com.vmturbo.cost.component.savings;

import java.util.ArrayDeque;
import java.util.Deque;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.cost.component.savings.Algorithm.Delta;
import com.vmturbo.cost.component.savings.tem.ProviderInfo;
import com.vmturbo.cost.component.savings.tem.VirtualMachineProviderInfo;

/**
 * Entity state tests.
 */
public class EntityStateTest {

    /**
     * Ensure the the entity state is being converted from JSON correctly.
     */
    @Test
    public void validateDeserializedEntityStateObject() {
        final long powerFactor = 1L;
        EntityState state = new EntityState(100L, SavingsUtil.EMPTY_PRICE_CHANGE);
        state.setDeletePending(true);
        state.setPowerFactor(powerFactor);
        EntityPriceChange recommendation = new EntityPriceChange.Builder()
                .sourceOid(0L)
                .destinationOid(0L)
                .sourceCost(12)
                .destinationCost(15)
                .build();
        state.setCurrentRecommendation(recommendation);
        Deque<Delta> deltaList = SavingsCalculatorTest.makeDeltaList(5d, -2d, 3d, 7d, -4d);
        state.setDeltaList(deltaList);
        final Double realizedSavings = 10d;
        state.setRealizedSavings(realizedSavings);
        final Double realizedInvestments = 3d;
        state.setRealizedInvestments(realizedInvestments);
        final Double missedSavings = 100d;
        state.setMissedSavings(missedSavings);
        final Double missedInvestments = 30d;
        state.setMissedInvestments(missedInvestments);
        final ProviderInfo providerInfo = new VirtualMachineProviderInfo(1000L);
        state.setProviderInfo(providerInfo);

        // Serialize EntityState object to JSON.
        String json = state.toJson();

        // Deserialize from JSON to EntityState object.
        EntityState deserializedState = EntityState.fromJson(json);

        // Check that the saved values are the same as the original state object.
        // Also verify that transient values are null.

        // The deletePending value before serialization was true. However, since this field is
        // transient, the value gets the default value for boolean which is false and it is correct.
        // The calculator will set the value to false if the entity is deleted. In which case, the
        // state record will also be removed from database.
        Assert.assertEquals(false, deserializedState.isDeletePending());

        Assert.assertEquals(powerFactor, deserializedState.getPowerFactor());
        Assert.assertEquals(recommendation, deserializedState.getCurrentRecommendation());
        Deque<Delta> deserialized = deserializedState.getDeltaList();
        Assert.assertEquals(deltaList.size(), deserialized.size());
        for (Delta delta = deltaList.pollFirst(); delta != null; delta = deltaList.pollFirst()) {
            Delta other = deserialized.pollFirst();
            Assert.assertEquals(delta.timestamp, other.timestamp);
            Assert.assertEquals(delta.delta, other.delta, 0.1d);
            Assert.assertEquals(delta.expiration, other.expiration);
        }
        // Assert.assertEquals(deltaList, deserializedState.getDeltaList());
        Assert.assertEquals(realizedSavings, deserializedState.getRealizedSavings());
        Assert.assertEquals(realizedInvestments, deserializedState.getRealizedInvestments());
        Assert.assertEquals(missedSavings, deserializedState.getMissedSavings());
        Assert.assertEquals(missedInvestments, deserializedState.getMissedInvestments());
        Assert.assertEquals(providerInfo, deserializedState.getProviderInfo());
    }

    /**
     * Verify whether older (pre-delta list) entity state is migrating correctly.
     */
    @Test
    public void testMigration() {
        String entityInfo = "{\"entityId\":74214731014905,\"powerFactor\":1,\"currentRecommendation\":{\"sourceCost\":0.007232876712328767,\"destinationCost\":0.0023150684931506848,\"delta\":-0.004917808219178083,\"sourceOid\":74214731014900,\"destinationOid\":74214731014903,\"active\":false},\"actionList\":[1.0,2.0,3.0],\"lastExecutedAction\":{},\"missedSavings\":0.0,\"missedInvestments\":0.0,\"expirationList\":[100,200,300]}";
        Deque<Delta> expectedDeltas = new ArrayDeque<>();
        for (double delta = 1; delta <= 3; delta++) {
            expectedDeltas.addLast(new Delta(0L, delta, (long)delta * 100));
        }
        // Deserialize from JSON to EntityState object.
        EntityState deserializedState = EntityState.fromJson(entityInfo);
        Assert.assertNotEquals(null, deserializedState);
        Deque<Delta> deserializedDeltas = deserializedState.getDeltaList();
        Assert.assertTrue(SavingsCalculatorTest.compareActions(expectedDeltas, deserializedDeltas));
    }
}
