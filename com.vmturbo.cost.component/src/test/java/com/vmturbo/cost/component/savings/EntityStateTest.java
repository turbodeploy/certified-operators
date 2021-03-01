package com.vmturbo.cost.component.savings;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class EntityStateTest {

    @Test
    public void validateDeserializedEntityStateObject() {
        final long powerFactor = 1L;
        EntityState state = new EntityState(100L);
        state.setDeletePending(true);
        state.setPowerFactor(powerFactor);
        EntityPriceChange recommendation = new EntityPriceChange.Builder()
                .sourceCost(12)
                .destinationCost(15)
                .build();
        state.setCurrentRecommendation(recommendation);
        List<Double> actionList = Arrays.asList(5d, -2d, 3d, 7d, -4d);
        state.setActionList(actionList);
        final Double realizedSavings = 10d;
        state.setRealizedSavings(realizedSavings);
        final Double realizedInvestments = 3d;
        state.setRealizedInvestments(realizedInvestments);
        final Double missedSavings = 100d;
        state.setMissedSavings(missedSavings);
        final Double missedInvestments = 30d;
        state.setMissedInvestments(missedInvestments);

        // Serialize EntityState object to JSON.
        String json = state.toJson();
        System.out.println(json);

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
        Assert.assertEquals(actionList, deserializedState.getActionList());
        Assert.assertNull(deserializedState.getRealizedSavings());
        Assert.assertNull(deserializedState.getRealizedInvestments());
        Assert.assertNull(deserializedState.getMissedSavings());
        Assert.assertNull(deserializedState.getMissedInvestments());
    }
}
