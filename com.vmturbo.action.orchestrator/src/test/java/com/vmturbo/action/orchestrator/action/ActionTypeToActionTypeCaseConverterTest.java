package com.vmturbo.action.orchestrator.action;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

/**
 * Tests for converting of ActionTypes to ActionTypeCases
 */
public class ActionTypeToActionTypeCaseConverterTest {

    /**
     * Tests that for each ActionType we have an appropriate ActionTypeCase.
     * If there is no ActionTypeCase for some ActionType then exception will be thrown and
     * test will be failed.
     */
    @Test
    public void testAllActionTypesMayBeConvertedToActionTypeCase() {
        for (ActionType actionType : ActionType.values()) {
            Assert.assertNotNull(ActionTypeToActionTypeCaseConverter
                    .getActionTypeCaseFor(actionType));
        }
    }
}
