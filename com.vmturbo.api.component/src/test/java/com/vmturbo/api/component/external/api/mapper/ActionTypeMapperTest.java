package com.vmturbo.api.component.external.api.mapper;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

/**
 * Unit tests for the {@link ActionTypeMapper}.
 */
public class ActionTypeMapperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testTypeValues() {
        for (ActionType type : ActionType.values()) {
            Assert.assertEquals(type, ActionTypeMapper.fromApi(ActionTypeMapper.toApi(type)));
        }
    }

    @Test
    public void testIllegalApiString() {
        expectedException.expect(IllegalArgumentException.class);
        ActionTypeMapper.fromApi("Deer have no gall bladders.");
    }
}
