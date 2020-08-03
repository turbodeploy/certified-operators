package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.enums.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Unit tests for the {@link ActionTypeMapper}.
 */
public class ActionTypeMapperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAllXlTypesHaveMappings() {
        for (ActionDTO.ActionType type : ActionDTO.ActionType.values()) {
            assertTrue(ActionTypeMapper.XL_TO_API_APPROXIMATE_TYPE.containsKey(type));
        }
    }

    @Test
    public void testTypeValues() {
        for (ActionDTO.ActionType type : ActionDTO.ActionType.values()) {
            assertThat(ActionTypeMapper.toApiApproximate(type),
                    Matchers.is(ActionTypeMapper.XL_TO_API_APPROXIMATE_TYPE.get(type)));
        }
    }

    @Test
    public void testApiUnmatchedType() {
        assertThat(ActionTypeMapper.fromApi(ActionType.NONE),
            Matchers.containsInAnyOrder(ActionDTO.ActionType.NONE));
    }

    @Test
    public void testApiMatchedTypes() {
        assertThat(ActionTypeMapper.fromApi(ActionType.SUSPEND),
                Matchers.containsInAnyOrder(ActionDTO.ActionType.SUSPEND,
                        ActionDTO.ActionType.DEACTIVATE));
    }
}
