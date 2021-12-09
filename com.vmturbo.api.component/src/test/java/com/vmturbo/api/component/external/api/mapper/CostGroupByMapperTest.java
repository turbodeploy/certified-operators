package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;

/**
 * Unit tests for {@link CostGroupByMapper}.
 */
public class CostGroupByMapperTest {

    /**
     * Test conversion for normal {@link CostGroupBy}.
     */
    @Test
    public void testConvert() {
        assertEquals(GroupByType.TAG, CostGroupByMapper.toGroupByType(CostGroupBy.TAG));
    }

    /**
     * Test conversion for {@code null} argument.
     */
    @Test
    public void testConvertWithNull() {
        assertNull(CostGroupByMapper.toGroupByType(null));
    }

    /**
     * Test that conversion doesn't fail for every {@link CostGroupBy}.
     */
    @Test
    public void testConvertForAllGroupBy() {
        for (final CostGroupBy costGroupBy : CostGroupBy.values()) {
            assertNotNull(CostGroupByMapper.toGroupByType(costGroupBy));
        }
    }
}
