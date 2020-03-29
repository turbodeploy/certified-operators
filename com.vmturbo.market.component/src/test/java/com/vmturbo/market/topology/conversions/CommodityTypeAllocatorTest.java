package com.vmturbo.market.topology.conversions;

import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Unit tests for {@link CommodityTypeAllocator}.
 */
public class CommodityTypeAllocatorTest {

    private CommodityTypeAllocator commodityTypeAllocator;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        commodityTypeAllocator = new CommodityTypeAllocator(new NumericIDAllocator());
    }

    /**
     * Test generate spec for commodity without key.
     */
    @Test
    public void testCommoditySpecificationNoKey() {
        final int slotNumber = 3;
        Collection<CommoditySpecificationTO> commoditySpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build(), slotNumber);
        assertEquals(slotNumber, commoditySpecs.size());
        assertThat(commoditySpecs, everyItem(
            HasPropertyWithValue.hasProperty("baseType",
                Is.is(CommodityDTO.CommodityType.POOL_CPU_VALUE))));
    }

    /**
     * Test generate spec for commodity with key.
     */
    @Test
    public void testCommoditySpecificationWithKey() {
        final int slotNumber = 3;
        Collection<CommoditySpecificationTO> commoditySpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
                .setKey("testKey")
                .build(), slotNumber);
        assertEquals(slotNumber, commoditySpecs.size());
        assertThat(commoditySpecs, everyItem(HasPropertyWithValue
            .hasProperty("baseType", Is.is(CommodityDTO.CommodityType.POOL_CPU_VALUE))));
    }

    /**
     * Test IsTimeSlotCommodity method.
     */
    @Test
    public void testIsTimeSlotCommodity() {
        final Collection<CommoditySpecificationTO> nonTimeSlotComSpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                .setKey("testKey")
                .build(), 1);
        assertEquals(1, nonTimeSlotComSpecs.size());
        assertFalse(commodityTypeAllocator.isTimeSlotCommodity(
            nonTimeSlotComSpecs.iterator().next().getType()));

        final int slotNumber = 3;
        Collection<CommoditySpecificationTO> timeSlotCommSpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
                .setKey("testKey")
                .build(), slotNumber);
        assertEquals(3, timeSlotCommSpecs.size());
        assertTrue(timeSlotCommSpecs.stream().anyMatch(e ->
            commodityTypeAllocator.isTimeSlotCommodity(e.getType())));
    }
}
