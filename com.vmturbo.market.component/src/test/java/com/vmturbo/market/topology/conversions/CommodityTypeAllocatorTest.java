package com.vmturbo.market.topology.conversions;

import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Pair;
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

     /**
     * Test convert get CommdityType and slot number for commodity market ID.
     */
    @Test
    public void testGetCommoditySlotNumber() {
        // test non-timeslot commodity
        final Collection<CommoditySpecificationTO> nonTimeSlotCommoditySpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                .build(), 1);
        assertEquals(1, nonTimeSlotCommoditySpecs.size());
        final Pair<CommodityType, Optional<Integer>> commTypeAndSlot =
            commodityTypeAllocator.marketCommIdToCommodityTypeAndSlot(nonTimeSlotCommoditySpecs
                .iterator().next().getType());
        assertEquals(CommodityDTO.CommodityType.VCPU_VALUE,
            commTypeAndSlot.first.getType());
        assertFalse(commTypeAndSlot.second.isPresent());

        // test timeslot commodity
        final Collection<CommoditySpecificationTO> timeSlotCommoditySpecs =
            commodityTypeAllocator.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
                .build(), 3);
        assertEquals(3, timeSlotCommoditySpecs.size());
        List<CommoditySpecificationTO> sortedspecs =  timeSlotCommoditySpecs.stream()
            .sorted(Comparator.comparing(CommoditySpecificationTO::getType))
            .collect(Collectors.toList());
        final Pair<CommodityType, Optional<Integer>> commTypeAndSlot1 = commodityTypeAllocator
            .marketCommIdToCommodityTypeAndSlot(sortedspecs.get(0).getType());
        assertEquals(CommodityDTO.CommodityType.POOL_CPU_VALUE, commTypeAndSlot1.first.getType());
        assertTrue(commTypeAndSlot1.second.isPresent());
        assertEquals(0, (int)commTypeAndSlot1.second.get());
        final Pair<CommodityType, Optional<Integer>> commTypeAndSlot2 = commodityTypeAllocator
            .marketCommIdToCommodityTypeAndSlot(sortedspecs.get(1).getType());
        assertEquals(CommodityDTO.CommodityType.POOL_CPU_VALUE, commTypeAndSlot2.first.getType());
        assertTrue(commTypeAndSlot2.second.isPresent());
        assertEquals(1, (int)commTypeAndSlot2.second.get());
        final Pair<CommodityType, Optional<Integer>> commTypeAndSlot3 = commodityTypeAllocator
            .marketCommIdToCommodityTypeAndSlot(sortedspecs.get(2).getType());
        assertEquals(CommodityDTO.CommodityType.POOL_CPU_VALUE, commTypeAndSlot3.first.getType());
        assertTrue(commTypeAndSlot3.second.isPresent());
        assertEquals(2, (int)commTypeAndSlot3.second.get());
    }
}
