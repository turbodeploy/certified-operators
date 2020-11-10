package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.ConsistentResizer.ResizingGroup;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * Test ConsistentResizer.
 */
public class ConsistentResizerTest {

    private static final double CAPACITY_COMPARISON_DELTA = 0.001;
    private static final double VMEM_CAPACITY_INCREMENT = 64;
    private static final double VCPU_CAPACITY_INCREMENT = 100;

    /**
     * Test ResizingGroup.generateResizes when not eligible for resize down.
     */
    @Test
    public void testResizingGroupGenerateResizesNotEligibleForResizeDown() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, false, VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes without capacity lower bound.
     * <p/>
     * Resize 200 -> 100 with capacity increment as 64.
     * The final new capacity will be 64 which is multiple of capacity increment.
     */
    @Test
    public void testResizingGroupGenerateResizeDownWithoutLowerBound() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(64, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes with capacity lower bound.
     * <p/>
     * Resize 300 -> 136 with capacity increment as 64 and capacity lower bound as 130.
     * The final new capacity will be 192 which is multiple of capacity increment larger than lower bound.
     */
    @Test
    public void testResizingGroupGenerateResizeDownWithLowerBound() {
        Resize resize1 = mockResize(TestUtils.VMEM, 300, 136, 130, true, VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 300, 136, 130, true, VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(192, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes with capacity lower bound.
     * <p/>
     * Resize 200 -> 136 with capacity increment as 64 and capacity lower bound as 130.
     * The final new capacity will be 192 which is multiple of capacity increment larger than lower bound.
     */
    @Test
    public void testResizingGroupNotGenerateResizeDownWithLowerBound() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 136, 195, true, VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 136, 195, true, VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes.
     * <p/>
     * Resize1 from 114 to 214; resize2 from 120 to 220. The changes of both resizes are larger than
     * capacity increment (100), so final new capacity is set to a multiple of the increment (200).
     */
    @Test
    public void testResizingGroupGenerateResizeUp() {
        Resize resize1 = mockResize(TestUtils.VMEM, 114, 214, 0, true, VCPU_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 120, 220, 0, true, VCPU_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(200, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes.
     * <p/>
     * Resize1 from 114 to 150; resize 2 from 120 to 150. The changes of both resizes are smaller than
     * capacity increment (100), so drop the actions.
     */
    @Test
    public void testResizingGroupGenerateResizeUpWithinCapacityIncrement() {
        Resize resize1 = mockResize(TestUtils.VMEM, 114, 150, 0, true, VCPU_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 120, 150, 0, true, VCPU_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    private Resize mockResize(CommoditySpecification commSpec, double commOldCap, double commNewCap,
                              double capacityLowerBound, boolean isEligibleForResizeDown,
                              double capacityIncrement) {
        Economy economy = new Economy();
        Trader trader = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
            Arrays.asList(commSpec), new double[]{commOldCap}, true, false);
        trader.setDebugInfoNeverUseInCode("trader");
        trader.getSettings().setIsEligibleForResizeDown(isEligibleForResizeDown);
        trader.getCommoditySold(commSpec).getSettings().setCapacityIncrement(capacityIncrement);
        trader.getCommoditySold(commSpec).getSettings().setCapacityLowerBound(capacityLowerBound);
        return new Resize(economy, trader, commSpec, commNewCap);
    }
}
