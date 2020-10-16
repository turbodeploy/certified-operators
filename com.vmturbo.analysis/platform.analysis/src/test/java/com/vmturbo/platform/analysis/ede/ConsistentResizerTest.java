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
    private static final double CAPACITY_INCREMENT = 64;

    /**
     * Test ResizingGroup.generateResizes when not eligible for resize down.
     */
    @Test
    public void testResizingGroupGenerateResizesNotEligibleForResizeDown() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, false);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true);
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
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, true);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true);
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
        Resize resize1 = mockResize(TestUtils.VMEM, 300, 136, 130, true);
        Resize resize2 = mockResize(TestUtils.VMEM, 300, 136, 130, true);
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
     * But 200 - 192 = 8 < 64 (capacity increment) so no resizes are generated.
     */
    @Test
    public void testResizingGroupNotGenerateResizeDownWithLowerBound() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 136, 130, true);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 136, 130, true);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>());
        rg.addResize(resize2, true, new HashMap<>());

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    private Resize mockResize(CommoditySpecification commSpec, double commOldCap, double commNewCap, double capacityLowerBound, boolean isEligibleForResizeDown) {
        Economy economy = new Economy();
        Trader trader = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
            Arrays.asList(commSpec), new double[]{commOldCap}, true, false);
        trader.setDebugInfoNeverUseInCode("trader");
        trader.getSettings().setIsEligibleForResizeDown(isEligibleForResizeDown);
        trader.getCommoditySold(commSpec).getSettings().setCapacityIncrement(CAPACITY_INCREMENT);
        trader.getCommoditySold(commSpec).getSettings().setCapacityLowerBound(capacityLowerBound);
        return new Resize(economy, trader, commSpec, commNewCap);
    }
}
