package com.vmturbo.platform.analysis.ede;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.commons.Pair;
import com.vmturbo.commons.analysis.RawMaterialsMap.RawMaterial;
import com.vmturbo.commons.analysis.RawMaterialsMap.RawMaterialInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldWithSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterialMetadata;
import com.vmturbo.platform.analysis.economy.RawMaterials;
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

    private static final float FAST_CONSISTENT_SCALING_FACTOR = 0.25f;
    private static final float SLOW_CONSISTENT_SCALING_FACTOR = 1.0f;

    private final Optional<RawMaterials> rawMaterials = Optional.of(
        new RawMaterials(RawMaterialInfo.newBuilder(
            Collections.emptyList()).requiresConsistentScalingFactor(true).build()));

    /**
     * Initialize IdentityGenerator.
     */
    @BeforeClass
    public static void init() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Test ResizingGroup.generateResizes when not eligible for resize down.
     */
    @Test
    public void testResizingGroupGenerateResizesNotEligibleForResizeDown() {

        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, false, VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes when not eligible for resize down with a consistent scaling factor.
     */
    @Test
    public void testResizingGroupGenerateResizesNotEligibleForResizeDownCSF() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 100, 0, false, VMEM_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes when not eligible for resize down due to insufficient
     * headroom. The number of actions generated should be 0
     */
    @Test
    public void testResizingGroupGenerateResizesNotEligibleForResizeDownInsufficientHeadroom() {
        Resize resize1 = mockResize(TestUtils.VM_TYPE, TestUtils.VMEM, 95, 55, 0, true,
                VMEM_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.VM_TYPE, TestUtils.VMEM, 100, 50, 0, true,
                VMEM_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        Trader sellerTrader = TestUtils.createTrader(new Economy(), TestUtils.PM_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.MEM), new double[]{800}, false, false);
        CommoditySold sellerCommSold = getCommoditySold(800, 490);
        sellerCommSold.getSettings().setUtilizationUpperBound(0.5);
        Optional<RawMaterials> rawMaterials =
                Optional.of(new RawMaterials(RawMaterialInfo.newBuilder(ImmutableList.of(
                                new RawMaterial(TestUtils.MEM.getType(), false, false)))
                        .requiresConsistentScalingFactor(false)
                        .build()));
        RawMaterialMetadata metadataForMem = new RawMaterialMetadata(TestUtils.MEM.getType(), false, false);
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialAndSupplier =
                ImmutableMap.of(metadataForMem, new Pair(sellerCommSold, sellerTrader));
        rg.addResize(resize1, true, rawMaterialAndSupplier, rawMaterials);
        rg.addResize(resize2, true, rawMaterialAndSupplier, rawMaterials);
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
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(64, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes without capacity lower bound with ConsistentScalingFactor.
     * <p/>
     * Resize 200 -> 100 with capacity increment as 64.
     * The final new capacity will be 64 which is multiple of capacity increment.
     */
    @Test
    public void testResizingGroupGenerateResizeDownWithoutLowerBoundWithCSF() {
        Resize resize1 = mockResize(TestUtils.VCPU, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VCPU, 200, 100, 0, true, VMEM_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity() * getCsf(resize1),
            resize2.getNewCapacity() * getCsf(resize2), CAPACITY_COMPARISON_DELTA);
        assertEquals(64, resize1.getNewCapacity() * getCsf(resize1), CAPACITY_COMPARISON_DELTA);
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
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(192, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes with capacity lower bound with consistent scaling factor.
     * <p/>
     * Resize 300 -> 136 with capacity increment as 64 and capacity lower bound as 130.
     * The final new capacity will be 192 which is multiple of capacity increment larger than lower bound.
     */
    @Test
    public void testResizingGroupGenerateResizeDownWithLowerBoundWithCSF() {
        Resize resize1 = mockResize(TestUtils.VCPU, 300, 136, 130, true, VMEM_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VCPU, 300, 136, 130, true, VMEM_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity() * getCsf(resize1),
            resize2.getNewCapacity() * getCsf(resize2), CAPACITY_COMPARISON_DELTA);
        assertEquals(192, resize1.getNewCapacity() * getCsf(resize1), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes with capacity lower bound and capacity upper bound.
     * <p/>
     * Resize 80 -> 120 with capacity increment as 37.5 and capacity lower bound and upper bound both at 120.
     * The final action should be to resize capacity to 120 because even though 120 is not a multiple of
     * 37.5 [ ceil(120/37.5) * 37.5 == 150 ] we still cap new capacity by the upper bound of 120.
     */
    @Test
    public void testResizingGenerateResizeUpLowerBoundDoesNotExceedUpperBound() {
        Resize resize1 = mockResize(TestUtils.VMEM, 80, 120, 120, true, 37.5);
        Resize resize2 = mockResize(TestUtils.VMEM, 80, 120, 120, true, 37.5);
        Stream.of(resize1, resize2).forEach(resize -> resize.getSellingTrader()
            .getCommoditySold(TestUtils.VMEM)
            .getSettings()
            .setCapacityUpperBound(120));

        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertThat(resize1.getNewCapacity(), is(lessThanOrEqualTo(120.0)));
        assertThat(resize2.getNewCapacity(), is(lessThanOrEqualTo(120.0)));
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
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes with capacity lower bound with ConsistentScalingFactor.
     * <p/>
     * Resize 200 -> 136 with capacity increment as 64 and capacity lower bound as 130.
     * The final new capacity will be 192 which is multiple of capacity increment larger than lower bound.
     */
    @Test
    public void testResizingGroupNotGenerateResizeDownWithLowerBoundWithCSF() {
        Resize resize1 = mockResize(TestUtils.VMEM, 200, 136, 195, true, VMEM_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VMEM, 200, 136, 195, true, VMEM_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

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
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity(), resize2.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(200, resize1.getNewCapacity(), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test ResizingGroup.generateResizes with ConsistentScalingFactor.
     * <p/>
     * Resize1 from 114 to 214; resize2 from 120 to 220. The changes of both resizes are larger than
     * capacity increment (100), so final new capacity is set to a multiple of the increment (200).
     */
    @Test
    public void testResizingGroupGenerateResizeUpWithCSF() {
        Resize resize1 = mockResize(TestUtils.VCPU, 114, 214, 0, true, VCPU_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VCPU, 120, 220, 0, true, VCPU_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(resize1.getNewCapacity() * getCsf(resize1),
            resize2.getNewCapacity() * getCsf(resize2), CAPACITY_COMPARISON_DELTA);
        assertEquals(200, resize1.getNewCapacity() * getCsf(resize1), CAPACITY_COMPARISON_DELTA);
    }

    /**
     * Test that new capacity are inverse scaled using the original CSF instead of the provider CSF.
     * Resize1 from 114m to 331m;
     * Resize2 from 120m to 104m.
     * So final new capacity is set to a multiple of the increment (300m).
     * Resize1 is inverse scaled to 300 / 0.25 = 1200 MHz
     * Resize2 is inverse scaled to 300 / 1.0 = 300 MHz
     */
    @Test
    public void testResizingGroupGenerateVCPUResizeUpWithDifferentProviderCSF() {
        Resize resize1 = mockResize(TestUtils.VCPU, 114, 414, 0, true, VCPU_CAPACITY_INCREMENT,
                                    FAST_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VCPU, 120, 520, 0, true, VCPU_CAPACITY_INCREMENT,
                                    SLOW_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        Trader sellerTrader = TestUtils.createTrader(new Economy(), TestUtils.VM_TYPE, Collections.singletonList(0L),
                                                     Collections.singletonList(TestUtils.VCPU), new double[]{4000}, false, false);
        sellerTrader.getSettings().setConsistentScalingFactor(0.2f);
        CommoditySold sellerCommSold = sellerTrader.getCommoditySold(TestUtils.VCPU);
        RawMaterialMetadata metadataForVCPU = new RawMaterialMetadata(TestUtils.VCPU.getType(), true, true);
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialAndSupplier =
                ImmutableMap.of(metadataForVCPU, new Pair<>(sellerCommSold, sellerTrader));
        Optional<RawMaterials> rawMaterials =
                Optional.of(new RawMaterials(RawMaterialInfo.newBuilder(ImmutableList.of(
                                new RawMaterial(TestUtils.VCPU.getType(), false, false)))
                                                     .requiresConsistentScalingFactor(true)
                                                     .build()));
        rg.addResize(resize1, true, rawMaterialAndSupplier, rawMaterials);
        rg.addResize(resize2, true, rawMaterialAndSupplier, rawMaterials);
        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        assertEquals(300 / SLOW_CONSISTENT_SCALING_FACTOR,
                     ((Resize)actions.get(0)).getNewCapacity(), CAPACITY_COMPARISON_DELTA);
        assertEquals(300 / FAST_CONSISTENT_SCALING_FACTOR,
                     ((Resize)actions.get(1)).getNewCapacity(), CAPACITY_COMPARISON_DELTA);
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
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes with ConsistentScalingFactor.
     * <p/>
     * Resize1 from 114 to 150; resize 2 from 120 to 150. The changes of both resizes are smaller than
     * capacity increment (100), so drop the actions.
     */
    @Test
    public void testResizingGroupGenerateResizeUpWithinCapacityIncrementWithCSF() {
        Resize resize1 = mockResize(TestUtils.VMEM, 114, 150, 0, true, VCPU_CAPACITY_INCREMENT,
            SLOW_CONSISTENT_SCALING_FACTOR);
        Resize resize2 = mockResize(TestUtils.VMEM, 120, 150, 0, true, VCPU_CAPACITY_INCREMENT,
            FAST_CONSISTENT_SCALING_FACTOR);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();
        rg.addResize(resize1, true, new HashMap<>(), rawMaterials);
        rg.addResize(resize2, true, new HashMap<>(), rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes for containers where VMem resize up is generated with
     * namespace quota.
     * <p/>
     * Both resize1 and resize2 from 100 to 200. Provider namespace has capacity 400, quantity 100
     * and consistentScalingFactor 0.2. In this case of VMem resizing (with requiresConsistentScalingFactor
     * false), seller consistentScalingFactor is not applied to the calculation of headroom of namespace
     * so that namespace has sufficient available headroom for these 2 resizes. So 2 actions are generated.
     */
    @Test
    public void testResizingGroupGenerateVMemResizeUpWithSufficientNamespaceQuota() {
        Optional<RawMaterials> rawMaterials = Optional.of(new RawMaterials(RawMaterialInfo.newBuilder(ImmutableList.of(
            new RawMaterial(TestUtils.VMEM.getType(), false, false),
            new RawMaterial(TestUtils.VMEMLIMITQUOTA.getType(), true, false, false)))
            .requiresConsistentScalingFactor(false)
            .build()));

        Resize resize1 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VMEM, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VMEM, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();

        Trader sellerTrader = TestUtils.createTrader(new Economy(), TestUtils.NAMESPACE_TYPE, Collections.singletonList(0L),
            Collections.singletonList(TestUtils.VMEMLIMITQUOTA), new double[]{400}, false, false);
        sellerTrader.getSettings().setConsistentScalingFactor(0.2f);
        CommoditySold sellerCommSold = getCommoditySold(400, 100);
        RawMaterialMetadata metadataForQuota = new RawMaterialMetadata(TestUtils.VMEMLIMITQUOTA.getType(), true, false);
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialAndSupplier =
                ImmutableMap.of(metadataForQuota, new Pair(sellerCommSold, sellerTrader));
        rg.addResize(resize1, true, rawMaterialAndSupplier, rawMaterials);
        rg.addResize(resize2, true, rawMaterialAndSupplier, rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        actions.forEach(action -> assertEquals(200, ((Resize)action).getNewCapacity(), CAPACITY_COMPARISON_DELTA));
    }

    /**
     * Test ResizingGroup.generateResizes for containers VCPU where no resize up is generated with
     * namespace quota as hard constraint.
     * <p/>
     * Both resize1 and resize2 from 100 to 200. Provider namespace has capacity 400, quantity 100
     * and consistentScalingFactor 0.2. In this case of VCPU resizing (with requiresConsistentScalingFactor
     * true), seller consistentScalingFactor is applied to the calculation of headroom of namespace so
     * that namespace only has 60 (0.2 * 300) available headroom which is not sufficient for these
     * 2 resizes. So no actions are generated.
     */
    @Test
    public void testResizingGroupGenerateVCPUNoResizeUpWithNamespaceQuotaAsHardConstraint() {
        Optional<RawMaterials> rawMaterials = Optional.of(new RawMaterials(RawMaterialInfo.newBuilder(ImmutableList.of(
            new RawMaterial(TestUtils.VCPU.getType(), false, false),
            new RawMaterial(TestUtils.VCPULIMITQUOTA.getType(), true, false, true)))
            .requiresConsistentScalingFactor(true)
            .build()));

        Resize resize1 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VCPU, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VCPU, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();

        Trader sellerTrader = TestUtils.createTrader(new Economy(), TestUtils.NAMESPACE_TYPE, Collections.singletonList(0L),
            Collections.singletonList(TestUtils.VCPULIMITQUOTA), new double[]{400}, false, false);
        sellerTrader.getSettings().setConsistentScalingFactor(0.2f);
        CommoditySold sellerCommSold = getCommoditySold(400, 100);
        RawMaterialMetadata metadataForQuota = new RawMaterialMetadata(TestUtils.VCPULIMITQUOTA.getType(), true, true);
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialAndSupplier =
                ImmutableMap.of(metadataForQuota, new Pair(sellerCommSold, sellerTrader));
        rg.addResize(resize1, true, rawMaterialAndSupplier, rawMaterials);
        rg.addResize(resize2, true, rawMaterialAndSupplier, rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(0, actions.size());
    }

    /**
     * Test ResizingGroup.generateResizes for containers VCPU where no resize up is generated with
     * namespace quota as soft constraint.
     * <p/>
     * Both resize1 and resize2 from 100 to 200. Provider namespace has capacity 400, quantity 100
     * and consistentScalingFactor 0.2. In this case of VCPU resizing (with requiresConsistentScalingFactor
     * true), seller consistentScalingFactor is applied to the calculation of headroom of namespace so
     * that namespace only has 60 (0.2 * 300) available headroom which is not sufficient for these
     * 2 resizes. However, with VCPULimitQuota commodity as soft constraint (isHardConstraint is false),
     * both resize up actions resize1 and resize2 can be still generated.
     */
    @Test
    public void testResizingGroupGenerateVCPUNoResizeUpWithNamespaceQuotaAsSoftConstraint() {
        Optional<RawMaterials> rawMaterials = Optional.of(new RawMaterials(RawMaterialInfo.newBuilder(ImmutableList.of(
                        new RawMaterial(TestUtils.VCPU.getType(), false, false),
                        new RawMaterial(TestUtils.VCPULIMITQUOTA.getType(), true, false, false)))
                .requiresConsistentScalingFactor(true)
                .build()));

        Resize resize1 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VCPU, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        Resize resize2 = mockResize(TestUtils.CONTAINER_TYPE, TestUtils.VCPU, 100, 200, 0, true, VCPU_CAPACITY_INCREMENT);
        ResizingGroup rg = new ConsistentResizer().new ResizingGroup();

        Trader sellerTrader = TestUtils.createTrader(new Economy(), TestUtils.NAMESPACE_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPULIMITQUOTA), new double[]{400}, false, false);
        sellerTrader.getSettings().setConsistentScalingFactor(0.2f);
        CommoditySold sellerCommSold = getCommoditySold(400, 100);
        RawMaterialMetadata metadataForQuota = new RawMaterialMetadata(TestUtils.VCPULIMITQUOTA.getType(), true, false);
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialAndSupplier =
                ImmutableMap.of(metadataForQuota, new Pair(sellerCommSold, sellerTrader));
        rg.addResize(resize1, true, rawMaterialAndSupplier, rawMaterials);
        rg.addResize(resize2, true, rawMaterialAndSupplier, rawMaterials);

        List<Action> actions = new ArrayList<>();
        rg.generateResizes(actions);
        assertEquals(2, actions.size());
        actions.forEach(action -> assertEquals(200, ((Resize)action).getNewCapacity(), CAPACITY_COMPARISON_DELTA));
    }

    private CommoditySold getCommoditySold(final double capacity, final double quantity) {
        CommoditySold commoditySold = new CommoditySoldWithSettings();
        commoditySold.setCapacity(capacity);
        commoditySold.setQuantity(quantity);
        return commoditySold;
    }

    /**
     * Get the consistent scaling factor on the resize.
     *
     * @param resize the resize.
     * @return the consistent scaling factor on the resize.
     */
    private double getCsf(@Nonnull final Resize resize) {
        return resize.getSellingTrader().getSettings().getConsistentScalingFactor();
    }

    private Resize mockResize(CommoditySpecification commSpec, double commOldCap, double commNewCap,
                              double capacityLowerBound, boolean isEligibleForResizeDown,
                              double capacityIncrement) {
        return mockResize(TestUtils.VM_TYPE, commSpec, commOldCap, commNewCap, capacityLowerBound,
            isEligibleForResizeDown, capacityIncrement);
    }

    private Resize mockResize(int traderType, CommoditySpecification commSpec, double commOldCap, double commNewCap,
        double capacityLowerBound, boolean isEligibleForResizeDown,
        double capacityIncrement) {
        Economy economy = new Economy();
        Trader trader = TestUtils.createTrader(economy, traderType, Collections.singletonList(0L),
            Arrays.asList(commSpec), new double[]{commOldCap}, true, false);
        trader.setDebugInfoNeverUseInCode("trader");
        trader.getSettings().setIsEligibleForResizeDown(isEligibleForResizeDown);
        trader.getCommoditySold(commSpec).getSettings().setCapacityIncrement(capacityIncrement);
        trader.getCommoditySold(commSpec).getSettings().setCapacityLowerBound(capacityLowerBound);
        return new Resize(economy, trader, commSpec, commNewCap);
    }

    private Resize mockResize(CommoditySpecification commSpec, double commOldCap, double commNewCap,
                              double capacityLowerBound, boolean isEligibleForResizeDown,
                              double capacityIncrement, float consistentScalingFactor) {
        Economy economy = new Economy();
        Trader trader = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Collections.singletonList(0L),
            Arrays.asList(commSpec), new double[]{commOldCap / consistentScalingFactor}, true, false);
        trader.setDebugInfoNeverUseInCode("trader");
        trader.getSettings().setIsEligibleForResizeDown(isEligibleForResizeDown);
        trader.getSettings().setConsistentScalingFactor(consistentScalingFactor);
        trader.getCommoditySold(commSpec).getSettings().setCapacityIncrement(capacityIncrement / consistentScalingFactor);
        trader.getCommoditySold(commSpec).getSettings().setCapacityLowerBound(capacityLowerBound / consistentScalingFactor);
        return new Resize(economy, trader, commSpec, commNewCap / consistentScalingFactor);
    }
}
