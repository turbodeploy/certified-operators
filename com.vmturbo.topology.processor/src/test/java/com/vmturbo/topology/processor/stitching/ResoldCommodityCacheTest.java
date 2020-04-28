package com.vmturbo.topology.processor.stitching;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests for {@link ResoldCommodityCache}.
 */
public class ResoldCommodityCacheTest {
    private final TargetStore targetStore = mock(TargetStore.class);

    private static final long PROBE_1_ID = 1000L;
    private static final long PROBE_2_ID = 2000L;

    private static final long TARGET_1A_ID = 1001L;
    private static final long TARGET_1B_ID = 1002L;

    private static final long TARGET_2_ID = 2001L;

    private final Target target1A = mock(Target.class);
    private final Target target1B = mock(Target.class);
    private final Target target2 = mock(Target.class);

    private final ProbeInfo probe1Info = ProbeInfo.newBuilder()
        .setProbeType("foo")
        .setProbeCategory("foo-category")
        .addSupplyChainDefinitionSet(TemplateDTO.newBuilder()
                .setTemplateClass(EntityType.CONTAINER_POD)
                .setTemplateType(TemplateType.BASE)
                .setTemplatePriority(0)
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.VCPU)
                    .setIsResold(true))
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.VMEM)
                    .setIsResold(true))
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.STORAGE_AMOUNT)
                    .setIsResold(false))
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.STORAGE_LATENCY))
        ).build();

    private final ProbeInfo probe2Info = ProbeInfo.newBuilder()
        .setProbeType("bar")
        .setProbeCategory("bar-category")
        .addSupplyChainDefinitionSet(TemplateDTO.newBuilder()
            .setTemplateType(TemplateType.BASE)
            .setTemplatePriority(0)
            .setTemplateClass(EntityType.WORKLOAD_CONTROLLER)
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.VCPU_LIMIT_QUOTA)
                    .setIsResold(true))
                .addCommoditySold(TemplateCommodity.newBuilder()
                    .setCommodityType(CommodityType.VMEM_LIMIT_QUOTA)
                    .setIsResold(true))
        ).build();

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        when(target1A.getId()).thenReturn(TARGET_1A_ID);
        when(target1B.getId()).thenReturn(TARGET_1B_ID);
        when(target2.getId()).thenReturn(TARGET_2_ID);

        when(target1A.getProbeId()).thenReturn(PROBE_1_ID);
        when(target1B.getProbeId()).thenReturn(PROBE_1_ID);
        when(target2.getProbeId()).thenReturn(PROBE_2_ID);

        when(target1A.getProbeInfo()).thenReturn(probe1Info);
        when(target1B.getProbeInfo()).thenReturn(probe1Info);
        when(target2.getProbeInfo()).thenReturn(probe2Info);
    }

    /**
     * Test a target with a probe not selling any commodities at all.
     */
    @Test
    public void testNoTargets() {
        when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final ResoldCommodityCache resoldCommodityCache = new ResoldCommodityCache(targetStore);
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).isPresent());
    }

    /**
     * Test a target with a probe not selling any commodities at all.
     */
    @Test
    public void testNoCommoditiesSold() {
        final ProbeInfo info = ProbeInfo.newBuilder()
            .setProbeType("foo")
            .setProbeCategory("foo-category")
            .addSupplyChainDefinitionSet(TemplateDTO.newBuilder()
                .setTemplateType(TemplateType.BASE)
                .setTemplatePriority(0)
                .setTemplateClass(EntityType.CONTAINER_POD))
            .build();
        when(target1A.getProbeInfo()).thenReturn(info);
        when(targetStore.getAll()).thenReturn(Collections.singletonList(target1A));

        final ResoldCommodityCache resoldCommodityCache = new ResoldCommodityCache(targetStore);
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).isPresent());
    }

    /**
     * Test a target with a probe selling some basic commodities.
     */
    @Test
    public void testSingleTarget() {
        when(targetStore.getAll()).thenReturn(Collections.singletonList(target1A));

        final ResoldCommodityCache resoldCommodityCache = new ResoldCommodityCache(targetStore);
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).get());
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VMEM_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_AMOUNT_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_LATENCY_VALUE).isPresent());
    }

    /**
     * Test the case when there are multiple targets for the same probe.
     */
    @Test
    public void testMultipleTargetsSameProbe() {
        when(targetStore.getAll()).thenReturn(Arrays.asList(target1A, target1B));

        final ResoldCommodityCache resoldCommodityCache = new ResoldCommodityCache(targetStore);
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).get());
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VMEM_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_AMOUNT_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_LATENCY_VALUE).isPresent());

        assertTrue(resoldCommodityCache.getIsResold(TARGET_1B_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).get());
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1B_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VMEM_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1B_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_AMOUNT_VALUE).get());
        assertFalse(resoldCommodityCache.getIsResold(TARGET_1B_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.STORAGE_LATENCY_VALUE).isPresent());
    }

    /**
     * Test the case when there are multiple probe types.
     */
    @Test
    public void testMultipleProbes() {
        when(targetStore.getAll()).thenReturn(Arrays.asList(target1A, target1B, target2));

        final ResoldCommodityCache resoldCommodityCache = new ResoldCommodityCache(targetStore);
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1A_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).get());
        assertTrue(resoldCommodityCache.getIsResold(TARGET_1B_ID, EntityType.CONTAINER_POD_VALUE,
            CommodityType.VCPU_VALUE).get());
        assertTrue(resoldCommodityCache.getIsResold(TARGET_2_ID, EntityType.WORKLOAD_CONTROLLER_VALUE,
            CommodityType.VCPU_LIMIT_QUOTA_VALUE).get());
    }
}