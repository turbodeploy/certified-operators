package com.vmturbo.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vStorageMB;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static com.vmturbo.stitching.StitchingUpdateUtilities.copyCommodities;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.stitching.StitchingUpdateUtilities.CopyCommodityType;

/**
 * Tests for {@link StitchingUpdateUtilities}.
 */
public class StitchingUpdateUtilitiesTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final EntityDTO.Builder emptyVm = virtualMachine("1")
        .build().toBuilder();
    private final EntityDTO.Builder vmWithCommodities = virtualMachine("2")
        .selling(vCpuMHz().sold())
        .selling(vMemKB().sold())
        .buying(cpuMHz().from("3"))
        .buying(memKB().from("3"))
        .build().toBuilder();

    @Test
    public void testDoesNotClear() {
        assertEquals(2, vmWithCommodities.getCommoditiesSoldList().size());
        assertEquals(1, vmWithCommodities.getCommoditiesBoughtList().size());

        copyCommodities(CopyCommodityType.ALL).from(emptyVm).to(vmWithCommodities);
        assertEquals(2, vmWithCommodities.getCommoditiesSoldList().size());
        assertEquals(1, vmWithCommodities.getCommoditiesBoughtList().size());
    }

    @Test
    public void testCopyAllCommodities() {
        assertEquals(0, emptyVm.getCommoditiesSoldList().size());
        assertEquals(0, emptyVm.getCommoditiesBoughtList().size());

        copyCommodities(CopyCommodityType.ALL).from(vmWithCommodities).to(emptyVm);
        assertEquals(2, emptyVm.getCommoditiesSoldList().size());
        assertEquals(1, emptyVm.getCommoditiesBoughtList().size());
    }

    @Test
    public void testCopyBoughtCommodities() {
        copyCommodities(CopyCommodityType.BOUGHT).from(vmWithCommodities).to(emptyVm);
        assertEquals(0, emptyVm.getCommoditiesSoldList().size());
        assertEquals(1, emptyVm.getCommoditiesBoughtList().size());
    }

    @Test
    public void testCopySoldCommodities() {
        copyCommodities(CopyCommodityType.SOLD).from(vmWithCommodities).to(emptyVm);
        assertEquals(2, emptyVm.getCommoditiesSoldList().size());
        assertEquals(0, emptyVm.getCommoditiesBoughtList().size());
    }

    @Test
    public void testSameSourceAndDestination() {
        expectedException.expect(IllegalArgumentException.class);

        copyCommodities(CopyCommodityType.ALL).from(vmWithCommodities).to(vmWithCommodities);
    }

    @Test
    public void testCopyCommoditiesIsAdditive() {
        EntityDTO.Builder other = virtualMachine("2")
            .selling(vCpuMHz().sold())
            .selling(vStorageMB().sold())
            .buying(cpuMHz().from("4"))
            .buying(memKB().from("5"))
            .build().toBuilder();

        assertEquals(2, other.getCommoditiesSoldList().size());
        assertEquals(2, other.getCommoditiesBoughtList().size());

        copyCommodities(CopyCommodityType.ALL).from(vmWithCommodities).to(other);
        assertEquals(4, other.getCommoditiesSoldList().size());
        assertEquals(3, other.getCommoditiesBoughtList().size());
    }
}