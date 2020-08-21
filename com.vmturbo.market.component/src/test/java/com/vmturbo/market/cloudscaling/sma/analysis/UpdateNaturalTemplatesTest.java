package com.vmturbo.market.cloudscaling.sma.analysis;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.isOneOf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Test if the vms get the right natural template.
 */
public class UpdateNaturalTemplatesTest {
    private static long businessAccount = SMATestConstants.BUSINESS_ACCOUNT_BASE + 1L;
    private static long zone = SMATestConstants.ZONE_BASE + 9L;
    private static String family = "foo";

    static SMATemplate small = new SMATemplate(4L, "small", family, 2, null);
    static SMATemplate small2 = new SMATemplate(5L, "small2", family, 2, null);
    static SMATemplate medium = new SMATemplate(8L, "medium", family, 4, null);
    static SMATemplate large = new SMATemplate(16L, "large", family, 8, null);
    static SMATemplate xlarge = new SMATemplate(32L, "xlarge", family, 16, null);

    static {
        small.setOnDemandCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(1, 3));
        small.setDiscountedCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(0, 1));
        small2.setOnDemandCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(3, 1));
        small2.setDiscountedCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(0, 1));
        medium.setOnDemandCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(4, 2));
        medium.setDiscountedCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(0, 2));
        large.setOnDemandCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(6, 4));
        large.setDiscountedCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(0, 3));
        xlarge.setOnDemandCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(8, 6));
        xlarge.setDiscountedCost(businessAccount, OSType.UNKNOWN_OS, new SMACost(0, 4));
    }

    /**
     * Test the natural template is the cheapest among the providers.
     */
    @Test
    public void testChoosingMin() {
        List<SMAVirtualMachine> vms = new ArrayList<SMAVirtualMachine>();
        long vmOid = SMATestConstants.VIRTUAL_MACHINE_BASE + 1L;
        vms.add(new SMAVirtualMachine(vmOid, "name:" + vmOid, SMAUtils.NO_GROUP_ID, businessAccount, small, Arrays.asList(new SMATemplate[]{small, small2, medium, large, xlarge}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        vms.add(new SMAVirtualMachine(10001L, "name:10001", SMAUtils.NO_GROUP_ID, businessAccount, small2, Arrays.asList(new SMATemplate[]{medium, large, xlarge, small, small2}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        vms.add(new SMAVirtualMachine(10002L, "name:10002", SMAUtils.NO_GROUP_ID, businessAccount, xlarge, Arrays.asList(new SMATemplate[]{medium, large, small, small2, xlarge}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        vms.add(new SMAVirtualMachine(10003L, "name:10003", SMAUtils.NO_GROUP_ID, businessAccount, large, Arrays.asList(new SMATemplate[]{medium, xlarge, large}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        vms.add(new SMAVirtualMachine(10004L, "name:10004", SMAUtils.NO_GROUP_ID, businessAccount, medium, Arrays.asList(new SMATemplate[]{xlarge}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        vms.add(new SMAVirtualMachine(10005L, "name:10005", SMAUtils.NO_GROUP_ID, businessAccount, medium, Arrays.asList(new SMATemplate[]{}), 0, zone, SMAUtils.BOGUS_RI, OSType.UNKNOWN_OS));
        Assert.assertThat(vms.get(0).getNaturalTemplate(), sameInstance(small));
        Assert.assertThat(vms.get(1).getNaturalTemplate(), sameInstance(small2));
        Assert.assertThat(vms.get(2).getNaturalTemplate(), isOneOf(small, small2));
        Assert.assertThat(vms.get(3).getNaturalTemplate(), sameInstance(medium));
        Assert.assertThat(vms.get(4).getNaturalTemplate(), sameInstance(xlarge));
        Assert.assertThat(vms.get(5).getNaturalTemplate(), sameInstance(medium));
    }
}
