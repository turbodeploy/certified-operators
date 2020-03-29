package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Test the methods of the ReservedInstanceAnalzyerRateAndRIs class.
 */
public class ReservedInstanceAnalzyerRateAndRIsTest {

    /**
     * Test ReservedInstanceSpecInfo constructor, hashCode and Equals.
     */
    @Test
    public void testReservedInstanceSpecInfo() {
        ReservedInstanceSpecInfo specInfo = ReservedInstanceSpecInfo.newBuilder()
            .setOs(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID)
            .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
            .setType(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_TYPE_1)
            .setSizeFlexible(ReservedInstanceAnalyzerConstantsTest.SIZE_FLEXIBLE_TRUE)
            .build();
        int hashCode = specInfo.hashCode();
        int hashCode1 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1.hashCode();
        Assert.assertTrue(hashCode == hashCode1);
        Assert.assertTrue(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1));
        Assert.assertTrue(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1.equals(specInfo));

        int hashCode2 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2.hashCode();
        Assert.assertFalse(hashCode == hashCode2);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2.equals(specInfo));

        int hashCode3 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3.hashCode();
        Assert.assertFalse(hashCode == hashCode3);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3.equals(specInfo));

        int hashCode4 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4.hashCode();
        Assert.assertFalse(hashCode == hashCode4);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4.equals(specInfo));
    }

    /**
     * Test ReservedInstanceSpec constructor, hashCode and Equals.
     */
    @Test
    public void testReservedInstanceSpec() {
        ReservedInstanceSpec spec = ReservedInstanceSpec.newBuilder()
            .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1)
            .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1).build();
        int hashCode = spec.hashCode();
        int hashCode1 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.hashCode();
        Assert.assertTrue(hashCode == hashCode1);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
        Assert.assertTrue(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.equals(spec));

        int hashCode2 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2.hashCode();
        Assert.assertFalse(hashCode == hashCode2);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2.equals(spec));

        int hashCode3 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3.hashCode();
        Assert.assertFalse(hashCode1 == hashCode3);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3.equals(spec));

        int hashCode4 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4.hashCode();
        Assert.assertFalse(hashCode1 == hashCode4);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4.equals(spec));
    }

}
