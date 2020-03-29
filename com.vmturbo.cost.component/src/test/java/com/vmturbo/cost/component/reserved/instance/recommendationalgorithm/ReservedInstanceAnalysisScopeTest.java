package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Test the ReservedInstanceAnalysisScope functionality.
 */
public class ReservedInstanceAnalysisScopeTest {

    /**
     * Build a StartBuyRIAnalysisRequest which is a list of OSType, list of Tenancy,
     * list of regions (Long), list of business accounts (Long), and purchase profile.
     * The purchase profile has RI type, and purchase date (long).
     */
    @Test
    public void testListConstructorFromRequest() {

        StartBuyRIAnalysisRequest.Builder requestBuilder =
            com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest.newBuilder();
        /*
         * lists
         */
        List<OSType> platforms = new ArrayList(Arrays.asList(OSType.values()));
        List<Tenancy> tenancies = new ArrayList(Arrays.asList(Tenancy.values()));
        List<Long> regions = new ArrayList(Arrays.asList(1L, 3L, 4L));
        List<Long> accounts = new ArrayList(Arrays.asList(5L, 6L, 7L, 8L));

        requestBuilder.addAllPlatforms(platforms);
        requestBuilder.addAllTenancies(tenancies);
        requestBuilder.addAllAccounts(accounts);
        requestBuilder.addAllRegions(regions);

        /*
         * RI Purchase profile
         */
        int term = 1;
        OfferingClass offeringClass = OfferingClass.STANDARD;
        PaymentOption paymentOption = PaymentOption.ALL_UPFRONT;
        ReservedInstanceType.Builder typeBuilder =
            com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.newBuilder();
        typeBuilder.setTermYears(term);
        typeBuilder.setOfferingClass(offeringClass);
        typeBuilder.setPaymentOption(paymentOption);

        RIPurchaseProfile.Builder profileBuilder =
            com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile.newBuilder();
        profileBuilder.setRiType(typeBuilder.build());
        RIPurchaseProfile riPurchaseProfile = profileBuilder.build();

        requestBuilder.setPurchaseProfile(riPurchaseProfile);

        /*
         * build request and generate scope
         */
        StartBuyRIAnalysisRequest request = requestBuilder.build();
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(request);

        assertTrue(longListEqual(regions, scope.getRegions()));
        assertFalse(tenancyListEqual(tenancies, scope.getTenancies()));
        assertTrue(tenancyListAllButHost(scope.getTenancies()));
        assertTrue(longListEqual(accounts, scope.getAccounts()));
        assertFalse(osTypeListEqual(platforms, scope.getPlatforms()));
        assertTrue(osTypeListAllButUnknown(scope.getPlatforms()));
        assertTrue(riPurchaseProfilesEqual(riPurchaseProfile, scope.getRiPurchaseProfile()));
    }

    /**
     * Build a StartBuyRIAnalysisRequest which is a an OSType, a Tenancy,
     * a regions (Long), a business accounts (Long), purchase profile.
     * The purchase profile has RI type, and purchase date (long).
     */
    @Test
    public void testSingletonConstructorFromRequest() {

        StartBuyRIAnalysisRequest.Builder requestBuilder =
            com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest.newBuilder();

        /*
         * Singletons
         */
        List<OSType> platforms = Arrays.asList(OSType.LINUX);
        List<Tenancy>  tenancies = Arrays.asList(Tenancy.DEFAULT);
        List<Long> regions = new ArrayList(Arrays.asList(100L));
        List<Long> accounts = new ArrayList(Arrays.asList(500L));

        requestBuilder.addAllPlatforms(platforms);
        requestBuilder.addAllTenancies(tenancies);
        requestBuilder.addAllAccounts(accounts);
        requestBuilder.addAllRegions(regions);

        /*
         * RI Purchase profile
         */
        final int term = 3;
        OfferingClass offeringClass = OfferingClass.CONVERTIBLE;
        PaymentOption paymentOption = PaymentOption.PARTIAL_UPFRONT;
        ReservedInstanceType.Builder typeBuilder =
            com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.newBuilder();
        typeBuilder.setTermYears(term);
        typeBuilder.setOfferingClass(offeringClass);
        typeBuilder.setPaymentOption(paymentOption);

        RIPurchaseProfile.Builder profileBuilder =
            com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile.newBuilder();
        profileBuilder.setRiType(typeBuilder.build());
        RIPurchaseProfile riPurchaseProfile = profileBuilder.build();

        requestBuilder.setPurchaseProfile(riPurchaseProfile);

        StartBuyRIAnalysisRequest request = requestBuilder.build();
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(request);

        assertTrue(longListEqual(regions, scope.getRegions()));
        assertTrue(tenancyListEqual(tenancies, scope.getTenancies()));
        assertTrue(longListEqual(accounts, scope.getAccounts()));
        assertTrue(osTypeListEqual(platforms, scope.getPlatforms()));
        assertTrue(riPurchaseProfilesEqual(riPurchaseProfile, scope.getRiPurchaseProfile()));

    }

    /**
     * Build an empty StartBuyRIAnalysisRequest.
     */
    @Test
    public void testNullConstructorFromRequest() {

        StartBuyRIAnalysisRequest.Builder requestBuilder =
            com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest.newBuilder();

        RIPurchaseProfile.Builder profileBuilder =
            com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile.newBuilder();

        RIPurchaseProfile riPurchaseProfile = profileBuilder.build();

        requestBuilder.setPurchaseProfile(riPurchaseProfile);

        StartBuyRIAnalysisRequest request = requestBuilder.build();
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(request);

        assertTrue("scope.getRegions()==" + scope.getRegions() + " != Empty",
            scope.getRegions().isEmpty());

        assertTrue(tenancyListAllButHost(scope.getTenancies()));
        assertTrue(osTypeListAllButUnknown(scope.getPlatforms()));
        assertTrue(scope.getAccounts().isEmpty());
        assertTrue(scope.getRegions().isEmpty());
        assertTrue("scope.getRiPurchaseProfile()=" + scope.getRiPurchaseProfile() + " != null",
            riPurchaseProfilesEqual(riPurchaseProfile, scope.getRiPurchaseProfile()));
    }

    /**
     * Test that OSType does not include UNKNOWN and includes all the other OSTypes.
     */
    @Test
    public void testConstructorWithNullOSType() {
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, null, 1.0f, false, null, null);
        assertFalse(scope.getPlatforms().contains(OSType.UNKNOWN_OS));
        assertTrue(osTypeListAllButUnknown(scope.getPlatforms()));
    }

    /**
     * Test that Tenancy does not include HOST, but includes all the other Tenancies.
     */
    @Test
    public void testConstructorWithNullTenancy() {
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, null, 1.0f, false, null, null);
        assertFalse(scope.getTenancies().contains(Tenancy.HOST));
        assertTrue(tenancyListAllButHost(scope.getTenancies()));
    }

    /**
     * Test illegal Tenancy is handled properly.  Tenancy.HOST is removed.
     */
    @Test
    public void testConstructorWithIllegalTenancy() {
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            new ArrayList(Arrays.asList(Tenancy.values())), null, 1.0f, false, null, null);
        assertFalse(scope.getTenancies().contains(Tenancy.HOST));
        assertTrue(tenancyListAllButHost(scope.getTenancies()));
    }

    /**
     * Test illegal Platform is handled properly.  OSType.UNKNOWN_OS is removed.
     */
    @Test
    public void testConstructorWithIllegalOSType() {
        ReservedInstanceAnalysisScope scope =
            new ReservedInstanceAnalysisScope(new ArrayList(Arrays.asList(OSType.values())),
                null, null, null, 1.0f, false, null, null);
        assertFalse(scope.getPlatforms().contains(OSType.UNKNOWN_OS));
        assertTrue(osTypeListAllButUnknown(scope.getPlatforms()));
    }

    private boolean longListEqual(List<Long> l1, ImmutableSet<Long> s2) {
        if (l1 == null && s2 == null) {
            return true;
        }
        if (l1 == null || s2 == null) {
            return false;
        }
        List<Long> l2 = new ArrayList(s2);
        if (!l1.containsAll(s2)) {
            return false;
        }
        if (!l2.containsAll(l1)) {
            return false;
        }
        return true;
    }

    private boolean tenancyListEqual(List<Tenancy> l1, ImmutableSet<Tenancy> s2) {
        if (l1 == null && s2 == null) {
            return true;
        }
        if (l1 == null || s2 == null) {
            return false;
        }
        List<Tenancy> l2 = new ArrayList(s2);
        if (!l1.containsAll(s2)) {
            return false;
        }
        if (!l2.containsAll(l1)) {
            return false;
        }
        return true;
    }

    private boolean tenancyListAllButHost(ImmutableSet<Tenancy> set) {
        if (set.size() != (Tenancy.values().length -1)) {
            return false;
        }
        if (set.contains(Tenancy.HOST)) {
            return false;
        }
        return true;
    }

    private boolean osTypeListEqual(List<OSType> l1, Set<OSType> s2) {
        if (l1 == null && s2 == null) {
            return true;
        }
        if (l1 == null || s2 == null) {
            return false;
        }
        List<OSType> l2 = new ArrayList(s2);
        if (!l1.containsAll(s2)) {
            return false;
        }
        if (!l2.containsAll(l1)) {
            return false;
        }
        return true;
    }

    private boolean osTypeListAllButUnknown(Set<OSType> set) {
        if (set.size() != (OSType.values().length -1)) {
            return false;
        }
        if (set.contains(OSType.UNKNOWN_OS)) {
            return false;
        }
        return true;
    }

    private boolean riPurchaseProfilesEqual(RIPurchaseProfile r1, RIPurchaseProfile r2) {
        if (r1 == null && r2 == null) {
            return true;
        }
        if (r1 == null || r2 == null) {
            return false;
        }
        if (r1.getRiType() == r2.getRiType()) {
            return true;
        }
        return false;
    }
}
