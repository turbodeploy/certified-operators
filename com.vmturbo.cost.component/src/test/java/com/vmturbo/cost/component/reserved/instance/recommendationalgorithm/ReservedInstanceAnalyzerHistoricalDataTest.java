package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class tests methods in the ReservedInstanceAnalyzerUtils class.
 * <p>
 * There are a number of tests with the @Ignore tag.  These tags should be removed as the corresponding
 * Jiras are closed:
 * 1) OM-50131: testComputeAnalysisClustersAccountScoping
 * 2) OM-50131: testComputeAnalysisClustersMultipleRecordsScopeToAccount
 * 3) OM-49957: testGetDemandFromRecordsConsumption
 * </p>
 */
public class ReservedInstanceAnalyzerHistoricalDataTest {

    /**
     * Test computeContexts method for one record historical demand and explores different scopes
     * specifications that succeed.
     */
    @Test
    public void testComputeAnalysisClustersForScope() {
        /*
         * Create input parameter: scope - defaults for everything
         */
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, null, 0f, true, null);
        // Create input parameter: tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store.
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);

        Assert.assertTrue(result.size() == 1);

        ReservedInstanceRegionalContext regionalContext = new ReservedInstanceRegionalContext(
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
            ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID);
        Assert.assertTrue(result.keySet().contains(regionalContext));


        List<ReservedInstanceZonalContext> zones = null;
        for (ReservedInstanceRegionalContext key : result.keySet()) {
            if (key.equals(regionalContext)) {
                zones = result.get(key);
            }
        }
        Assert.assertTrue(zones != null);
        Assert.assertTrue(zones.size() == 1);
        ReservedInstanceZonalContext zonalContext = new ReservedInstanceZonalContext(
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
            ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID);
        Assert.assertTrue("expected zonalContext=" + zonalContext + " not in results=" + zones,
            zones.contains(zonalContext));

        /*
         * Create input parameter: scope - all attributes are explicitly specified
         */
        scope = new ReservedInstanceAnalysisScope(Arrays.asList(OSType.LINUX),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID),
            Arrays.asList(Tenancy.DEFAULT),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_11_OID), 0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();
        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.size() == 1);
        Assert.assertTrue(result.keySet().contains(regionalContext));
        zones = null;
        for (ReservedInstanceRegionalContext key : result.keySet()) {
            if (key.equals(regionalContext)) {
                zones = result.get(key);
            }
        }
        Assert.assertTrue(zones != null);
        Assert.assertTrue(zones.size() == 1);
        Assert.assertTrue("expected zonalContext=" + zonalContext + " not in results=" + zones,
            zones.contains(zonalContext));

        /*
         * Create input parameter: scope - multiple attributes are explicitly specified
         */
        scope = new ReservedInstanceAnalysisScope(Arrays.asList(OSType.LINUX, OSType.WINDOWS, OSType.SUSE),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID,
                ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID),
            Arrays.asList(Tenancy.DEFAULT, Tenancy.DEDICATED),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_11_OID,
                          ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_12_OID),
            0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();
        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.keySet().contains(regionalContext));
        zones = null;
        for (ReservedInstanceRegionalContext key : result.keySet()) {
            if (key.equals(regionalContext)) {
                zones = result.get(key);
            }
        }
        Assert.assertTrue(zones != null);
        Assert.assertTrue(zones.size() == 1);
        Assert.assertTrue("expected zonalContext=" + zonalContext + " not in results=" + zones,
            zones.contains(zonalContext));
    }

    /**
     * Test that OS Type (platform) scoping is observed with one record store.
     */
    @Test
    public void testComputeAnalysisClustersPlatformScoping() {
        // Create input parameter: scope.
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(Arrays.asList(OSType.WINDOWS),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID),
            Arrays.asList(Tenancy.DEFAULT),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_11_OID), 0f, true, null);
        // Create input parameter tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 0);

        // Create input parameter: scope.
        scope = new ReservedInstanceAnalysisScope(Arrays.asList(OSType.WINDOWS, OSType.SUSE),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID),
            Arrays.asList(Tenancy.DEFAULT),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_11_OID), 0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();
        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.size() == 0);

    }

    /**
     * Test that tenancy scoping is observed with one record store.
     */
    @Test
    public void testComputeAnalysisClustersTenancyScoping() {
        // Create input parameter: scope.
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null,
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID),
            Arrays.asList(Tenancy.DEDICATED),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_11_OID), 0f, true, null);
        // Create input parameter tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 0);
    }

    /**
     * Test that the region scoping is observed with one record store.
     */
    @Test
    public void testComputeAnalysisClustersRegionScoping() {
        // Create input parameter: scope.
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null,
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID),
            null,
            null, 0f, true, null);
        // Create input parameter tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 0);
    }

    /**
     * Test that the account scoping is observed with one record store.
     * This test fails because computeContexts is not checking that the account is specified in the
     * scope.
     */
    @Test
    @Ignore
    public void testComputeAnalysisClustersAccountScoping() {
        // Create input parameter: scope.
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_12_OID), 0f, true, null);
        // Create input parameter tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 0);

        /*
         * Multiple wrong accounts specified
         */
        scope = new ReservedInstanceAnalysisScope(null, null,
            null,
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_12_OID,
                ReservedInstanceAnalyzerConstantsTest.BUSINESS_ACCOUNT_21_OID), 0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.size() == 0);
    }

    /*
     * Create historical demand.  Only one record.
     */
    private Set<ComputeTierTypeHourlyByWeekRecord> createOneRecordSet() {
        ComputeTierTypeHourlyByWeekRecord record1Isf = new ComputeTierTypeHourlyByWeekRecord(
            ReservedInstanceAnalyzerConstantsTest.HOUR1,
            ReservedInstanceAnalyzerConstantsTest.DAY1,
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
            ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID,
            (byte)Platform.LINUX.getValue(),
            (byte)Tenancy.DEFAULT.getNumber(),
            ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION,
            ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = new HashSet<>();
        recordSet.add(record1Isf);
        return recordSet;
    }

    /**
     * Test computeContexts method for multiple record historical demand and explores different
     * scopes such that the test succeeds.
     */
    @Test
    public void testComputeAnalysisClustersForMultiRecords() {
        /*
         * Create input parameter: scope - defaults for everything
         */
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, null, 0f, true, null);
        // Create input parameter: tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store.
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createMultiRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 2);

        ReservedInstanceRegionalContext regionOhioContext = new ReservedInstanceRegionalContext(
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
            ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID);
        Assert.assertTrue(result.keySet().contains(regionOhioContext));

        List<ReservedInstanceZonalContext> ohioZones = result.get(regionOhioContext);
        Assert.assertTrue(ohioZones.size() == 2);

        /*
         * Because this is ISF, the compute tier is t2.nano, which is the smallest instance type in
         * t2 family, even though the demand is for t2.micro.
         */
        ReservedInstanceRegionalContext regionOregonContext = new ReservedInstanceRegionalContext(
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO,
            ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID);
        Assert.assertTrue(result.keySet().contains(regionOregonContext));

        List<ReservedInstanceZonalContext> oregonZones = result.get(regionOregonContext);
        Assert.assertTrue(oregonZones.size() == 1);

        /*
         * Create input parameter: scope - restrict Regions to Ohio
         */
        scope = new ReservedInstanceAnalysisScope(null,
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID),
            Arrays.asList(Tenancy.DEFAULT),
            null, 0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();
        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.size() == 1);
        Assert.assertTrue(result.keySet().contains(regionOhioContext));
        Assert.assertFalse(result.keySet().contains(regionOregonContext));
        List<ReservedInstanceZonalContext> zones = result.get(regionOhioContext);
        Assert.assertTrue(zones != null);
        Assert.assertTrue(zones.size() == 2);


        /*
         * Create input parameter scope: explicitly set all attributes
         */
        scope = new ReservedInstanceAnalysisScope(Arrays.asList(OSType.LINUX, OSType.WINDOWS, OSType.SUSE),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID,
                ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID),
            Arrays.asList(Tenancy.DEFAULT, Tenancy.DEDICATED),
            Arrays.asList(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
                ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID), 0f, true, null);
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();
        result = new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
            tierFamilies,
            cloudTopology);
        Assert.assertTrue(result.keySet().size() == 2);
        Assert.assertTrue(result.keySet().contains(regionOhioContext));
        Assert.assertTrue(result.keySet().contains(regionOregonContext));
    }

    /**
     * Scope by account with multiple records.
     * This fails because scoping by account is not working.
     */
    @Test
    @Ignore
    public void testComputeAnalysisClustersMultipleRecordsScopeToAccount() {
        /*
         * Create input parameter: scope - restrict master account
         * Broken.
         */
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(null, null,
            null, Arrays.asList(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID), 0f, true, null);
        // Create input parameter: tierFamilies
        Map<String, List<TopologyEntityDTO>> tierFamilies = createTierFamilies();
        // Create input parameter: cloudTopology
        TopologyEntityCloudTopology cloudTopology = createCloudTopology();
        // Create input parameter: store.
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createMultiRecordSet();
        Mockito.doReturn(recordSet.stream()).when(store).getAllDemandStats();

        Map<ReservedInstanceRegionalContext,
            List<ReservedInstanceZonalContext>> result =
            new ReservedInstanceAnalyzerHistoricalData(store).computeAnalysisClusters(scope,
                tierFamilies,
                cloudTopology);
        Assert.assertTrue(result.size() == 1);
        ReservedInstanceRegionalContext regionOregonContext = new ReservedInstanceRegionalContext(
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
            ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID);
        Assert.assertTrue(result.keySet().contains(regionOregonContext));
        List<ReservedInstanceZonalContext> zones = result.get(regionOregonContext);
        Assert.assertTrue(zones != null);
        Assert.assertTrue(zones.size() == 1);
    }

    /**
     * Create historical demand with multiple records.
     */
    private Set<ComputeTierTypeHourlyByWeekRecord> createMultiRecordSet() {
        ComputeTierTypeHourlyByWeekRecord record1 = new ComputeTierTypeHourlyByWeekRecord(
            // two records with master account 1 in ohio region
            ReservedInstanceAnalyzerConstantsTest.HOUR1,
            ReservedInstanceAnalyzerConstantsTest.DAY1,
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
            ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID,
            (byte)Platform.LINUX.getValue(),
            (byte)Tenancy.DEFAULT.getNumber(),
            ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION,
            ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION);
        ComputeTierTypeHourlyByWeekRecord record2 = new ComputeTierTypeHourlyByWeekRecord(
            ReservedInstanceAnalyzerConstantsTest.HOUR2,
            ReservedInstanceAnalyzerConstantsTest.DAY1,
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
            ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_2_OID,
            (byte)Platform.LINUX.getValue(),
            (byte)Tenancy.DEFAULT.getNumber(),
            ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION,
            ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION);
        // one record master account 10 in oregon region
        ComputeTierTypeHourlyByWeekRecord record3 = new ComputeTierTypeHourlyByWeekRecord(
            ReservedInstanceAnalyzerConstantsTest.HOUR3,
            ReservedInstanceAnalyzerConstantsTest.DAY1,
            ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID,
            ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OREGON_1_OID,
            (byte)Platform.LINUX.getValue(),
            (byte)Tenancy.DEFAULT.getNumber(),
            ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION,
            ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION);
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = new HashSet<>();
        recordSet.add(record1);
        recordSet.add(record2);
        recordSet.add(record3);
        return recordSet;
    }

    /**
     * Create cloud topology.
     * Only includes mocked responses for m5.large and t2.micro compute tiers OIDs
     * and Ohio and Oregon regions OIDs.
     */
    private TopologyEntityCloudTopology createCloudTopology() {

        TopologyEntityCloudTopology cloudTopology = Mockito.mock(TopologyEntityCloudTopology.class);
        Mockito.doReturn(Optional.of(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE))
            .when(cloudTopology).getEntity(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID);
        Mockito.doReturn(Optional.of(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO))
            .when(cloudTopology)
            .getEntity(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID);

        Mockito.doReturn(Optional.of(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO))
            .when(cloudTopology)
            .getEntity(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID);
        Mockito.doReturn(Optional.of(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON))
            .when(cloudTopology)
            .getEntity(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID);
        return cloudTopology;
    }

    /**
     * create the tier families map.
     */
    private Map<String, List<TopologyEntityDTO>> createTierFamilies() {
        Map<String, List<TopologyEntityDTO>> map = new HashMap<>();
        map.put(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_FAMILY,
            (List<TopologyEntityDTO>)Arrays.asList(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE));
        map.put(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_FAMILY,
            (List<TopologyEntityDTO>)Arrays.asList(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO,
                                                    ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO));
        return map;
    }

    /**
     * Test getComputeTierToBuyFor method.
     */
    @Test
    public void testGetComputeTierToBuyFor() {
        Map<String, List<TopologyEntityDTO>> computeTierFamilies = createTierFamilies();
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        // not ISF
        TopologyEntityDTO profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
                OSType.WINDOWS,
                Tenancy.DEFAULT,
                computeTierFamilies
            );
        Assert.assertTrue(profile.equals(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE));
        // not ISF
        profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
                OSType.SUSE,
                Tenancy.DEFAULT,
                computeTierFamilies
            );
        // not ISF
        profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
                OSType.RHEL,
                Tenancy.DEFAULT,
                computeTierFamilies
            );
        Assert.assertTrue(profile.equals(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO));
        // not ISF
        profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
                OSType.LINUX,
                Tenancy.HOST,
                computeTierFamilies
            );
        Assert.assertTrue(profile.equals(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO));
        // ISF
        profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
                OSType.LINUX,
                Tenancy.DEFAULT,
                computeTierFamilies
            );
        Assert.assertTrue(profile.equals(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO));
    }

    /**
     * Currently, not checking Dedicated tenancy for instance size flexible.
     */
    @Test
    public void testGetComputeTierToBuyForDedicated() {
        Map<String, List<TopologyEntityDTO>> computeTierFamilies = createTierFamilies();
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        // ISF
        TopologyEntityDTO profile =
            new ReservedInstanceAnalyzerHistoricalData(store).getComputeTierToBuyFor(
                ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
                OSType.LINUX,
                Tenancy.DEDICATED,
                computeTierFamilies
            );
        Assert.assertTrue(profile.equals(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO));
    }

    /**
     * Test the getDemandFromRecords method for Allocation data.
     */
    @Test
    public void testGetDemandFromRecordsAllocation() {
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);
        float[] demand =
            new ReservedInstanceAnalyzerHistoricalData(store).getDemandFromRecords(Lists.newArrayList(recordSet),
                ReservedInstanceHistoricalDemandDataType.ALLOCATION);
        Assert.assertTrue(demand.length == 168);
        Map<Float, Integer> map = computeMemberSize(demand);
        Assert.assertTrue(map.get(-1.0f) == 167);
        Assert.assertTrue(map.get(ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION.floatValue()) == 1);


        recordSet = createMultiRecordSet();
        demand =
            new ReservedInstanceAnalyzerHistoricalData(store).getDemandFromRecords(Lists.newArrayList(recordSet),
                ReservedInstanceHistoricalDemandDataType.ALLOCATION);
        Assert.assertTrue(demand.length == 168);
        map = computeMemberSize(demand);
        Assert.assertTrue(map.get(-1.0f) == 165);
        Assert.assertTrue(map.get(ReservedInstanceAnalyzerConstantsTest.VALUE_ALLOCATION.floatValue()) == 3);
    }

    /**
     * Test the getDemandFromRecords method for Consumption data.
     * This test fails because the ReservedInstanceHistoricalDemandDataType enumerate type has
     * the demand type and field in DB record swapped.
     */
    @Ignore
    @Test
    public void testGetDemandFromRecordsConsumption() {
        Set<ComputeTierTypeHourlyByWeekRecord> recordSet = createOneRecordSet();
        ComputeTierDemandStatsStore store = Mockito.spy(ComputeTierDemandStatsStore.class);

        float[] demand = new ReservedInstanceAnalyzerHistoricalData(store).getDemandFromRecords(Lists.newArrayList(recordSet),
                ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
        Assert.assertTrue(demand.length == 168);
        Map<Float, Integer> map = computeMemberSize(demand);
        Assert.assertTrue(map.get(-1.0f) == 167);
        Assert.assertTrue(map.get(ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION.floatValue()) == 1);

        recordSet = createMultiRecordSet();
        demand =
            new ReservedInstanceAnalyzerHistoricalData(store).getDemandFromRecords(Lists.newArrayList(recordSet),
                ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
        Assert.assertTrue(demand.length == 168);
        map = computeMemberSize(demand);
        Assert.assertTrue(map.get(-1.0f) == 165);
        Assert.assertTrue(map.get(ReservedInstanceAnalyzerConstantsTest.VALUE_CONSUMPTION.floatValue()) == 3);

    }

    private Map<Float, Integer> computeMemberSize(float[] array) {
        Map<Float, Integer> map = new HashMap<>();
        for (float f: array) {
            if (map.containsKey(f)) {
                map.put(f, map.get(f) +1);
            } else {
                map.put(f, 1);
            }
        }
        return map;
    }
}
