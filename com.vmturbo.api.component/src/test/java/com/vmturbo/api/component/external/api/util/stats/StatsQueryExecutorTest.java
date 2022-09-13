package com.vmturbo.api.component.external.api.util.stats;

import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.snapshotWithStats;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.stat;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.statInput;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.statWithKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeaturesRequiredException;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class StatsQueryExecutorTest {

    private StatsQueryContextFactory contextFactory = mock(StatsQueryContextFactory.class);

    private StatsQueryScopeExpander scopeExpander = mock(StatsQueryScopeExpander.class);

    private StatsSubQuery statsSubQuery1 = mock(StatsSubQuery.class);

    private StatsSubQuery statsSubQuery2 = mock(StatsSubQuery.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private UuidMapper uuidMapper = mock(UuidMapper.class);

    private LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private StatsQueryExecutor executor = new StatsQueryExecutor(contextFactory, scopeExpander,
            repositoryApi, uuidMapper, licenseCheckClient);

    private ApiId scope = mock(ApiId.class);

    private StatsQueryScope expandedScope = mock(StatsQueryScope.class);

    final StatsQueryContext statsQueryContext = mock(StatsQueryContext.class);

    TopologyEntityDTO vmDto;

    private static final long MILLIS = 1_000_000;
    private static final String COOLING = "Cooling";
    private static final String POWER = "Power";
    private static final double ERROR = 1e-7;

    private static final long VCENTER_TARGET_OID = 1L;
    private static final long FABRIC_TARGET_OID = 2L;

    private static final Set<String> POWER_COOLING = ImmutableSet.of(POWER, COOLING);

    @Before
    public void setup() throws Exception {
        when(scopeExpander.expandScope(eq(scope), any())).thenReturn(expandedScope);
        when(contextFactory.newContext(eq(scope), any(), any())).thenReturn(statsQueryContext);

        executor.addSubquery(statsSubQuery1);
        executor.addSubquery(statsSubQuery2);

        when(uuidMapper.fromUuid(any())).thenReturn(scope);
        when(scope.uuid()).thenReturn("1");
        when(scope.isEntity()).thenReturn(true);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        ApiEntityType apiEntityType = ApiEntityType.fromType(1);
        Set<ApiEntityType> apiEntityTypeSet = new HashSet<>();
        apiEntityTypeSet.add(apiEntityType);
        when(scope.getScopeTypes()).thenReturn(Optional.of(apiEntityTypeSet));
        when(scope.getDisplayName()).thenReturn("Scope entity");
        vmDto = loadDtoFromJson("VmDTO.json");
    }

    @Test
    public void testGetStatsEmptyScope() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.emptySet());
        when(expandedScope.getExpandedOids()).thenReturn(Collections.emptySet());

        assertThat(executor.getAggregateStats(scope, period), is(Collections.emptyList()));
    }

    @Test
    public void testGetStatsEmptyAccountScope() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        Set<ApiEntityType> baSet = new HashSet<>();
        baSet.add(ApiEntityType.BUSINESS_ACCOUNT);
        when(scope.getScopeTypes()).thenReturn(Optional.of(baSet));
        when(expandedScope.getScopeOids()).thenReturn(Collections.emptySet());
        when(expandedScope.getExpandedOids()).thenReturn(Collections.emptySet());
        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        assertThat(executor.getAggregateStats(scope, period), is(Collections.emptyList()));
    }

    @Test
    public void testGetStatsRequestAll() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        final String scopeDisplayName = "Market";
        when(scope.getDisplayName()).thenReturn(scopeDisplayName);
        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // One of the queries is applicable.
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(false);

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);

        final StatApiDTO stat = stat("foo");
        StatSnapshotApiDTO snapshot = snapshotWithStats(MILLIS, stat);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot));
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> results = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(results.size(), is(1));

        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertThat(resultSnapshot.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(resultSnapshot.getStatistics(), containsInAnyOrder(stat));
        assertThat(resultSnapshot.getDisplayName(), is(scopeDisplayName));
    }

    /**
     * Load the DTO from a JSON file.
     * @param fileName file name
     * @return DTO
     * @throws IOException error reading the file
     */
    private TopologyEntityDTO loadDtoFromJson(String fileName) throws IOException {
        String path = getClass().getClassLoader().getResource(fileName).getFile();
        String str = Files.asCharSource(new File(path), Charset.defaultCharset()).read();
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    @Test
    public void testGetStatsRequestMergeQueryStats() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);


        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    /**
     * Test the vStorage stat with a good key.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testGoodVStorageKey() throws Exception {
        Assert.assertEquals("/",
                testVStorageKey("KEY: VirtualMachine::127a3e53fcb3de994c37620f15cc15e00ae950be"));
    }

    /**
     * Test the vStorage stat with a converted key.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConvertedVStorageKey() throws Exception {
        Assert.assertEquals("/", testVStorageKey("/"));
    }

    /**
     * Test the vStorage stat with a bad key.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testBadStorageKey() throws Exception {
        Assert.assertEquals("BAD KEY", testVStorageKey("BAD KEY"));
    }

    /**
     * Test the vStorage stat with an empty key.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testNoStorageKey1() throws Exception {
        Assert.assertEquals("KEY:", testVStorageKey("KEY:"));
    }

    /**
     * Test the vStorage stat with an empty key.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testNoStorageKey2() throws Exception {
        Assert.assertEquals("KEY: ", testVStorageKey("KEY: "));
    }

    @Nonnull
    private String testVStorageKey(@Nonnull String key)
            throws OperationFailedException, InterruptedException, ConversionException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);

        StatApiDTO stat = new StatApiDTO();
        stat.setName("VStorage");
        stat.setDisplayName(key);
        StatSnapshotApiDTO snapshot = snapshotWithStats(MILLIS, stat);
        when(statsSubQuery1.getAggregateStats(any(), any()))
                .thenReturn(Collections.singletonList(snapshot));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);
        assertThat(stats.size(), is(1));

        List<StatApiDTO> results = stats.get(0).getStatistics();
        assertThat(results.size(), is(1));

        return results.get(0).getDisplayName();
    }

    @Test
    public void testGetStatsRunSubqueries() throws Exception {
        final StatApiInputDTO fooInput = statInput("foo");
        final StatApiInputDTO barInput = statInput("bar");
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Lists.newArrayList(fooInput, barInput));
        when(statsQueryContext.getRequestedStats()).thenReturn(new HashSet<>(period.getStatistics()));

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        // One query handles foo, one query handles bar.
        when(statsSubQuery1.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(fooInput)));
        when(statsSubQuery2.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(barInput)));


        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);
        verify(statsSubQuery1).getAggregateStats(Collections.singleton(fooInput), statsQueryContext);
        verify(statsSubQuery2).getAggregateStats(Collections.singleton(barInput), statsQueryContext);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsSubqueryForLeftoverStats() throws Exception {
        final StatApiInputDTO fooInput = statInput("foo");
        final StatApiInputDTO barInput = statInput("bar");
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Lists.newArrayList(fooInput, barInput));
        when(statsQueryContext.getRequestedStats()).thenReturn(new HashSet<>(period.getStatistics()));

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        // One query handles foo, one query handles everything not explicitly handled by another.
        when(statsSubQuery1.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(fooInput)));
        when(statsSubQuery2.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.leftovers());

        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);
        verify(statsSubQuery1).getAggregateStats(Collections.singleton(fooInput), statsQueryContext);
        verify(statsSubQuery2).getAggregateStats(Collections.singleton(barInput), statsQueryContext);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsRequestSameStatNameDifferentRelatedEntity() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        final StatApiDTO stat1 = stat("foo", "11");
        final StatApiDTO stat2 = stat("foo", "12");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);
        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        // verify that there are 2 stats with same name, but different relatedEntity
        assertThat(snapshotApiDTO.getStatistics().size(), is(2));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsRequestSameStatNameDifferentKey() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        final StatApiDTO stat1 = statWithKey("foo", "key1");
        final StatApiDTO stat2 = statWithKey("foo", "key2");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);
        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        // verify that there are 2 stats with same name, but different key
        assertThat(snapshotApiDTO.getStatistics().size(), is(2));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    /**
     * Only white listed entity types should contain power and cooling stats.
     *
     * @throws Exception exception
     */
    @Test
    public void testOnlyWhiteListedEntityTypesShowPowerCooling() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiDTO stat1 = stat(COOLING);
        final StatApiDTO stat2 = stat(POWER);
        initPowerTest(stat1, stat2);

        when(scope.isEntity()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);

        // An entity that is not white listed to show power and colling
        when(scope.getScopeTypes()).thenReturn(Optional.of(Sets.newHashSet(ApiEntityType.VIRTUAL_DATACENTER)));

        final List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);
        assertFalse(containsPowerOrCooling(stats));
    }

    /**
     * When scoped to target, we only show power when the target is fabric
     *
     * @throws Exception exception
     */
    @Test
    public void testTargetScopeShouldOnlyShowPowerForFabric() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiDTO stat1 = stat(COOLING);
        final StatApiDTO stat2 = stat(POWER);
        initPowerTest(stat1, stat2);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Sets.newHashSet(ApiEntityType.CHASSIS)));
        when(scope.isEntity()).thenReturn(false);
        when(scope.isTarget()).thenReturn(true);

        // Scope oid of fabric target
        when(scope.oid()).thenReturn(FABRIC_TARGET_OID);
        final List<StatSnapshotApiDTO> fabricTargetStats = executor.getAggregateStats(scope, period);
        assertTrue(containsPowerOrCooling(fabricTargetStats));

        // Scope oid of hypervisor target
        when(scope.oid()).thenReturn(VCENTER_TARGET_OID);
        final List<StatSnapshotApiDTO> hypervisorTargetStats = executor.getAggregateStats(scope, period);
        assertFalse(containsPowerOrCooling(hypervisorTargetStats));
    }

    /**
     * Market and Plans do not show power/cooling.
     *
     * @throws Exception exception
     */
    @Test
    public void testPowerAndCoolingHiddenWhenScopeIsMarketOrPlan() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiDTO stat1 = stat(COOLING);
        final StatApiDTO stat2 = stat(POWER);
        initPowerTest(stat1, stat2);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Sets.newHashSet(ApiEntityType.CHASSIS)));
        when(scope.isEntity()).thenReturn(false);

        when(scope.isRealtimeMarket()).thenReturn(true);
        final List<StatSnapshotApiDTO> marketStats = executor.getAggregateStats(scope, period);
        assertFalse(containsPowerOrCooling(marketStats));
        when(scope.isRealtimeMarket()).thenReturn(false);

        when(scope.isPlan()).thenReturn(true);
        final List<StatSnapshotApiDTO> planStats = executor.getAggregateStats(scope, period);
        assertFalse(containsPowerOrCooling(planStats));
    }

    /**
     * vCenter pm buying power from dc should be filtered out
     *
     * @throws Exception exception
     */
    @Test
    public void testPMPowerCoolingBoughtFromDCShouldBeFilteredOut() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiDTO cooling = stat(COOLING);
        final StatApiDTO power = stat(POWER);
        final StatFilterApiDTO relation = setBoughtRelation(Sets.newHashSet(power, cooling));
        initPowerTest(cooling, power);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Sets.newHashSet(ApiEntityType.PHYSICAL_MACHINE)));
        when(scope.isEntity()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);

        // Setup the fabric and vcenter datacenters
        final ApiId dcApiId = createMockDcApiId();
        final BaseApiDTO fabricDC = new BaseApiDTO();
        final BaseApiDTO vCenterDC = new BaseApiDTO();
        setUpPowerCoolingCommodities(power, cooling, dcApiId, fabricDC, vCenterDC);

        when(dcApiId.hasCachedEntityInfo()).thenReturn(true);
        final List<StatSnapshotApiDTO> vcStats = executor.getAggregateStats(scope, period);
        assertFalse(containsPowerOrCooling(vcStats));

        // power/cooling bought from a non dc and power/cooling sold should not be filtered.
        relation.setValue(StringConstants.RELATION_SOLD);
        power.setFilters(Collections.singletonList(relation));
        cooling.setRelatedEntity(new BaseApiDTO());
        final List<StatSnapshotApiDTO> fabricStats = executor.getAggregateStats(scope, period);
        assertTrue(fabricStats.get(0).getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getName().equalsIgnoreCase(POWER)));
        assertTrue(fabricStats.get(0).getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getName().equalsIgnoreCase(COOLING)));
    }

    @Test
    public void testCacheRetrievalWhenFilteringOutPowerCooling() throws Exception {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiDTO cooling = stat(COOLING);
        final StatApiDTO power = stat(POWER);
        setBoughtRelation(Sets.newHashSet(power, cooling));
        initPowerTest(cooling, power);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Sets.newHashSet(ApiEntityType.PHYSICAL_MACHINE)));
        when(scope.isEntity()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);

        // Setup the fabric and vcenter datacenters
        final ApiId dcApiId = createMockDcApiId();
        final BaseApiDTO fabricDC = new BaseApiDTO();
        final BaseApiDTO vCenterDC = new BaseApiDTO();
        setUpPowerCoolingCommodities(power, cooling, dcApiId, fabricDC, vCenterDC);

        doAnswer(invocation -> {
            when(dcApiId.hasCachedEntityInfo()).thenReturn(true);
            return null;
        }).when(uuidMapper).bulkResolveEntities(Sets.newHashSet(dcApiId));
        executor.getAggregateStats(scope, period);
        // From uuid should be called exactly twice since there are 2 stat commodities
        Mockito.verify(uuidMapper, times(2)).fromUuid(fabricDC.getUuid());
        Mockito.verify(uuidMapper, times(2)).fromUuid(vCenterDC.getUuid());

        // When we call it again, we should only see the invocations go up by 1 for each of those
        // stats since we would have stored provider information in the cache during the second pass
        executor.getAggregateStats(scope, period);
        Mockito.verify(uuidMapper, times(3)).fromUuid(fabricDC.getUuid());
        Mockito.verify(uuidMapper, times(3)).fromUuid(vCenterDC.getUuid());
    }

    /**
     * Test creation of cloud tier stats snapshots.
     *
     * @throws Exception when testCreateCloudTierStatsSnapshot fails
     */
    @Test
    public void testCreateCloudTierStatsSnapshot() throws Exception {
        List<StatApiInputDTO> statApiInputDTOS = new ArrayList<>();
        StatApiInputDTO statApiInputDTO1 = new StatApiInputDTO();
        statApiInputDTO1.setName(UICommodityType.CPU.apiStr());
        StatApiInputDTO statApiInputDTO2 = new StatApiInputDTO();
        statApiInputDTO2.setName("numCores");
        statApiInputDTOS.add(statApiInputDTO1);
        statApiInputDTOS.add(statApiInputDTO2);
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(statApiInputDTOS);

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        ApiEntityType apiEntityType = ApiEntityType.fromType(56);
        Set<ApiEntityType> apiEntityTypeSet = new HashSet<>();
        apiEntityTypeSet.add(apiEntityType);
        when(scope.getScopeTypes()).thenReturn(Optional.of(apiEntityTypeSet));

        // mock minimal entity
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(0)
                .setEntityType(56)
                .build();

        final float commodityCapacity = 20.0f;
        final int numCores = 8;

        // create mock topology entity dto
        // commodity type (CPU)
        TopologyDTO.CommodityType commodityType = TopologyDTO.CommodityType.newBuilder()
                .setType(40)
                .build();
        // commodity sold
        TopologyDTO.CommoditySoldDTO commoditySoldDTO = TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType)
                .setCapacity(commodityCapacity)
                .build();
        // compute tier info
        TopologyDTO.TypeSpecificInfo.ComputeTierInfo computeTierInfo = TopologyDTO.TypeSpecificInfo.ComputeTierInfo
                .newBuilder()
                .setNumOfCores(numCores)
                .build();
        // type specific info
        TopologyDTO.TypeSpecificInfo typeSpecificInfo = TopologyDTO.TypeSpecificInfo.newBuilder()
                .setComputeTier(computeTierInfo)
                .build();
        // topology entity dto
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(56)
                .addCommoditySoldList(commoditySoldDTO)
                .setTypeSpecificInfo(typeSpecificInfo)
                .build();

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils.mockSingleEntityRequest(topologyEntityDTO);

        when(uuidMapper.fromUuid(any())).thenReturn(scope);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);

        // ACT
        List<StatSnapshotApiDTO> results = executor.getAggregateStats(scope, period);

        // ASSERT
        assertThat(results.size(), is(1));
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertThat(resultSnapshot.getStatistics().size(), is(2));
        final StatApiDTO statApiDTO1 = resultSnapshot.getStatistics().get(0);
        assertThat(statApiDTO1.getValue(), is(commodityCapacity));
        assertThat(statApiDTO1.getName(), is(UICommodityType.CPU.apiStr()));
        final StatApiDTO statApiDTO2 = resultSnapshot.getStatistics().get(1);
        assertThat(statApiDTO2.getValue(), is((float)numCores));
        assertThat(statApiDTO2.getName(), is("numCores"));
        compareCapacity(statApiDTO1.getCapacity(), createCapacityValue(commodityCapacity));
        compareCapacity(statApiDTO2.getCapacity(), createCapacityValue(numCores));
    }

    private void compareCapacity(StatValueApiDTO expected, StatValueApiDTO actual) {
        assertEquals(expected.getAvg(), actual.getAvg(), ERROR);
        assertEquals(expected.getMax(), actual.getMax(), ERROR);
        assertEquals(expected.getMin(), actual.getMin(), ERROR);
        assertEquals(expected.getTotal(), actual.getTotal(), ERROR);

    }

    private StatValueApiDTO createCapacityValue(float capacityValue) {
        final StatValueApiDTO capacity = new StatValueApiDTO();
        capacity.setAvg(capacityValue);
        capacity.setMax(capacityValue);
        capacity.setMin(capacityValue);
        capacity.setTotal(capacityValue);
        return capacity;
    }

    /**
     * Test that the {@link StatsQueryExecutor} supports sub-queries returning stats with no epoch.
     *
     * @throws Exception when the getAggregateStats operation fails
     */
    @Test
    public void testGetStatsWithNoEpoch() throws Exception {
        // ARRANGE
        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(1L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));

        // One of the queries is applicable.
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(false);

        final StatApiDTO stat = stat("foo");
        StatSnapshotApiDTO snapshot = snapshotWithStats(MILLIS, stat);

        // The key to this test: unset the epoch field to see if this breaks anything!
        snapshot.setEpoch(null);

        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot));

        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        RepositoryApi.SingleEntityRequest singleEntityRequest = ApiTestUtils
                .mockSingleEntityRequest(vmDto);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(singleEntityRequest);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.empty());
        // ACT
        List<StatSnapshotApiDTO> results = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(results.size(), is(1));

        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertThat(resultSnapshot.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(resultSnapshot.getStatistics(), containsInAnyOrder(stat));
    }

    /**
     * Trying to request plan stats should fail if the planner feature is not available.
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void getPlanStatsUnlicensed() throws Exception {
        doThrow(new LicenseFeaturesRequiredException(Collections.singleton(ProbeLicense.PLANNER)))
                .when(licenseCheckClient).checkFeatureAvailable(ProbeLicense.PLANNER);
        when(scope.isPlan()).thenReturn(true);
        executor.getAggregateStats(scope, null);
    }

    private void initPowerTest(@Nonnull StatApiDTO stat1, @Nonnull StatApiDTO stat2)
            throws Exception {
        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getScopeOids()).thenReturn(Collections.singleton(111L));
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(111L));

        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
                .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
                .thenReturn(Collections.singletonList(snapshot2));

        // Context containing two targets, vcenter and  ucs.
        final List<ThinTargetInfo> thinTargetInfos = Lists.newArrayList(
                ImmutableThinTargetInfo.builder().oid(VCENTER_TARGET_OID).displayName("target1").isHidden(false).probeInfo(
                        ImmutableThinProbeInfo.builder().oid(3L).type("vCenter").category("hypervisor").uiCategory("hypervisor").build()).build(),
                ImmutableThinTargetInfo.builder().oid(FABRIC_TARGET_OID).displayName("target2").isHidden(false).probeInfo(
                        ImmutableThinProbeInfo.builder().oid(4L).type("UCS").category("fabric").uiCategory("fabric").build()).build());
        when(statsQueryContext.getTargets()).thenReturn(thinTargetInfos);
    }

    private boolean containsPowerOrCooling(@Nonnull List<StatSnapshotApiDTO> stats) {
        if (stats.isEmpty()) {
            return false;
        }
        return stats.get(0).getStatistics().stream().anyMatch(statApiDTO -> POWER_COOLING.contains(statApiDTO.getName()));
    }


    private ApiId createMockDcApiId() {
        final ApiId dcApiId = mock(ApiId.class);
        final Set<Long> targetIds = Sets.newHashSet(FABRIC_TARGET_OID, VCENTER_TARGET_OID);
        final CachedEntityInfo dcInfo = mock(CachedEntityInfo.class);

        when(dcInfo.getEntityType()).thenReturn(ApiEntityType.DATACENTER);
        when(dcInfo.getDiscoveringTargetIds()).thenReturn(targetIds);
        when(dcApiId.getCachedEntityInfo()).thenReturn(Optional.of(dcInfo));
        when(dcApiId.hasCachedEntityInfo()).thenReturn(false);

        return dcApiId;
    }

    private StatFilterApiDTO setBoughtRelation(@Nonnull Set<StatApiDTO> stats) {
        final StatFilterApiDTO relation = new StatFilterApiDTO();
        stats.forEach(statApiDTO -> {
            relation.setValue(StringConstants.RELATION_BOUGHT);
            statApiDTO.setFilters(Collections.singletonList(relation));
        });

        return relation;
    }

    private void setUpPowerCoolingCommodities(@Nonnull StatApiDTO power, @Nonnull StatApiDTO cooling, @Nonnull ApiId dcId,
            @Nonnull BaseApiDTO fabricDC, @Nonnull BaseApiDTO vCenterDC)
            throws OperationFailedException {
        fabricDC.setUuid("98");
        vCenterDC.setUuid("99");
        power.setRelatedEntity(vCenterDC);
        cooling.setRelatedEntity(fabricDC);
        when(uuidMapper.fromUuid(fabricDC.getUuid())).thenReturn(dcId);
        when(uuidMapper.fromUuid(vCenterDC.getUuid())).thenReturn(dcId);
    }
}
