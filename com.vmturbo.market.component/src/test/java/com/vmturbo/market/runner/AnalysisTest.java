package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.market.runner.Analysis.AnalysisState;

/**
 * Unit tests for {@link Analysis}.
 */
public class AnalysisTest {

    private long topologyContextId = 1111;
    private long topologyId = 2222;
    private TopologyType topologyType = TopologyType.PLAN;
    private Set<TopologyEntityDTO> EMPTY = ImmutableSet.of();

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setTopologyType(topologyType)
            .build();

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    private final SettingServiceMole testSettingService =
                 spy(new SettingServiceMole());
    private SettingServiceBlockingStub settingServiceClient;

    private final Clock mockClock = mock(Clock.class);
    private static final Instant START_INSTANT = Instant.EPOCH.plus(90, ChronoUnit.MINUTES);
    private static final Instant END_INSTANT = Instant.EPOCH.plus(100, ChronoUnit.MINUTES);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
                     testSettingPolicyService, testSettingService);

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
        settingServiceClient = getSettingService(10.0f);

        when(mockClock.instant())
            .thenReturn(START_INSTANT)
            .thenReturn(END_INSTANT);
    }

    /*
     * Set rateOfResize with given value and return settings.
     */
    private SettingServiceBlockingStub getSettingService(float resizeValue) {
        SettingServiceBlockingStub settingServiceClient = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(testSettingService.getGlobalSetting(any()))
            .thenReturn(GetGlobalSettingResponse.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(
                        GlobalSettingSpecs.RateOfResize.getSettingName())
                    .setNumericSettingValue(
                        SettingDTOUtil.createNumericSettingValue(resizeValue)))
                .build());
        return settingServiceClient;
    }

    /**
     * Test the {@link Analysis} constructor.
     */
    @Test
    public void testConstructor() {
        Analysis analysis  = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
                .setTopologyInfo(topologyInfo)
                .setIncludeVDC(true)
                .setSettingsServiceClient(settingServiceClient)
                .build();
        assertEquals(topologyContextId, analysis.getContextId());
        assertEquals(topologyId, analysis.getTopologyId());
        assertEquals(EMPTY, analysis.getTopology());
        assertEquals(Instant.EPOCH, analysis.getStartTime());
        assertEquals(Instant.EPOCH, analysis.getCompletionTime());
    }

    /**
     * Test the {@link Analysis#execute} method.
     */
    @Test
    public void testExecute() {
        Analysis analysis  = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
                .setTopologyInfo(topologyInfo)
                .setIncludeVDC(true)
                .setClock(mockClock)
                .setSettingsServiceClient(settingServiceClient)
                .build();
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(analysis.getState(), AnalysisState.SUCCEEDED);
        assertEquals(START_INSTANT, analysis.getStartTime());
        assertEquals(END_INSTANT, analysis.getCompletionTime());

        assertTrue(analysis.getActionPlan().isPresent());
        assertTrue(analysis.getProjectedTopology().isPresent());
        assertTrue(analysis.getPriceIndexMessage().isPresent());
    }

    /**
     * Test the {@link Analysis#execute} method for a failed run.
     */
    @Test
    public void testFailedAnalysis() {
        Set<TopologyEntityDTO> set = Sets.newHashSet(buyer());
        Analysis analysis  = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
            .setIncludeVDC(true)
            .setTopologyDTOs(set)
                // RateOfResize negative to throw exception
                .setSettingsServiceClient(getSettingService(-1))
            .setClock(mockClock)
            .build();
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(AnalysisState.FAILED, analysis.getState());
        assertNotNull(analysis.getErrorMsg());

        assertFalse(analysis.getActionPlan().isPresent());
        assertFalse(analysis.getProjectedTopology().isPresent());
        assertFalse(analysis.getPriceIndexMessage().isPresent());
    }

    /**
     * A buyer that buys a negative amount (and therefore causes a failure).
     * @return a buyer
     */
    private TopologyEntityDTO buyer() {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(1000)
            .setOid(7)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(10)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder().setType(1).build())
                    .setUsed(-1)))
            .build(); // buyer
    }

    /**
     * Test that invoking {@link Analysis#execute} multiple times throws an exception.
     */
    @Test
    public void testTwoExecutes() {
        Analysis analysis  = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
                .setTopologyInfo(topologyInfo)
                .setIncludeVDC(true)
                .setSettingsServiceClient(settingServiceClient)
                .build();
        boolean first = analysis.execute();
        boolean second = analysis.execute();
        assertTrue(first);
        assertFalse(second);
    }

    @Test
    public void testActionPlanTimestamps() {
        Analysis analysis  = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
            .setTopologyInfo(topologyInfo)
            .setIncludeVDC(true)
            .setSettingsServiceClient(settingServiceClient)
            .setClock(mockClock)
            .build();

        analysis.execute();
        final ActionPlan actionPlan = analysis.getActionPlan().get();
        assertEquals(actionPlan.getAnalysisStartTimestamp(), START_INSTANT.toEpochMilli());
        assertEquals(actionPlan.getAnalysisCompleteTimestamp(), END_INSTANT.toEpochMilli());
    }
}
