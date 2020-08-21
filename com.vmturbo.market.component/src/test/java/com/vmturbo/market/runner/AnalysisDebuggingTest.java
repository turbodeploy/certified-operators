package com.vmturbo.market.runner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.market.MarketDebug.AnalysisInput;
import com.vmturbo.common.protobuf.market.MarketDebug.GetAnalysisInfoResponse;
import com.vmturbo.common.protobuf.market.MarketDebugREST;
import com.vmturbo.common.protobuf.market.MarketDebugREST.MarketDebugServiceController.MarketDebugServiceResponse;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.rpc.MarketDebugRpcService;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * This is a test to run an analysis based on a {@link GetAnalysisInfoResponse} saved from a call to
 * {@link MarketDebugRpcService}.
 *
 * To run the test, you need to be in a development environment (e.g. your local environment)
 * with the grpc.debug.services.enabled system property set to true. Then:
 *     1) Enable analytics collection
 *
 *     curl -X POST "http://localhost:8087/MarketDebugService/controlAnalysisCollection" -H  "accept: application/json;charset=UTF-8" -H  "content-type: application/json;charset=UTF-8" -d "{  \"enable\": true}"
 *
 *     2) Broadcast the topology/run the plan you want to analyze.
 *     3) Once the plan is complete (look at the logs in the market component, or just wait),
 *        query the MarketDebugService and save the output JSON to a file.
 *
 *     curl -X POST "http://localhost:8087/MarketDebugService/getAnalysisInfo" -H  "accept: application/json;charset=UTF-8" -H  "content-type: application/json;charset=UTF-8" -d "{  \"latest_realtime\": true}"
 *
 *     4) Paste the path to your response file into the {@link AnalysisDebuggingTest#PATH_TO_YOUR_RESPONSE_FILE}
 *        variable, and modify {@link AnalysisDebuggingTest#testSavedAnalysis()} as
 *        needed. You can use the various utility methods to help debug - and feel free to add your own.
 *
 * Note: If you want source attachments for the market code, you can add the maven-source-plugin
 * to com.vmturbo.analysis/platform.analysis/pom.xml and re-build:
 *        <plugin>
 *         <groupId>org.apache.maven.plugins</groupId>
 *         <artifactId>maven-source-plugin</artifactId>
 *         <executions>
 *           <execution>
 *             <id>attach-sources</id>
 *             <goals>
 *               <goal>jar</goal>
 *             </goals>
 *           </execution>
 *         </executions>
 *       </plugin>
 */
public class AnalysisDebuggingTest {

    private static final String PATH_TO_YOUR_RESPONSE_FILE = "";

    private static final Predicate<EntityAnalysis> ALL_ENTITIES = entityAnalysis -> true;

    private static final Predicate<EntityAnalysis> ORIGINAL_AND_PROJECTED = entityAnalysis ->
            entityAnalysis.originalEntity.isPresent() && entityAnalysis.projectedEntity.isPresent();

    private static final Predicate<EntityAnalysis> HAS_ACTIONS = entityAnalysis -> !entityAnalysis.relatedActions.isEmpty();

    private static final Predicate<EntityAnalysis> DIFF_MEM_USAGE_NO_ACTIONS =
            ORIGINAL_AND_PROJECTED.and(HAS_ACTIONS.negate()).and(entityAnalysis ->
                    !commoditiesEquivalent(entityAnalysis, CommodityType.MEM));

    private GroupServiceMole groupServiceMole = spy(new GroupServiceMole());

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);

    private InitialPlacementFinder initialPlacementFinder =
            mock(InitialPlacementFinder.class);

    private final TopologyInfo topoInfo = TopologyInfo.newBuilder().setTopologyContextId(1000l).build();
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceMole);

    /**
     * Returns a predicate matching the specified entities.
     */
    private static Predicate<EntityAnalysis> entityOids(final long ... oids) {
        final Set<Long> set = new HashSet<>();
        for (long oid : oids) {
            set.add(oid);
        }
        return entityAnalysis -> set.contains(entityAnalysis.oid);
    }

    @Before
    public void setup() {
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        IdentityGenerator.initPrefix(0);
    }

    @Test
    @Ignore
    public void testSavedAnalysis() throws FileNotFoundException {
        final GetAnalysisInfoResponse response =
                parseResponseFile(PATH_TO_YOUR_RESPONSE_FILE);
        final Analysis analysis = analysisFromInput(response.getInput());
        analysis.execute();
    }

    /**
     * Organize the results of the analysis by entity.
     *
     * @param analysis The analysis to organize.
     * @param analysisPredicate A predicate to apply to the entities to include in the organized
     *                          result. See the predicates at the top of the file.
     * @return A map from oid -> {@link EntityAnalysis} for that oid for each entity in the
     *         input {@link Analysis} that matches the predicate.
     */
    @Nonnull
    private Map<Long, EntityAnalysis> organizeByEntity(@Nonnull final Analysis analysis,
                                               Predicate<EntityAnalysis> analysisPredicate) {
        final Map<Long, TopologyEntityDTO> originalEntities = analysis.getTopology();
        final Map<Long, ProjectedTopologyEntity> projectedEntities = analysis.getProjectedTopology().get().stream()
            .collect(Collectors.toMap(entity -> entity.getEntity().getOid(), Function.identity()));
        final Map<Long, Set<Action>> relatedActions = new HashMap<>();
        for (Action action : analysis.getActionPlan().get().getActionList()) {
            try {
                ActionDTOUtil.getInvolvedEntityIds(action).forEach(involvedEntityId -> {
                    final Set<Action> actionsForEntity = relatedActions.computeIfAbsent(involvedEntityId, k -> new HashSet<>());
                    actionsForEntity.add(action);
                });
            } catch (UnsupportedActionException e) {
                // bad
            }
        }

        final Map<Long, EntityAnalysis> entityAnalysisMap = new HashMap<>();
        originalEntities.forEach((entityId, entity) -> {
            assertFalse(entityAnalysisMap.containsKey(entityId));
            final EntityAnalysis eAnalysis = new EntityAnalysis(entityId,
                    Optional.of(entity),
                    Optional.ofNullable(projectedEntities.get(entityId)),
                    relatedActions.getOrDefault(entityId, Collections.emptySet()));
            if (analysisPredicate.test(eAnalysis)) {
                entityAnalysisMap.put(entityId, eAnalysis);
            }
        });

        Sets.difference(projectedEntities.keySet(), originalEntities.keySet()).forEach(projectedOnlyId -> {
            assertFalse(entityAnalysisMap.containsKey(projectedOnlyId));
            final EntityAnalysis eAnalysis = new EntityAnalysis(projectedOnlyId,
                    Optional.empty(),
                    Optional.of(projectedEntities.get(projectedOnlyId)),
                    relatedActions.getOrDefault(projectedOnlyId, Collections.emptySet()));
            if (analysisPredicate.test(eAnalysis)) {
                entityAnalysisMap.put(projectedOnlyId, eAnalysis);
            }
        });
        return entityAnalysisMap;
    }

    @Nonnull
    private List<Action> getActionsForEntity(final long oid, Analysis analysis) {
        assertThat(analysis.getState(), is(AnalysisState.SUCCEEDED));
        return analysis.getActionPlan().get().getActionList().stream()
                .filter(action -> {
                    try {
                        return ActionDTOUtil.getInvolvedEntityIds(action).contains(oid);
                    } catch (UnsupportedActionException e) {
                        return false;
                    }
                }).collect(Collectors.toList());
    }

    @Nonnull
    private Optional<TopologyEntityDTO> getOriginalEntity(final long oid, @Nonnull final Analysis analysis) {
        assertThat(analysis.getState(), is(AnalysisState.SUCCEEDED));
        return Optional.ofNullable(analysis.getTopology().get(oid));
    }

    @Nonnull
    private Optional<ProjectedTopologyEntity> getProjectedEntity(final long oid, @Nonnull final Analysis analysis) {
        assertThat(analysis.getState(), is(AnalysisState.SUCCEEDED));
        return analysis.getProjectedTopology().get().stream()
                .filter(entity -> entity.getEntity().getOid() == oid)
                .findAny();
    }

    @Nonnull
    private GetAnalysisInfoResponse parseResponseFile(@Nonnull final String path) throws FileNotFoundException {
        final JsonReader jsonReader = new JsonReader(new FileReader(path));
        final Gson gson = new Gson();
        final MarketDebugServiceResponse<MarketDebugREST.GetAnalysisInfoResponse> serializedResponse =
                gson.fromJson(jsonReader, new TypeToken<MarketDebugServiceResponse<MarketDebugREST.GetAnalysisInfoResponse>>(){}.getType());
        Assert.assertNotNull(serializedResponse.response);

        return serializedResponse.response.toProto();
    }

    @Nonnull
    private Analysis analysisFromInput(@Nonnull final AnalysisInput analysisInput) {
        AnalysisConfig.Builder analysisConfig = AnalysisConfig.newBuilder(analysisInput.getQuoteFactor(),
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            analysisInput.getSuspensionThrottlingPerCluster() ? SuspensionsThrottlingConfig.CLUSTER : SuspensionsThrottlingConfig.DEFAULT,
                    analysisInput.getSettingsMap())
                .setIncludeVDC(analysisInput.getIncludeVdc())
                .setUseQuoteCacheDuringSNM(analysisInput.getUseQuoteCacheDuringSnm())
                .setReplayProvisionsForRealTime(analysisInput.getReplayProvisionsForRealTime())
                .setRightsizeLowerWatermark(analysisInput.getRightSizeLowerWatermark())
                .setRightsizeUpperWatermark(analysisInput.getRightSizeUpperWatermark());
        if (analysisInput.hasMaxPlacementsOverride()) {
            analysisConfig.setMaxPlacementsOverride(Optional.of(analysisInput.getMaxPlacementsOverride()));
        }

        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(mock(TopologyEntityCloudTopology.class));
        when(cloudCostCalculatorFactory.newCalculator(topoInfo, any())).thenReturn(cloudCostCalculator);

        final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
        when(priceTableFactory.newPriceTable(any(), eq(CloudCostData.empty()))).thenReturn(mock(MarketPriceTable.class));
        final WastedFilesAnalysisFactory wastedFilesAnalysisFactory =
            mock(WastedFilesAnalysisFactory.class);
        final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
                mock(BuyRIImpactAnalysisFactory.class);
        final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService = mock(MigratedWorkloadCloudCommitmentAnalysisService.class);
        doNothing().when(migratedWorkloadCloudCommitmentAnalysisService).startAnalysis(anyLong(), any(), anyList());


        final Analysis analysis = new Analysis(analysisInput.getTopologyInfo(),
            Sets.newHashSet(analysisInput.getEntitiesList()),
            new GroupMemberRetriever(GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel())),
            Clock.systemUTC(),
            analysisConfig.build(), cloudTopologyFactory, cloudCostCalculatorFactory, priceTableFactory,
            wastedFilesAnalysisFactory, buyRIImpactAnalysisFactory, tierExcluderFactory,
                mock(AnalysisRICoverageListener.class), consistentScalingHelperFactory, initialPlacementFinder,
                migratedWorkloadCloudCommitmentAnalysisService);
        return analysis;
    }

    private static boolean commoditiesEquivalent(EntityAnalysis entityAnalysis, CommodityType type) {
        final Optional<CommoditySoldDTO> originalSold = entityAnalysis.originalEntity.get().getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType() == type.getNumber())
                .findAny();
        final Optional<CommoditySoldDTO> projectedSold = entityAnalysis.projectedEntity.get().getEntity().getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType() == type.getNumber())
                .findAny();
        if (originalSold.isPresent() && projectedSold.isPresent()) {
            return numEquivalent(originalSold.get().getUsed(), projectedSold.get().getUsed()) &&
                    numEquivalent(originalSold.get().getCapacity(), projectedSold.get().getCapacity()) &&
                    numEquivalent(originalSold.get().getPeak(), projectedSold.get().getPeak()) &&
                    numEquivalent(originalSold.get().getReservedCapacity(), projectedSold.get().getReservedCapacity());
        } else if (originalSold.isPresent() || projectedSold.isPresent()) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean numEquivalent(double a, double b) {
        return Math.abs(a - b) < 0.5;
    }

    /**
     * The effects of an analysis on a single entity in the topology.
     */
    private static class EntityAnalysis {
        public final long oid;
        public final Optional<TopologyEntityDTO> originalEntity;
        public final Optional<ProjectedTopologyEntity> projectedEntity;
        public final Set<Action> relatedActions;

        private EntityAnalysis(final long oid,
                               final Optional<TopologyEntityDTO> originalEntity,
                               final Optional<ProjectedTopologyEntity> projectedEntity,
                               final Set<Action> relatedActions) {
            this.oid = oid;
            this.originalEntity = originalEntity;
            this.projectedEntity = projectedEntity;
            this.relatedActions = relatedActions;
        }
    }
}
