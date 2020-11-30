package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.NUM_ACTIONS;
import static com.vmturbo.extractor.models.ModelDefinitions.SEVERITY_ENUM;
import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.severity.SeverityMapper;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity.Builder;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Unit tests for the {@link SearchActionWriter}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class SearchActionWriterTest {

    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDbConfig dbConfig;

    private WriterConfig writerConfig = ImmutableWriterConfig.builder()
            .lastSeenUpdateIntervalMinutes(1)
            .lastSeenAdditionalFuzzMinutes(1)
            .insertTimeoutSeconds(10)
            .populateScopeTable(true)
            .build();

    private DataProvider dataProvider = mock(DataProvider.class);

    private ExecutorService pool = mock(ExecutorService.class);

    private SearchActionWriter actionWriter;

    private List<Record> searchActionReplacerCapture;

    private MultiStageTimer timer = new MultiStageTimer(logger);

    /**
     * Common setup code before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink searchActionReplacerSink = mock(DslReplaceRecordSink.class);
        this.searchActionReplacerCapture = captureSink(searchActionReplacerSink, false);

        actionWriter = spy(new SearchActionWriter(dataProvider, endpoint, writerConfig, pool));

        doReturn(searchActionReplacerSink).when(actionWriter).getSearchActionReplacerSink(any(DSLContext.class));
        doAnswer(inv -> null).when(dataProvider).getTopologyGraph();
    }

    /**
     * Test the typical action writing cycle for search.
     *
     * @throws UnsupportedDialectException if the type of endpoint is unsupported
     * @throws InterruptedException if interrupted
     * @throws SQLException if there's a problem using the db endpoint
     */
    @Test
    public void testWriteActionsForSearch() throws UnsupportedDialectException, InterruptedException, SQLException {
        final long vmId = 11L;
        // mock actions
        ActionSpec.Builder actionSpec = ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                        .setId(111L)
                        .setInfo(ActionInfo.newBuilder()
                                .setMove(Move.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(vmId)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()));
        actionWriter.accept(ActionOrchestratorAction.newBuilder()
                .setActionId(actionSpec.getRecommendation().getId())
                .setActionSpec(actionSpec).build());
        // mock severities
        actionWriter.acceptSeverity(new SeverityMapper(ImmutableMap.of(vmId,
                EntitySeverity.newBuilder().setEntityId(vmId).setSeverity(ActionDTO.Severity.MAJOR).build())));
        // mock entities
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setOid(vmId)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        doReturn(new TopologyGraphCreator<Builder, SupplyChainEntity>()
                .addEntity(SupplyChainEntity.newBuilder(entityDTO))
                .build()).when(dataProvider).getTopologyGraph();
        // write
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);
        // verify search actions records
        assertThat(searchActionReplacerCapture.size(), is(1));
        assertThat(searchActionReplacerCapture.get(0).get(ENTITY_OID_AS_OID), is(vmId));
        assertThat(searchActionReplacerCapture.get(0).get(NUM_ACTIONS), is(1));
        assertThat(searchActionReplacerCapture.get(0).get(SEVERITY_ENUM), is(Severity.MAJOR));
    }

    /**
     * Test the action count for some special entity types or groups which should be expanded to
     * related entities when counting actions in the scope. It tests the following cases: region,
     * business account, group of regions, group of business accounts. For example: if vm1 is in
     * region1, and there are two actions: scale vm1, buy RI in region1. Then the action count for
     * region1 should be 2 although the scale action doesn't involve region1 in its action spec.
     *
     * @throws UnsupportedDialectException if the type of endpoint is unsupported
     * @throws InterruptedException if interrupted
     * @throws SQLException if there's a problem using the db endpoint
     */
    @Test
    public void testSearchActionCountForExpandedEntityTypes()
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final long action1 = 111L;
        final long action2 = 112L;
        final long vm1 = 11L;
        final long region1 = 21L;
        final long account1 = 31L;
        final long computeTier1 = 41L;
        final long regionGroup1 = 51L;
        final long billingFamily1 = 61L;

        // mock actions
        ActionSpec.Builder scaleActionSpec = ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                        .setId(action1)
                        .setInfo(ActionInfo.newBuilder()
                                .setScale(Scale.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(vm1)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()));
        ActionSpec.Builder buyRiActionSpec = ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                        .setId(action2)
                        .setInfo(ActionInfo.newBuilder()
                                .setBuyRi(BuyRI.newBuilder()
                                        .setBuyRiId(12345)
                                        .setMasterAccount(ActionEntity.newBuilder()
                                                .setId(account1)
                                                .setType(EntityType.BUSINESS_ACCOUNT_VALUE))
                                        .setRegion(ActionEntity.newBuilder()
                                                .setId(region1)
                                                .setType(EntityType.REGION_VALUE))
                                        .setComputeTier(ActionEntity.newBuilder()
                                                .setId(computeTier1)
                                                .setType(EntityType.COMPUTE_TIER_VALUE))))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()));

        // mock actions
        Stream.of(scaleActionSpec, buyRiActionSpec).forEach(actionSpec ->
                actionWriter.accept(ActionOrchestratorAction.newBuilder()
                        .setActionId(actionSpec.getRecommendation().getId())
                        .setActionSpec(actionSpec).build()));

        // mock entities
        TopologyEntityDTO vmDTO = TopologyEntityDTO.newBuilder()
                .setOid(vm1)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(region1)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                .build();
        TopologyEntityDTO regionDTO = TopologyEntityDTO.newBuilder()
                .setOid(region1)
                .setEntityType(EntityType.REGION_VALUE)
                .build();
        TopologyEntityDTO accountDTO = TopologyEntityDTO.newBuilder()
                .setOid(account1)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(vm1)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();
        doReturn(new TopologyGraphCreator<Builder, SupplyChainEntity>()
                .addEntity(SupplyChainEntity.newBuilder(vmDTO))
                .addEntity(SupplyChainEntity.newBuilder(regionDTO))
                .addEntity(SupplyChainEntity.newBuilder(accountDTO))
                .build()).when(dataProvider).getTopologyGraph();
        // mock groups
        final Long2ObjectMap<List<Long>> groupToLeafEntityIds = new Long2ObjectOpenHashMap<>();
        groupToLeafEntityIds.put(regionGroup1, Lists.newArrayList(region1));
        groupToLeafEntityIds.put(billingFamily1, Lists.newArrayList(account1));
        doReturn(groupToLeafEntityIds).when(dataProvider).getGroupToLeafEntities();

        final Map<Long, Integer> expectedActionCount = ImmutableMap.<Long, Integer>builder()
                .put(vm1, 1)
                .put(region1, 2)
                .put(account1, 2)
                .put(regionGroup1, 2)
                .put(billingFamily1, 2)
                .build();
        // mock related entities
        final Map<Long, Map<Integer, Set<Long>>> entityToRelated = ImmutableMap.of(
                region1, ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(vm1)),
                account1, ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(vm1))
        );
        doReturn(new SupplyChain(entityToRelated, true)).when(dataProvider).getSupplyChain();
        // write with full supply chain and verify
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);
        verifyActionCount(expectedActionCount);

        // write with partial supply chain and verify
        doReturn(new SupplyChain(entityToRelated, false)).when(dataProvider).getSupplyChain();
        searchActionReplacerCapture.clear();
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);
        verifyActionCount(expectedActionCount);
    }

    private void verifyActionCount(Map<Long, Integer> expectedCountByEntity) {
        // verify total number of records
        assertThat(searchActionReplacerCapture.size(), is(expectedCountByEntity.size()));
        final Map<Long, Record> recordsByEntity = searchActionReplacerCapture.stream()
                .collect(Collectors.toMap(record -> record.get(ENTITY_OID_AS_OID),
                        record -> record));
        expectedCountByEntity.forEach((id, count) -> {
            assertThat(recordsByEntity.get(id).get(NUM_ACTIONS), is(count));
        });
    }

    /**
     * Test the action count for ARM entities. Entity relationship is:
     *     businessApp1 --> businessTransaction1 --> service1 --> app1 --> vm1 --> host1
     *     vm2 --> host2
     * There are 2 actions: scale vm1, move vm2 from host2 to host1. For businessApp1, only
     * the first action should be counted.
     *
     * @throws UnsupportedDialectException if the type of endpoint is unsupported
     * @throws InterruptedException if interrupted
     * @throws SQLException if there's a problem using the db endpoint
     */
    @Test
    public void testSearchActionCountForARMEntities()
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final long action1 = 111L;
        final long action2 = 112L;
        final long vm1 = 11L;
        final long vm2 = 12L;
        final long host1 = 21L;
        final long host2 = 22L;
        final long businessApp1 = 31L;
        final long businessTransaction1 = 41L;
        final long service1 = 51L;
        final long app1 = 61L;

        // mock actions
        // scale vm1
        ActionSpec.Builder scaleActionSpec = ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                        .setId(action1)
                        .setInfo(ActionInfo.newBuilder()
                                .setScale(Scale.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(vm1)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()));
        // move vm2 from host2 to host1
        ActionSpec.Builder moveActionSpec = ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                        .setId(action2)
                        .setInfo(ActionInfo.newBuilder()
                                .setMove(Move.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(vm2)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                        .addChanges(ChangeProvider.newBuilder()
                                                .setSource(ActionEntity.newBuilder()
                                                        .setId(host2)
                                                        .setType(EntityType.PHYSICAL_MACHINE_VALUE))
                                                .setDestination(ActionEntity.newBuilder()
                                                        .setId(host1)
                                                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)))))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()));

        // mock actions
        Stream.of(scaleActionSpec, moveActionSpec).forEach(actionSpec ->
                actionWriter.accept(ActionOrchestratorAction.newBuilder()
                        .setActionId(actionSpec.getRecommendation().getId())
                        .setActionSpec(actionSpec).build()));

        // mock entities
        TopologyEntityDTO baDTO1 = entity(businessApp1, EntityType.BUSINESS_APPLICATION, businessTransaction1);
        TopologyEntityDTO btDTO1 = entity(businessTransaction1, EntityType.BUSINESS_TRANSACTION, service1);
        TopologyEntityDTO serviceDTO1 = entity(service1, EntityType.SERVICE, app1);
        TopologyEntityDTO appDTO1 = entity(app1, EntityType.APPLICATION_COMPONENT, vm1);
        TopologyEntityDTO vmDTO1 = entity(vm1, EntityType.VIRTUAL_MACHINE, host1);
        TopologyEntityDTO vmDTO2 = entity(vm2, EntityType.VIRTUAL_MACHINE, host2);
        TopologyEntityDTO hostDTO1 = entity(host1, EntityType.PHYSICAL_MACHINE);
        TopologyEntityDTO hostDTO2 = entity(host2, EntityType.PHYSICAL_MACHINE);
        doReturn(new TopologyGraphCreator<Builder, SupplyChainEntity>()
                .addEntity(SupplyChainEntity.newBuilder(baDTO1))
                .addEntity(SupplyChainEntity.newBuilder(btDTO1))
                .addEntity(SupplyChainEntity.newBuilder(serviceDTO1))
                .addEntity(SupplyChainEntity.newBuilder(appDTO1))
                .addEntity(SupplyChainEntity.newBuilder(vmDTO1))
                .addEntity(SupplyChainEntity.newBuilder(hostDTO1))
                .addEntity(SupplyChainEntity.newBuilder(vmDTO2))
                .addEntity(SupplyChainEntity.newBuilder(hostDTO2))
                .build()).when(dataProvider).getTopologyGraph();

        final Map<Long, Integer> expectedActionCount = ImmutableMap.<Long, Integer>builder()
                .put(businessApp1, 1)
                .put(businessTransaction1, 1)
                .put(service1, 1)
                .put(vm1, 1)
                .put(host1, 1)
                .put(vm2, 1)
                .put(host2, 1)
                .build();

        // mock that related entities are FULLY calculated
        final Map<Long, Map<Integer, Set<Long>>> entityToRelated = new HashMap<>();
        Stream.of(businessApp1, businessTransaction1, service1, app1, vm1, host1).forEach(oid -> {
            entityToRelated.put(oid, ImmutableMap.<Integer, Set<Long>>builder()
                    .put(EntityType.BUSINESS_APPLICATION_VALUE, Sets.newHashSet(businessApp1))
                    .put(EntityType.BUSINESS_TRANSACTION_VALUE, Sets.newHashSet(businessTransaction1))
                    .put(EntityType.SERVICE_VALUE, Sets.newHashSet(service1))
                    .put(EntityType.APPLICATION_COMPONENT_VALUE, Sets.newHashSet(app1))
                    .put(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(vm1))
                    .put(EntityType.PHYSICAL_MACHINE_VALUE, Sets.newHashSet(host1))
                    .build());
        });
        doReturn(new SupplyChain(entityToRelated, true)).when(dataProvider).getSupplyChain();

        // write with full supply chain and verify
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);
        verifyActionCount(expectedActionCount);

        // write with partial supply chain and verify
        searchActionReplacerCapture.clear();
        doReturn(new SupplyChain(entityToRelated, false)).when(dataProvider).getSupplyChain();
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);
        verifyActionCount(expectedActionCount);
    }

    private TopologyEntityDTO entity(long oid, EntityType entityType, long... providers) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber());
        for (long provider : providers) {
            builder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(provider));
        }
        return builder.build();
    }
}
