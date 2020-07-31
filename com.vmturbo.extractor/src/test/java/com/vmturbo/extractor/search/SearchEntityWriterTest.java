package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_STATE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.NUM_ACTIONS;
import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkGroup;
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodityWithHistoricalUtilization;
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodityWithPercentile;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATACENTER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Test that {@link SearchEntityWriter} writes entities and groups to search_entity table with
 * correct fields as defined in {@link SearchMetadataMapping}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class SearchEntityWriterTest {

    @Autowired
    private ExtractorDbConfig dbConfig;

    private SearchEntityWriter writer;
    private final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    private final MultiStageTimer timer = mock(MultiStageTimer.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private List<Record> entitiesReplacerCapture;
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Entities.
     */
    private static final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
            .addCommoditySoldList(soldCommodityWithPercentile(CommodityType.VCPU, 300, 1000, 0.25))
            .addCommoditySoldList(soldCommodityWithPercentile(CommodityType.VMEM, 1024, 4096, 0.25))
            .build();

    private static final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
            .addCommoditySoldList(soldCommodityWithHistoricalUtilization(CommodityType.CPU, 4000, 20000, 0.20))
            .addCommoditySoldList(soldCommodityWithHistoricalUtilization(CommodityType.MEM, 2048, 10240, 0.20))
            .build();

    private static final TopologyEntityDTO dc = mkEntity(DATACENTER);

    private static final List<TopologyEntityDTO> ALL_ENTITIES = ImmutableList.copyOf(
            Arrays.asList(vm, pm, dc));

    /**
     * Groups.
     */
    private static final Grouping g1 = mkGroup(GroupType.COMPUTE_HOST_CLUSTER,
            Collections.singletonList(pm.getOid()));

    private static final Map<Long, Integer> ACTION_COUNT_MAP = new ImmutableMap.Builder<Long, Integer>()
            .put(vm.getOid(), 3)
            .put(pm.getOid(), 8)
            .put(dc.getOid(), 0)
            .put(g1.getId(), 10)
            .build();


    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        this.writer = spy(new SearchEntityWriter(endpoint,
                Executors.newSingleThreadScheduledExecutor()));
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesReplacerSink = mock(DslReplaceRecordSink.class);
        this.entitiesReplacerCapture = captureSink(entitiesReplacerSink, false);
        doReturn(entitiesReplacerSink).when(writer).getEntityReplacerSink(any(DSLContext.class));
        doAnswer(a -> Stream.empty()).when(dataProvider).getAllGroups();
        doReturn(Collections.singletonList(dc.getDisplayName())).when(dataProvider)
                .getRelatedEntityNames(vm.getOid(), DATACENTER);
        doReturn(Collections.singletonList(dc.getDisplayName())).when(dataProvider)
                .getRelatedEntityNames(pm.getOid(), DATACENTER);
        ACTION_COUNT_MAP.forEach((oid, count) ->
                doReturn(count).when(dataProvider).getActionCount(oid));
    }

    /**
     * Tests that we can propertly insert entities into the search_entities table.
     *
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the DB endpoint is misconfigured
     * @throws IOException                 for IO exceptions
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testInsertEntities() throws SQLException, UnsupportedDialectException, IOException, InterruptedException {
        final Consumer<TopologyEntityDTO> entityConsumer = writer.startTopology(
                info, ExtractorTestUtil.config, timer);
        ALL_ENTITIES.forEach(entityConsumer);
        int n = writer.finish(dataProvider);
        // check the number of records written to db
        assertThat(n, is(ALL_ENTITIES.size()));
        assertThat(entitiesReplacerCapture.size(), is(ALL_ENTITIES.size()));

        final Map<Long, TopologyEntityDTO> entityByOid = ALL_ENTITIES.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, t -> t));
        // check all fields in db record
        for (Record record : entitiesReplacerCapture) {
            long oid = record.get(ENTITY_OID_AS_OID);
            JsonString jsonString = record.get(ATTRS);
            final Map<String, Object> attrs = jsonString == null ? Collections.emptyMap()
                    : (Map<String, Object>)mapper.readValue(jsonString.toString(), Map.class);
            // verify common fields for entity
            verifyCommonFields(record, entityByOid.get(oid));

            // verify entity specific fields
            if (oid == vm.getOid()) {
                // commodity
                assertThat(attrs.get(SearchMetadataMapping.COMMODITY_VCPU_USED.getJsonKeyName()), is(300.0));
                assertThat(attrs.get(SearchMetadataMapping.COMMODITY_VCPU_PERCENTILE_UTILIZATION.getJsonKeyName()), is(0.25));
                assertThat(attrs.get(SearchMetadataMapping.PRIMITIVE_GUEST_OS_TYPE.getJsonKeyName()), is("LINUX"));
                // related entity
                assertThat(attrs.get(SearchMetadataMapping.RELATED_DATA_CENTER.getJsonKeyName()),
                        is(Collections.singletonList(dc.getDisplayName())));
            } else if (oid == pm.getOid()) {
                // commodity
                assertThat(attrs.get(SearchMetadataMapping.COMMODITY_CPU_USED.getJsonKeyName()), is(4000.0));
                assertThat(attrs.get(SearchMetadataMapping.COMMODITY_CPU_HISTORICAL_UTILIZATION.getJsonKeyName()), is(0.2));
                // related entity
                assertThat(attrs.get(SearchMetadataMapping.RELATED_DATA_CENTER.getJsonKeyName()),
                        is(Collections.singletonList(dc.getDisplayName())));
            }
        }
    }

    /**
     * Test that wec can insert group data into the search_entities table.
     *
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the DB endpoint is misconfigured
     * @throws InterruptedException        if interrupetd
     */
    @Test
    public void testInsertGroups() throws SQLException, UnsupportedDialectException, InterruptedException {
        doReturn(Stream.of(g1)).when(dataProvider).getAllGroups();

        int n = writer.finish(dataProvider);
        // check the number of records written to db
        assertThat(n, is(1));
        assertThat(entitiesReplacerCapture.size(), is(1));

        Record record = entitiesReplacerCapture.get(0);

        assertThat(record.get(ENTITY_OID_AS_OID), is(g1.getId()));
        assertThat(record.get(ENTITY_NAME), is(g1.getDefinition().getDisplayName()));
        assertThat(record.get(ENTITY_TYPE_ENUM), is(
                GroupTypeUtils.protoToDb(g1.getDefinition().getType())));
        assertThat(record.get(NUM_ACTIONS), is(ACTION_COUNT_MAP.get(g1.getId())));
    }

    private void verifyCommonFields(Record record, TopologyEntityDTO entity) {
        assertThat(record.get(ENTITY_OID_AS_OID), is(entity.getOid()));
        assertThat(record.get(ENTITY_NAME), is(entity.getDisplayName()));
        assertThat(record.get(ENTITY_TYPE_ENUM), is(
                SearchEntityTypeUtils.protoIntToDb(entity.getEntityType())));
        assertThat(record.get(ENVIRONMENT_TYPE_ENUM), is(
                EnvironmentTypeUtils.protoToDb(entity.getEnvironmentType())));
        assertThat(record.get(ENTITY_STATE_ENUM), is(
                EntityStateUtils.protoToDb(entity.getEntityState())));
        // action
        assertThat(record.get(NUM_ACTIONS), is(ACTION_COUNT_MAP.get(entity.getOid())));
    }
}
