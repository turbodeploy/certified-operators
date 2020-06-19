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
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodity;
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

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Test that {@link SearchEntityWriter} writes entities and groups to search_entity table with
 * correct fields as defined in {@link SearchEntityMetadataMapping}.
 */
public class SearchEntityWriterTest {

    private final DbEndpoint endpoint = spy(new ExtractorDbConfig().ingesterEndpoint());
    private final SearchEntityWriter writer = spy(new SearchEntityWriter(endpoint,
            Executors.newSingleThreadScheduledExecutor()));
    private final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    private final MultiStageTimer timer = mock(MultiStageTimer.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private List<Record> entitiesReplacerCapture;
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Entities.
     */
    private static final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
            .addCommoditySoldList(soldCommodity(CommodityType.VCPU, 300, 1000))
            .addCommoditySoldList(soldCommodity(CommodityType.VMEM, 1024, 4096))
            .build();

    private static final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
            .addCommoditySoldList(soldCommodity(CommodityType.CPU, 4000, 20000))
            .addCommoditySoldList(soldCommodity(CommodityType.MEM, 2048, 10240))
            .build();

    private static final TopologyEntityDTO dc = mkEntity(DATACENTER);

    private static final List<TopologyEntityDTO> ALL_ENTITIES = ImmutableList.copyOf(
            Arrays.asList(vm, pm, dc));

    /**
     * Groups
     */
    private static final Grouping g1 = mkGroup(GroupType.COMPUTE_HOST_CLUSTER, Arrays.asList(pm.getOid()));

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
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException {
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

    @Test
    public void testInsertEntities() throws SQLException, UnsupportedDialectException, IOException {
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
            final Map<String, Object> attrs = jsonString == null ? Collections.emptyMap() :
                    (Map<String, Object>)mapper.readValue(jsonString.toString(), Map.class);
            // verify common fields for entity
            verifyCommonFields(record, entityByOid.get(oid));

            // verify entity specific fields
            if (oid == vm.getOid()) {
                // commodity
                assertThat(attrs.get(SearchEntityMetadataMapping.COMMODITY_VCPU_USED.getJsonKeyName()), is(300.0));
                assertThat(attrs.get(SearchEntityMetadataMapping.COMMODITY_VCPU_UTILIZATION.getJsonKeyName()), is(0.3));
                assertThat(attrs.get(SearchEntityMetadataMapping.PRIMITIVE_GUEST_OS_TYPE.getJsonKeyName()), is("LINUX"));
                // related entity
                assertThat(attrs.get(SearchEntityMetadataMapping.RELATED_DATA_CENTER.getJsonKeyName()),
                        is(Collections.singletonList(dc.getDisplayName())));
            } else if (oid == pm.getOid()) {
                // commodity
                assertThat(attrs.get(SearchEntityMetadataMapping.COMMODITY_CPU_USED.getJsonKeyName()), is(4000.0));
                assertThat(attrs.get(SearchEntityMetadataMapping.COMMODITY_CPU_UTILIZATION.getJsonKeyName()), is(0.2));
                // related entity
                assertThat(attrs.get(SearchEntityMetadataMapping.RELATED_DATA_CENTER.getJsonKeyName()),
                        is(Collections.singletonList(dc.getDisplayName())));
            }
        }
    }

    @Test
    public void testInsertGroups() throws SQLException, UnsupportedDialectException {
        doReturn(Stream.of(g1)).when(dataProvider).getAllGroups();

        int n = writer.finish(dataProvider);
        // check the number of records written to db
        assertThat(n, is(1));
        assertThat(entitiesReplacerCapture.size(), is(1));

        Record record = entitiesReplacerCapture.get(0);

        assertThat(record.get(ENTITY_OID_AS_OID), is(g1.getId()));
        assertThat(record.get(ENTITY_NAME), is(g1.getDefinition().getDisplayName()));
        assertThat(record.get(ENTITY_TYPE_ENUM), is(
                EnumUtils.protoGroupTypeToDbType(g1.getDefinition().getType())));
        assertThat(record.get(NUM_ACTIONS), is(ACTION_COUNT_MAP.get(g1.getId())));
    }

    private void verifyCommonFields(Record record, TopologyEntityDTO entity) {
        assertThat(record.get(ENTITY_OID_AS_OID), is(entity.getOid()));
        assertThat(record.get(ENTITY_NAME), is(entity.getDisplayName()));
        assertThat(record.get(ENTITY_TYPE_ENUM), is(
                EnumUtils.protoIntEntityTypeToDbType(entity.getEntityType())));
        assertThat(record.get(ENVIRONMENT_TYPE_ENUM), is(
                EnumUtils.protoEnvironmentTypeToDbType(entity.getEnvironmentType())));
        assertThat(record.get(ENTITY_STATE_ENUM), is(
                EnumUtils.protoEntityStateToDbState(entity.getEntityState())));
        // action
        assertThat(record.get(NUM_ACTIONS), is(ACTION_COUNT_MAP.get(entity.getOid())));
    }
}
