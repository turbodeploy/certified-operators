package com.vmturbo.common.protobuf.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import com.google.protobuf.MessageOrBuilder;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest.Builder;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequestOrBuilder;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests of {@link ProtobufSummarizers} class.
 */
public class ProtobufSummarizersTest {

    /**
     * Apply the default summarizer to a simple protobuf message and make sure we get the correct
     * result.
     */
    @Test
    public void testStandardSummaryWorks() {
        // make sure we get the non-specialized overload, even if an overload for this message
        // type is eventually defined, by declaring it as MessageOrBuilder
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setDisplayName("sample entity")
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                .build())
                        .setCapacity(1.0)
                        .build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.MEM_VALUE)
                                .build())
                        .setCapacity(2.0)
                        .build());
        // check both as an un-built builder and as the built message
        for (MessageOrBuilder mOrB : Arrays.asList(entity, (MessageOrBuilder)entity.build())) {
            assertThat(ProtobufSummarizers.DEFAULT_SUMMARIZER.summarize(mOrB), is(
                    "{\"entity_type\":10,\"oid\":1,"
                            + "\"display_name\":\"sample entity\","
                            + "\"commodity_sold_list\":["
                            + "{\"commodityType\":{\"type\":40},\"capacity\":1.0},"
                            + "{\"commodityType\":{\"type\":21},\"capacity\":2.0}"
                            + "]}"));
        }
    }

    /**
     * Test that the entities elision for {@link GetAveragedEntityStatsRequest} works as expected.
     */
    @Test
    public void testGetAveragedEntityStatsRequestSummarizer() {
        final Builder builder = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder()
                        .addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(StringConstants.CPU)
                                .build()));
        final String filter = "\"filter\":{\"commodityRequests\":[{\"commodityName\":\"CPU\"}]}";
        final ProtobufSummarizer<GetAveragedEntityStatsRequestOrBuilder> summarizer
                = ProtobufSummarizers.GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER;
        assertThat(summarizer.summarize(builder), is("{" + filter + "}"));
        builder.addEntities(1L);
        assertThat(summarizer.summarize(builder), is("{\"entities\":[1]," + filter + "}"));
        builder.addEntities(2L);
        assertThat(summarizer.summarize(builder), is("{\"entities\":[1,2]," + filter + "}"));
        builder.addEntities(3L);
        assertThat(summarizer.summarize(builder), is("{\"entities\":[1,2,...+1]," + filter + "}"));
        builder.addAllEntities(Arrays.asList(4L, 5L, 6L));
        assertThat(summarizer.summarize(builder), is("{\"entities\":[1,2,...+4]," + filter + "}"));
    }

    /**
     * Test that the summarizer registry returns/uses the correct summarizer when a registered
     * summarizer applies to the supplied message/builder instances/type.
     */
    @Test
    public void testSummarizerRegistryForRegisteredType() {
        assertThat(ProtobufSummarizers.get(GetAveragedEntityStatsRequest.class),
                is(ProtobufSummarizers.GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(GetAveragedEntityStatsRequest.Builder.class),
                is(ProtobufSummarizers.GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(GetAveragedEntityStatsRequest.getDefaultInstance()),
                is(ProtobufSummarizers.GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(GetAveragedEntityStatsRequest.newBuilder()),
                is(ProtobufSummarizers.GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER));
        GetAveragedEntityStatsRequest.Builder builder = GetAveragedEntityStatsRequest.newBuilder()
                .addEntities(1L);
        assertThat(ProtobufSummarizers.summarize(builder), is("{\"entities\":[1]}"));
        assertThat(ProtobufSummarizers.summarize(builder.build()), is("{\"entities\":[1]}"));
    }

    /**
     * Test that the summarizer registry returns/uses the defualt summarizer when no registered
     * summarizer applies to the supplied message/builder instances/type.
     */
    public void testSummarizerRegistryForUnregisteredType() {
        assertThat(ProtobufSummarizers.get(StatsFilter.class),
                is(ProtobufSummarizers.DEFAULT_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(StatsFilter.Builder.class),
                is(ProtobufSummarizers.DEFAULT_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(StatsFilter.getDefaultInstance()),
                is(ProtobufSummarizers.DEFAULT_SUMMARIZER));
        assertThat(ProtobufSummarizers.get(StatsFilter.newBuilder()),
                is(ProtobufSummarizers.DEFAULT_SUMMARIZER));
        StatsFilter.Builder builder = StatsFilter.newBuilder()
                .setStartDate(1L);
        assertThat(ProtobufSummarizers.summarize(builder), is("{\"start_date\":1}"));
        assertThat(ProtobufSummarizers.summarize(builder.build()), is("{\"start_date\":1}"));
    }
}
