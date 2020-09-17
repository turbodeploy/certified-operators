package com.vmturbo.extractor.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.EntityMetricWriter;
import com.vmturbo.extractor.topology.ITopologyWriter;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Utility class with stuff that's used both topology and model tests.
 */
public class ExtractorTestUtil {

    private ExtractorTestUtil() {
    }

    /**
     * WriterConfig used for tests.
     */
    public static final WriterConfig config = ImmutableWriterConfig.builder()
            .addAllReportingCommodityWhitelist(
                    Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
                            .map(CommodityType::getNumber)
                            .collect(Collectors.toList()))
            .insertTimeoutSeconds(60)
            .lastSeenAdditionalFuzzMinutes(10)
            .lastSeenUpdateIntervalMinutes(20)
            .unaggregatedCommodities(Constants.UNAGGREGATED_KEYED_COMMODITY_TYPES)
            .build();

    /**
     * Utility to run some entities through an an {@link ITopologyWriter}.
     */
    public static class EntitiesProcessor {
        private final ITopologyWriter writer;
        private final TopologyInfo info;
        private final WriterConfig config;
        final List<TopologyEntityDTO> entities = new ArrayList<>();

        private EntitiesProcessor(ITopologyWriter writer, TopologyInfo info, WriterConfig config) {
            this.writer = writer;
            this.info = info;
            this.config = config;
        }

        /**
         * Create a new processor instance.
         *
         * @param writer writer to receive entities
         * @param info   topology info
         * @param config writer config
         * @return this entities processor
         */
        public static EntitiesProcessor of(
                final EntityMetricWriter writer, final TopologyInfo info, final WriterConfig config) {
            return new EntitiesProcessor(writer, info, config);
        }

        /**
         * Add one or more entities to the list of entities to be processed.
         *
         * @param entities entities to process
         * @return this entities processor
         */
        public EntitiesProcessor process(TopologyEntityDTO... entities) {
            this.entities.addAll(Arrays.asList(entities));
            return this;
        }

        /**
         * Add a collection of entities to the list of entities to be processed.
         *
         * @param entities entities to process
         * @return this entities processor
         */
        public EntitiesProcessor process(Collection<TopologyEntityDTO> entities) {
            this.entities.addAll(entities);
            return this;
        }

        /**
         * Send all the supplied entities through the writer, and then invoke finish processing.
         *
         * @param dataProvider data provider instance
         * @return number of entities processed
         * @throws UnsupportedDialectException if db endpoint is malformed
         * @throws SQLException                if there's an SQL execution
         * @throws IOException                 if there's an IO problem
         * @throws InterruptedException        if interrupted
         */
        public int finish(DataProvider dataProvider)
                throws UnsupportedDialectException, SQLException, IOException, InterruptedException {
            final Consumer<TopologyEntityDTO> consumer =
                    writer.startTopology(info, config, new MultiStageTimer(null));
            entities.forEach(consumer);
            return writer.finish(dataProvider);
        }
    }

}
