package com.vmturbo.repository.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Utility class to help measure topology sizes from dumped TopologyEntityDTO files.
 */
public class LiveTopologyStoreMeasurer {

    private static final Logger logger = LogManager.getLogger();

    private void doForBinaryEntity(String filePath, Consumer<TopologyEntityDTO> consumer) throws IOException {
        File file = new File(filePath);
        MutableInt lineCnt = new MutableInt(0);
        try (InputStream is = FileUtils.openInputStream(file)) {
            try {
                while (true) {
                    TopologyEntityDTO e = TopologyEntityDTO.parseDelimitedFrom(is);
                    if (e == null) {
                        break;
                    }
                    lineCnt.increment();
                    consumer.accept(e);
                    logger.info("Processed {}", lineCnt);
                }
            } catch (IOException e) {
                // Noop.
            }
        }
    }

    private void doForJsonEntity(String filePath, Consumer<TopologyEntityDTO> consumer) throws IOException {
        int lineCnt = 0;
        final BufferedReader reader = new BufferedReader(new FileReader(filePath));
        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();
            consumer.accept(entity);
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }
    }

    /**
     * Read the topology entity DTOs from a file, and construct a {@link SourceRealtimeTopology}.
     *
     * @param inputFile The file to read topologies from. Could be a binary file (TopologyEntityDTOs,
     *                  written in delimited fashion) or a JSON file (one DTO per line).
     * @param entityConsumer Additional consumer for each entity in the file. Does not affect the
     *                       topology construction.
     * @return The {@link SourceRealtimeTopology}.
     * @throws IOException If there is an error reading from the file.
     */
    @Nonnull
    public SourceRealtimeTopology buildSourceRealtimeTopology(String inputFile, Consumer<TopologyEntityDTO> entityConsumer) throws IOException {
        final GlobalSupplyChainCalculator globalSupplyChainCalculator =
                new GlobalSupplyChainCalculator();
        SearchResolver<RepoGraphEntity>
                searchResolver = new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator, searchResolver);
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeSourceTopology(
                TopologyInfo.getDefaultInstance());
        Map<EntityType, MutableLong> countsByType = new HashMap<>();
        Consumer<TopologyEntityDTO> c = entity -> {
            countsByType.computeIfAbsent(EntityType.forNumber(entity.getEntityType()), k -> new MutableLong(0)).increment();
            sourceRealtimeTopologyBuilder.addEntity(entity);
            entityConsumer.accept(entity);
        };
        if (inputFile.endsWith(".binary")) {
            doForBinaryEntity(inputFile, c);
        } else {
            logger.warn("Attempting to parse JSON format. This is slower. Rewrite to binary instead.");
            doForJsonEntity(inputFile, c);
        }
        SourceRealtimeTopology topology = sourceRealtimeTopologyBuilder.finish();
        logger.info(countsByType);
        return topology;
    }

    /**
     * Rewrite a file of TopologyEntityDTOs stored as JSON to binary format. Binary is smaller,
     * and much faster to read, which makes it easier to use for tests.
     *
     * @param inputFile The input file path. The output file path will be the same as the input file,
     *                  with a ".binary" added.
     * @throws IOException If there is an error with IO.
     */
    public void rewriteAsBinary(String inputFile) throws IOException {

        final BufferedReader reader = new BufferedReader(new FileReader(inputFile));

        String outFile = inputFile + ".binary";

        FileOutputStream fos = new FileOutputStream(outFile);

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        int cnt = 0;
        logger.info("Writing entities to {} as binary delimited TopologyEntityDTOs", outFile);
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();

            entity.writeDelimitedTo(fos);
            if (cnt++ % 1000 == 0) {
                logger.info(cnt);
            }
        }
        fos.flush();
        fos.close();

        logger.info("Completed writing {} entities to {}", cnt, outFile);
    }

}
