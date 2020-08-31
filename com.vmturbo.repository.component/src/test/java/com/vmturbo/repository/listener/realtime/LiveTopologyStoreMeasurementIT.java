package com.vmturbo.repository.listener.realtime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;


import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * This test is ignored for automatic builds.
 *
 * It's intended to measure the memory usage (and other performance) of a topology graph with a
 * large customer topology.
 */
@Ignore
public class LiveTopologyStoreMeasurementIT {
    private final Logger logger = LogManager.getLogger();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator =
            new GlobalSupplyChainCalculator();

    /**
     * Use this "test" to rewrite entities saved as a string to binary format.
     * This makes it much (i.e. 10x) faster to load them afterwards for other tests.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Ignore
    public void testRewriteAsBinary() throws Exception {

        final String filePath = "/home/turbo/work/topologies/pwc.live.realtime.source.entities";

        final BufferedReader reader =
                new BufferedReader(new FileReader(filePath));

        String outFile = "/home/turbo/work/topologies/pwc.live.entities.binary";

        FileOutputStream fos = new FileOutputStream(outFile);

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        int cnt = 0;
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
    }

    private void doForEntity(String filePath, Consumer<TopologyEntityDTO> consumer) throws IOException {
        File file = new File(filePath);
        MutableInt lineCnt = new MutableInt(0);
        try (InputStream is = FileUtils.openInputStream(file)) {
            try {
                while (true) {
                    Topology e = Topology.parseDelimitedFrom(is);
                    if (e == null) {
                        break;
                    }
                    e.getData().getEntitiesList().forEach(segment -> {
                        lineCnt.increment();
                        consumer.accept(segment.getEntity());
                    });
                    logger.info("Processed {}", lineCnt);
                }
            } catch (IOException e) {
                // Noop.

            }
        }
    }

    /**
     * Test the size of a topology.
     *
     * @throws IOException If anything goes wrong.
     */
    @Test
    @Ignore
    public void testSize() throws IOException {
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());

        doForEntity("/home/turbo/work/saved-topology/citi.binary", entity -> {
            sourceRealtimeTopologyBuilder.addEntities(Collections.singletonList(entity));
        });

        SourceRealtimeTopology topology = sourceRealtimeTopologyBuilder.finish();
//        logger.info(ObjectSizeCalculator.getObjectSize(topology));
    }

    @Test
    @Ignore
    public void testRealtimeSource() throws IOException {
        // Put the filename here.
        final String filePath = "/home/turbo/Work/topologies/bofa/may23/";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());
        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();

        final Stopwatch stopwatch = Stopwatch.createUnstarted();

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();
            countsByType.computeIfAbsent(EntityType.forNumber(entity.getEntityType()), k -> new MutableLong(0)).increment();
            stopwatch.start();
            sourceRealtimeTopologyBuilder.addEntities(Collections.singleton(entity));
            stopwatch.stop();
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }

        logger.info(countsByType);
        stopwatch.start();
        sourceRealtimeTopologyBuilder.finish();
        stopwatch.stop();

//        logger.info("Total construction time: {}\n" +
//                "Size: {}",
//            stopwatch.elapsed(TimeUnit.MILLISECONDS),
//            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        stopwatch.reset();
        stopwatch.start();
        final TopologyGraph<RepoGraphEntity> topologyGraph = liveTopologyStore.getSourceTopology()
                                                                       .get().entityGraph();
        topologyGraph.entities()
                .map(RepoGraphEntity::getTopologyEntity)
                .forEach(e -> cnt.increment());
        stopwatch.stop();
        logger.info("Took {} to de-compress {} entities", stopwatch.elapsed(TimeUnit.SECONDS), cnt.intValue());

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, x -> true,
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("Hybrid GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
//        logger.info("Size with global supply chain: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, e -> EnvironmentTypeUtil.match(e.getEnvironmentType(), EnvironmentType.CLOUD),
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("Cloud GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset();
        stopwatch.start();
        globalSupplyChainCalculator.getSupplyChainNodes(topologyGraph, e -> EnvironmentTypeUtil.match(e.getEnvironmentType(), EnvironmentType.ON_PREM),
                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);
        stopwatch.stop();
        logger.info("On-prem GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    @Ignore
    public void testRealtimeProjected() throws IOException {
        // Put the filename here.
        final String filePath = "/Volumes/Workspace/topologies/bofa/may23/repo/live.topology.source.entities";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        LiveTopologyStore liveTopologyStore = new LiveTopologyStore(globalSupplyChainCalculator);

        ProjectedTopologyBuilder ptbldr = liveTopologyStore.newProjectedTopology(1, TopologyInfo.getDefaultInstance());

        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();

        Stopwatch watch = Stopwatch.createUnstarted();

        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            TopologyEntityDTO.Builder eBldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), eBldr);
            ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(1)
                .setProjectedPriceIndex(2)
                .setEntity(eBldr)
                .build();

            countsByType.computeIfAbsent(EntityType.forNumber(eBldr.getEntityType()), k -> new MutableLong(0)).increment();
            watch.start();
            ptbldr.addEntities(Collections.singleton(entity));
            watch.stop();
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }

        logger.info(countsByType);


        watch.start();
        ptbldr.finish();
        watch.stop();

//        logger.info("Construction time: {}\nSize: {}", watch.elapsed(TimeUnit.MILLISECONDS),
//            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getProjectedTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        watch.reset();
        watch.start();
        liveTopologyStore.getProjectedTopology().get().getEntities(Collections.emptySet(), Collections.emptySet())
            .forEach(e -> cnt.increment());
        watch.stop();
        logger.info("Took {} to de-compress {} entities", watch.elapsed(TimeUnit.SECONDS), cnt.intValue());
    }
}