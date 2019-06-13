package com.vmturbo.repository.listener.realtime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;

/**
 * This test is ignored for automatic builds.
 *
 * It's intended to measure the memory usage (and other performance) of a topology graph with a
 * large customer topology.
 */
@Ignore
public class LiveTopologyStoreMeasurementIT {
    private static final Logger logger = LogManager.getLogger();

    @Test
    @Ignore
    public void testRealtimeSource() throws IOException {
        // Put the filename here.
        final String filePath = "/Volumes/Workspace/topologies/bofa/may23/repo/live.topology.source.entities";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        final LiveTopologyStore liveTopologyStore =
            new LiveTopologyStore(GlobalSupplyChainCalculator.newFactory().newCalculator());
        SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder = liveTopologyStore.newRealtimeTopology(TopologyInfo.getDefaultInstance());
        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();
        Stopwatch stopwatch = Stopwatch.createUnstarted();
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

        System.out.println(countsByType);
        stopwatch.start();
        sourceRealtimeTopologyBuilder.finish();
        stopwatch.stop();
        logger.info("Total construction time: {}s ", stopwatch.elapsed(TimeUnit.SECONDS));
        logger.info("Size: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        stopwatch.reset();
        stopwatch.start();
        liveTopologyStore.getSourceTopology().get().globalSupplyChainNodes();
        stopwatch.stop();
        logger.info("GSC took {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        logger.info("Size with global supply chain: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        stopwatch.reset();
        stopwatch.start();
        liveTopologyStore.getSourceTopology().get().entityGraph().entities()
            .map(RepoGraphEntity::getTopologyEntity)
            .forEach(e -> cnt.increment());
        stopwatch.stop();
        logger.info("Took {}s to de-compress {} entities", stopwatch.elapsed(TimeUnit.SECONDS), cnt.intValue());
    }


    @Test
    @Ignore
    public void testRealtimeProjected() throws IOException {
        // Put the filename here.
        final String filePath = "";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        LiveTopologyStore liveTopologyStore = new LiveTopologyStore(GlobalSupplyChainCalculator.newFactory().newCalculator());

        ProjectedTopologyBuilder ptbldr = liveTopologyStore.newProjectedTopology(1, TopologyInfo.getDefaultInstance());

        int lineCnt = 0;
        Map<EntityType, MutableLong> countsByType = new HashMap<>();
        while (reader.ready()) {
            TopologyEntityDTO eBldr = gson.fromJson(reader.readLine(), TopologyEntityDTO.class);
            countsByType.computeIfAbsent(EntityType.forNumber(eBldr.getEntityType()), k -> new MutableLong(0)).increment();
            ptbldr.addEntities(Collections.singleton(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(1)
                .setProjectedPriceIndex(2)
                .setEntity(eBldr)
                .build()));
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                logger.info("Processed {}", lineCnt);
            }
        }

        logger.info(countsByType);
        Stopwatch timer = Stopwatch.createStarted();
        ptbldr.finish();
        timer.stop();
        logger.info("Took {}s", timer.elapsed(TimeUnit.SECONDS));
        logger.info("Size: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getProjectedTopology().get())));
    }
}