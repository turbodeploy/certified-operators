package com.vmturbo.repository.listener.realtime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.util.JsonFormat;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver;

/**
 * This test is ignored for automatic builds.
 *
 * It's intended to measure the memory usage (and other performance) of a topology graph with a
 * large customer topology.
 */
@Ignore
public class LiveTopologyStoreMeasurementIT {
    private final Logger logger = LogManager.getLogger();

    private final SupplyChainResolver<RepoGraphEntity> supplyChainResolver = new SupplyChainResolver<>();

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

        logger.info("Total construction time: {}\n" +
                "Size: {}",
            stopwatch.elapsed(TimeUnit.MILLISECONDS),
            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        stopwatch.reset();
        stopwatch.start();
        liveTopologyStore.getSourceTopology().get().globalSupplyChainNodes(Optional.empty(), supplyChainResolver);
        liveTopologyStore.getSourceTopology().get().globalSupplyChainNodes(Optional.of(UIEnvironmentType.CLOUD), supplyChainResolver);
        liveTopologyStore.getSourceTopology().get().globalSupplyChainNodes(Optional.of(UIEnvironmentType.ON_PREM), supplyChainResolver);
        stopwatch.stop();
        logger.info("GSC Took {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        logger.info("Size with global supply chain: {}", FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        stopwatch.reset();
        stopwatch.start();
        liveTopologyStore.getSourceTopology().get().entityGraph().entities()
            .map(RepoGraphEntity::getTopologyEntity)
            .forEach(e -> cnt.increment());
        stopwatch.stop();
        logger.info("Took {} to de-compress {} entities", stopwatch.elapsed(TimeUnit.SECONDS), cnt.intValue());
    }


    @Test
    @Ignore
    public void testRealtimeProjected() throws IOException {
        // Put the filename here.
        final String filePath = "/Volumes/Workspace/topologies/bofa/may23/repo/live.topology.source.entities";
        Preconditions.checkArgument(!StringUtils.isEmpty(filePath));
        final BufferedReader reader =
            new BufferedReader(new FileReader(filePath));
        LiveTopologyStore liveTopologyStore = new LiveTopologyStore(GlobalSupplyChainCalculator.newFactory().newCalculator());

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

        logger.info("Construction time: {}\nSize: {}", watch.elapsed(TimeUnit.MILLISECONDS),
            FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getProjectedTopology().get())));

        final MutableInt cnt = new MutableInt(0);
        watch.reset();
        watch.start();
        liveTopologyStore.getProjectedTopology().get().getEntities(Collections.emptySet(), Collections.emptySet())
            .forEach(e -> cnt.increment());
        watch.stop();
        logger.info("Took {} to de-compress {} entities", watch.elapsed(TimeUnit.SECONDS), cnt.intValue());
    }
}