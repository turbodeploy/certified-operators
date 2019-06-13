package com.vmturbo.repository.listener.realtime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricTimer;
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
        long msTaken = 0;
        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        while (reader.ready()) {
            final TopologyEntityDTO.Builder bldr = TopologyEntityDTO.newBuilder();
            parser.merge(reader.readLine(), bldr);
            TopologyEntityDTO entity = bldr.build();
            countsByType.computeIfAbsent(EntityType.forNumber(entity.getEntityType()), k -> new MutableLong(0)).increment();
            long startTime = System.currentTimeMillis();
            sourceRealtimeTopologyBuilder.addEntities(Collections.singleton(entity));
            msTaken += System.currentTimeMillis() - startTime;
            lineCnt++;
            if (lineCnt % 1000 == 0) {
                System.out.println("Processed " + lineCnt);
            }
        }

        System.out.println(countsByType);
        long preFinishTime = System.currentTimeMillis();
        sourceRealtimeTopologyBuilder.finish();
        msTaken += System.currentTimeMillis() - preFinishTime;
        System.out.println("Total construction time: " + msTaken);
        System.out.println("Size: " + FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));

        DataMetricTimer timer2 = new DataMetricTimer();
        liveTopologyStore.getSourceTopology().get().globalSupplyChainNodes();
        System.out.println("GSC Took " + timer2.getTimeElapsedSecs());
        System.out.println("Size with global supply chain: " + FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getSourceTopology().get())));
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
                System.out.println("Processed " + lineCnt);
            }
        }

        System.out.println(countsByType);
        DataMetricTimer timer = new DataMetricTimer();
        ptbldr.finish();
        System.out.println("Took " + timer.getTimeElapsedSecs());
        System.out.println("Size: " + FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(liveTopologyStore.getProjectedTopology().get())));
    }
}