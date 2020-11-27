package com.vmturbo.topology.processor.stitching.journal;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.ioThroughputKBps;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAmount;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vStorageMB;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class StitchingEntitySemanticDifferTest {
    private final TopologyStitchingEntity a = entity("a", 1L, 11L);
    private final TopologyStitchingEntity b = entity("b", 2L, 12L);
    private final TopologyStitchingEntity c = entity("c", 3L, 13L);
    private final TopologyStitchingEntity d = entity("d", 4L, 14L);
    private final TopologyStitchingEntity e = entity("e", 5L, 15L);
    private final TopologyStitchingEntity f = entity("f", 6L, 16L);
    private final TopologyStitchingEntity g = entity("g", 7L, 17L);
    private StitchingEntity snapshot;

    StitchingEntitySemanticDiffer differ =
        new StitchingEntitySemanticDiffer(Verbosity.LOCAL_CONTEXT_VERBOSITY);

    @Before
    public void setup() {
        final List<Builder> boughtFromC = new ArrayList<>();
        boughtFromC.add(cpuMHz().from("c").used(100.0).build().toBuilder());
        boughtFromC.add(memKB().from("c").used(140.0).build().toBuilder());

        a.addConsumer(b);
        a.addProviderCommodityBought(c, new CommoditiesBought(boughtFromC));
        a.addProviderCommodityBought(d, new CommoditiesBought(Collections.singletonList(
                storageAmount().from("d").used(500.0).build().toBuilder())));
        a.getTopologyCommoditiesSold()
            .add(new CommoditySold(vCpuMHz().capacity(75.0).build().toBuilder(), e)); // Accesses e
        a.getTopologyCommoditiesSold()
            .add(new CommoditySold(vStorageMB().capacity(200.0).build().toBuilder(), null)); // Accesses nothing

        snapshot = a.snapshot();
    }

    @Test
    public void testUnchanged() {
        // When no changes, should return an empty string.
        assertEquals("", differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveConsumer() {
        a.removeConsumer(b);
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Consumers: removed=[VIRTUAL_MACHINE-2-12]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testAddConsumer() {
        a.addConsumer(f);
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Consumers: added=[VIRTUAL_MACHINE-6-16]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveAndAddConsumers() {
        a.removeConsumer(b);
        a.addConsumer(f);
        a.addConsumer(g);

        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Consumers: removed=[VIRTUAL_MACHINE-2-12] added=[VIRTUAL_MACHINE-6-16, VIRTUAL_MACHINE-7-17]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));

    }

    @Test
    public void testRemoveProvider() {
        a.removeProvider(c);
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Providers: removed=[VIRTUAL_MACHINE-3-13]\n" +
                "  CommoditiesBought:\n" +
                "  {\n" +
                "  --\"VIRTUAL_MACHINE-3-13\": [\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"CPU\",\n" +
                "  ------\"used\": 100.0\n" +
                "  ----},\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"MEM\",\n" +
                "  ------\"used\": 140.0\n" +
                "  ----}\n" +
                "  --],\n" +
                "    \"VIRTUAL_MACHINE-4-14\": [\n" +
                "      {\n" +
                "        \"commodityType\": \"STORAGE_AMOUNT\",\n" +
                "        \"used\": 500.0\n" +
                "      }\n" +
                "    ]\n" +
                "  }",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testAddProvider() {
        a.addProviderCommodityBought(f, new CommoditiesBought(
                Collections.singletonList(ioThroughputKBps().used(10.0).build().toBuilder())));
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Providers: added=[VIRTUAL_MACHINE-6-16]\n" +
                "  CommoditiesBought:\n" +
                "  {\n" +
                "  ++\"VIRTUAL_MACHINE-6-16\": [\n" +
                "  ++++{\n" +
                "  ++++++\"commodityType\": \"IO_THROUGHPUT\",\n" +
                "  ++++++\"used\": 10.0\n" +
                "  ++++}\n" +
                "  ++],\n" +
                "    ... 2 additional fields ...\n" +
                "  }",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testProviderAndConsumerChangesTogether() {
        a.addProviderCommodityBought(f, new CommoditiesBought(
                Collections.singletonList(ioThroughputKBps().used(10.0).build().toBuilder())));
        a.addConsumer(c);
        a.addConsumer(d);
        a.removeConsumer(b);

        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Providers: added=[VIRTUAL_MACHINE-6-16]\n" +
                "  Consumers: removed=[VIRTUAL_MACHINE-2-12] added=[VIRTUAL_MACHINE-3-13, VIRTUAL_MACHINE-4-14]\n" +
                "  CommoditiesBought:\n" +
                "  {\n" +
                "  ++\"VIRTUAL_MACHINE-6-16\": [\n" +
                "  ++++{\n" +
                "  ++++++\"commodityType\": \"IO_THROUGHPUT\",\n" +
                "  ++++++\"used\": 10.0\n" +
                "  ++++}\n" +
                "  ++],\n" +
                "    ... 2 additional fields ...\n" +
                "  }",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveAndAddProviders() {
        a.removeProvider(c);
        a.removeProvider(d);
        a.addProviderCommodityBought(f, new CommoditiesBought(
                Collections.singletonList(ioThroughputKBps().used(10.0).build().toBuilder())));

        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Providers: removed=[VIRTUAL_MACHINE-3-13, VIRTUAL_MACHINE-4-14] added=[VIRTUAL_MACHINE-6-16]\n" +
                "  CommoditiesBought:\n" +
                "  {\n" +
                "  --\"VIRTUAL_MACHINE-3-13\": [\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"CPU\",\n" +
                "  ------\"used\": 100.0\n" +
                "  ----},\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"MEM\",\n" +
                "  ------\"used\": 140.0\n" +
                "  ----}\n" +
                "  --],\n" +
                "  --\"VIRTUAL_MACHINE-4-14\": [\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"STORAGE_AMOUNT\",\n" +
                "  ------\"used\": 500.0\n" +
                "  ----}\n" +
                "  --],\n" +
                "  ++\"VIRTUAL_MACHINE-6-16\": [\n" +
                "  ++++{\n" +
                "  ++++++\"commodityType\": \"IO_THROUGHPUT\",\n" +
                "  ++++++\"used\": 10.0\n" +
                "  ++++}\n" +
                "  ++]\n" +
                "  }",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveCommoditySold() {
        a.getTopologyCommoditiesSold().remove(0);
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  CommoditiesSold:\n" +
                "  [\n" +
                "  --{\n" +
                "  ----\"capacity\": 75.0,\n" +
                "  ----\"commodityType\": \"VCPU\",\n" +
                "  ----\"vcpuData\": { }\n" +
                "  --},\n" +
                "    {\n" +
                "      \"capacity\": 200.0,\n" +
                "      \"commodityType\": \"VSTORAGE\",\n" +
                "      \"vstoragePartitionData\": { }\n" +
                "    }\n" +
                "  ]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testAddCommoditySold() {
        a.getTopologyCommoditiesSold()
            .add(new CommoditySold(vMemKB().key("foo").capacity(45.0).build().toBuilder(), null));
        a.getTopologyCommoditiesSold()
            .add(new CommoditySold(vMemKB().key("bar").capacity(45.0).build().toBuilder(), null));
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  CommoditiesSold:\n" +
                "  [\n" +
                "    {\n" +
                "      \"capacity\": 75.0,\n" +
                "      \"commodityType\": \"VCPU\",\n" +
                "      \"vcpuData\": { }\n" +
                "    },\n" +
                "  ++{\n" +
                "  ++++\"capacity\": 45.0,\n" +
                "  ++++\"commodityType\": \"VMEM\",\n" +
                "  ++++\"key\": \"bar\",\n" +
                "  ++++\"vmemData\": { }\n" +
                "  ++},\n" +
                "  ++{\n" +
                "  ++++\"capacity\": 45.0,\n" +
                "  ++++\"commodityType\": \"VMEM\",\n" +
                "  ++++\"key\": \"foo\",\n" +
                "  ++++\"vmemData\": { }\n" +
                "  ++},\n" +
                "    {\n" +
                "      \"capacity\": 200.0,\n" +
                "      \"commodityType\": \"VSTORAGE\",\n" +
                "      \"vstoragePartitionData\": { }\n" +
                "    }\n" +
                "  ]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveAndAddCommoditySold() {
        a.getTopologyCommoditiesSold().remove(0);
        a.getTopologyCommoditiesSold()
            .add(new CommoditySold(vMemKB().key("foo").capacity(45.0).build().toBuilder(), null));

        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  CommoditiesSold:\n" +
                "  [\n" +
                "    {\n" +
                "      \"capacity\": ((75.0 --> 45.0)),\n" +
                "      \"commodityType\": ((\"VCPU\" --> \"VMEM\")),\n" +
                "  ++++\"key\": \"foo\",\n" +
                "  ----\"vcpuData\": { },\n" +
                "  ++++\"vmemData\": { }\n" +
                "    },\n" +
                "    {\n" +
                "      \"capacity\": 200.0,\n" +
                "      \"commodityType\": \"VSTORAGE\",\n" +
                "      \"vstoragePartitionData\": { }\n" +
                "    }\n" +
                "  ]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testRemoveCommodityBought() {
        a.getCommodityBoughtListByProvider().get(c).get(0).getBoughtList().remove(0);
        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  CommoditiesBought:\n" +
                "  {\n" +
                "    \"VIRTUAL_MACHINE-3-13\": [\n" +
                "  ----{\n" +
                "  ------\"commodityType\": \"CPU\",\n" +
                "  ------\"used\": 100.0\n" +
                "  ----},\n" +
                "      {\n" +
                "        \"commodityType\": \"MEM\",\n" +
                "        \"used\": 140.0\n" +
                "      }\n" +
                "    ],\n" +
                "    \"VIRTUAL_MACHINE-4-14\": [\n" +
                "      {\n" +
                "        \"commodityType\": \"STORAGE_AMOUNT\",\n" +
                "        \"used\": 500.0\n" +
                "      }\n" +
                "    ]\n" +
                "  }",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    @Test
    public void testMergeInformation() {
        a.addMergeInformation(new StitchingMergeInformation(1234L, 5678L, StitchingErrors.none()));
        a.addMergeInformation(new StitchingMergeInformation(5555L, 4444L, StitchingErrors.none()));

        assertEquals(
            "VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  Merge Information:\n" +
                "  [\n" +
                "  ++{\n" +
                "  ++++\"error\": {\n" +
                "  ++++++\"errCode\": 0\n" +
                "  ++++},\n" +
                "  ++++\"oid\": \"5555\",\n" +
                "  ++++\"origin\": \"DISCOVERED\",\n" +
                "  ++++\"targetId\": \"4444\"\n" +
                "  ++},\n" +
                "  ++{\n" +
                "  ++++\"error\": {\n" +
                "  ++++++\"errCode\": 0\n" +
                "  ++++},\n" +
                "  ++++\"oid\": \"1234\",\n" +
                "  ++++\"origin\": \"DISCOVERED\",\n" +
                "  ++++\"targetId\": \"5678\"\n" +
                "  ++}\n" +
                "  ]",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY)
        );
    }

    @Test
    public void testLastUpdatedTime() {
        a.updateLastUpdatedTime(9876543210L);
        assertEquals("VIRTUAL_MACHINE a a (oid-1 tgt-11)\n" +
                "  lastUpdatedTime: (([1970-01-01T02:07:34.321] --> [1970-04-25T07:29:03.210]))",
            differ.semanticDiff(snapshot, a, FormatRecommendation.PRETTY));
    }

    private static TopologyStitchingEntity entity(@Nonnull final String name,
                                                  final long oid,
                                                  final long targetId) {
        final EntityDTO.Builder entityBuilder = EntityDTO.newBuilder()
            .setId(name)
            .setDisplayName(name)
            .setEntityType(EntityType.VIRTUAL_MACHINE);

        return new TopologyStitchingEntity(entityBuilder, oid, targetId, 7654321L);
    }
}