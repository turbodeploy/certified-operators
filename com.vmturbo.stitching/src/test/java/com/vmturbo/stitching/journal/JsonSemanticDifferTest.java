package com.vmturbo.stitching.journal;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAmount;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonSemanticDiffer.DiffOutput;

public class JsonSemanticDifferTest {

    @Test
    public void testObjectReplace() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}", "{\"x\":\"z\"}");
        assertEquals(
            "{\n" +
            "  \"x\": ((\"y\" --> \"z\"))\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testObjectAddition() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}", "{\"x\":\"y\", \"z\":\"w\"}}");
        assertEquals(
            "{\n" +
            "  \"x\": \"y\",\n" +
            "++\"z\": \"w\"\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testObjectAdditionManyChanges() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{}",
            "{\"x\": {\"z\":\"w\", \"1\":\"2\", \"3\":\"4\", \"5\":\"6\", \"7\":\"8\"}}");
        assertEquals(
            "{\n" +
            "++\"x\": {\n" +
            "++++\"1\": \"2\",\n" +
            "++++\"3\": \"4\",\n" +
            "++++\"5\": \"6\",\n" +
            "++++\"7\": \"8\",\n" +
            "++++\"z\": \"w\"\n" +
            "++}\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testNestedObjectAddition() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}",
            "{\"x\":\"y\", \"z\": {\"w\": \"a\"}}}");
        assertEquals(
            "{\n" +
            "  \"x\": \"y\",\n" +
            "++\"z\": {\n" +
            "++++\"w\": \"a\"\n" +
            "++}\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testObjectAdditionAndChange() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}", "{\"x\":\"b\", \"z\":\"w\"}}");
        assertEquals(
            "{\n" +
            "  \"x\": ((\"y\" --> \"b\")),\n" +
            "++\"z\": \"w\"\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testObjectRemoval() throws Exception {
        final DiffOutput output = new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\", \"z\":\"w\"}}", "{\"x\":\"y\"}");
        assertEquals(
            "{\n" +
            "  \"x\": \"y\",\n" +
            "--\"z\": \"w\"\n" +
            "}",
            output.diff);
        assertFalse(output.identical);
    }

    @Test
    public void testObjectReplaceWithUnchanged() throws Exception {
        assertEquals(
            "{\n" +
            "  \"x\": ((\"y\" --> \"z\")),\n" +
            "  ... 2 additional fields ...\n" +
            "}",
            new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\", \"foo\":\"bar\", \"qux\":\"quux\"}",
                "{\"x\":\"z\", \"foo\":\"bar\", \"qux\":\"quux\"}").diff);
    }

    @Test
    public void testObjectWithChildrenReplace() throws Exception {
        assertEquals(
            "{\n" +
            "  \"x\": {\n" +
            "    \"y\": ((\"z\" --> \"w\"))\n" +
            "  }\n" +
            "}",
            new JsonSemanticDiffer().semanticDiff("{\"x\":{\"y\":\"z\"}}", "{\"x\":{\"y\":\"w\"}}").diff);
    }

    @Test
    public void testSimpleArrayReplace() throws Exception {
        assertEquals(
            "[\n" +
            "  ((1 --> 2))\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1]", "[2]").diff);
    }

    @Test
    public void testArrayRemoveAtStart() throws Exception {
        assertEquals(
            "[\n" +
            "--1,\n" +
            "  2,\n" +
            "  3\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3]", "[2,3]").diff);
    }

    @Test
    public void testArrayClearArray() throws Exception {
        assertEquals(
            "[\n" +
            "--1\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1]", "[]").diff);
    }

    @Test
    public void testObjectDelete() throws Exception {
        assertEquals(
            "{\n" +
            "  \"x\": \"y\"\n" +
            "} --> []",
            new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}", "[]").diff);
    }

    @Test
    public void testArrayRemoveAtEnd() throws Exception {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "  2,\n" +
            "--3\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3]", "[1,2]").diff);
    }

    @Test
    public void testAddToEmpty() throws Exception {
        assertEquals(
            "[\n" +
            "++[\n" +
            "++++1\n" +
            "++],\n" +
            "++2\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[]", "[[1],2]").diff);
    }

    @Test
    public void testArrayRemoveInMiddle() throws Exception {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "--2,\n" +
            "  3\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3]", "[1,3]").diff);
    }

    @Test
    public void testSimpleArrayAddition() throws Exception {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "++2\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1]", "[1,2]").diff);
    }

    @Test
    public void testArrayReversal() throws Exception {
        assertEquals(
            "[\n" +
            "++4,\n" +
            "++3,\n" +
            "++2,\n" +
            "  1,\n" +
            "--2,\n" +
            "--3,\n" +
            "--4\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3,4]", "[4,3,2,1]").diff);
    }


    @Test
    public void testArrayManyChanges() throws Exception {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "++{\n" +
            "++++\"x\": \"y\"\n" +
            "++},\n" +
            "  2,\n" +
            "  ((3 --> 5)),\n" +
            "--4\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3,4]", "[1,{\"x\":\"y\"},2,5]").diff);
    }

    @Test
    public void testArrayChangesWithDuplicates() throws Exception {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "  1,\n" +
            "  1,\n" +
            "++1\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,1,1]", "[1,1,1,1]").diff);
    }

    @Ignore("Because a utility used by the diff algorithm does not guarantee the same output for the same inputs, " +
        "it's very hard to write a test case for a complicated diff which can be rendered in multiple ways correctly. " +
        "The test is still useful to have as an example that can be run and output to the screen.")
    @Test
    public void testHumanReadableDiff() throws Exception {
        EntityDTO vm1 = virtualMachine("foo")
            .displayName("vm-1")
            .buying(cpuMHz().from("bar").used(20.0))
            .buying(storageAmount().used(100.0).from("baz"))
            .selling(vCpuMHz().capacity(20.0))
            .selling(vMemKB().capacity(10.0))
            .build();

        EntityDTO vm2 = virtualMachine("foo")
            .displayName("vm-1")
            .buying(storageAmount().used(100.0).from("baz"))
            .buying(cpuMHz().from("bar").used(19.2).key("key").peak(123.4))
            .buying(memKB().from("qux").used(33.3).active(false).computeUsed(false))
            .selling(vCpuMHz().capacity(15.0))
            .build();

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm1);
        final String j2 = gson.toJson(vm2);

        assertEquals(
            "{\n" +
            "  \"commoditiesBought\": [\n" +
            "    {\n" +
            "      \"bought\": [\n" +
            "        {\n" +
            "          \"commodityType\": ((\"CPU\" --> \"MEM\")),\n" +
            "          \"used\": ((20.0 --> 33.3))\n" +
            "        }\n" +
            "      ],\n" +
            "      \"providerId\": ((\"bar\" --> \"qux\"))\n" +
            "    },\n" +
            "++++{\n" +
            "++++++\"bought\": [\n" +
            "++++++++{\n" +
            "++++++++++\"commodityType\": \"CPU\",\n" +
            "++++++++++\"key\": \"key\",\n" +
            "++++++++++\"peak\": 123.4,\n" +
            "++++++++++\"used\": 19.2\n" +
            "++++++++}\n" +
            "++++++],\n" +
            "++++++\"providerId\": \"bar\"\n" +
            "++++},\n" +
            "    {\n" +
            "      \"bought\": [\n" +
            "        {\n" +
            "          \"commodityType\": \"STORAGE_AMOUNT\",\n" +
            "          \"used\": 100.0\n" +
            "        }\n" +
            "      ],\n" +
            "      \"providerId\": \"baz\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"commoditiesSold\": [\n" +
            "    {\n" +
            "      \"capacity\": ((10.0 --> 15.0)),\n" +
            "      \"commodityType\": ((\"VMEM\" --> \"VCPU\")),\n" +
            "------\"vmemData\": { }\n" +
            "    },\n" +
            "----{\n" +
            "------\"capacity\": 20.0,\n" +
            "------\"commodityType\": \"VCPU\",\n" +
            "------\"vcpuData\": { }\n" +
            "----}\n" +
            "  ],\n" +
            "  ... 5 additional fields ...\n" +
            "}",
            new JsonSemanticDiffer().semanticDiff(j1, j2).diff);
    }

    @Ignore("Because a utility used by the diff algorithm does not guarantee the same output for the same inputs, " +
        "it's very hard to write a test case for a complicated diff which can be rendered in multiple ways correctly. " +
        "The test is still useful to have as an example that can be run and output to the screen.")
    @Test
    public void testVeryDifferent() throws Exception {
        EntityDTO vm = virtualMachine("foo")
            .displayName("vm-1")
            .buying(cpuMHz().from("bar").used(20.0))
            .buying(storageAmount().used(100.0).from("baz"))
            .selling(vCpuMHz().capacity(20.0))
            .selling(vMemKB().capacity(10.0))
            .build();

        EntityDTO pm = physicalMachine("bar")
            .displayName("physicalMachine")
            .selling(cpuMHz().capacity(987.123))
            .build();

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm);
        final String j2 = gson.toJson(pm);

        assertEquals(
            "{\n" +
            "--\"commoditiesBought\": [\n" +
            "----{\n" +
            "------\"bought\": [\n" +
            "--------{\n" +
            "----------\"commodityType\": \"CPU\",\n" +
            "----------\"used\": 20.0\n" +
            "--------}\n" +
            "------],\n" +
            "------\"providerId\": \"bar\"\n" +
            "----},\n" +
            "----{\n" +
            "------\"bought\": [\n" +
            "--------{\n" +
            "----------\"commodityType\": \"STORAGE_AMOUNT\",\n" +
            "----------\"used\": 100.0\n" +
            "--------}\n" +
            "------],\n" +
            "------\"providerId\": \"baz\"\n" +
            "----}\n" +
            "--],\n" +
            "  \"commoditiesSold\": [\n" +
            "    {\n" +
            "      \"capacity\": ((10.0 --> 987.123)),\n" +
            "      \"commodityType\": ((\"VMEM\" --> \"CPU\")),\n" +
            "------\"vmemData\": { }\n" +
            "    },\n" +
            "----{\n" +
            "------\"capacity\": 20.0,\n" +
            "------\"commodityType\": \"VCPU\",\n" +
            "------\"vcpuData\": { }\n" +
            "----}\n" +
            "  ],\n" +
            "  \"displayName\": ((\"vm-1\" --> \"physicalMachine\")),\n" +
            "  \"entityType\": ((\"VIRTUAL_MACHINE\" --> \"PHYSICAL_MACHINE\")),\n" +
            "  \"id\": ((\"foo\" --> \"bar\")),\n" +
            "--\"virtualMachineData\": {\n" +
            "----\"vmState\": { }\n" +
            "--},\n" +
            "--\"virtualMachineRelatedData\": { }\n" +
            "}",
            new JsonSemanticDiffer().semanticDiff(j1, j2).diff);

    }

    @Test
    public void testArrayCopy() throws IOException {
        assertEquals(
            "[\n" +
            "++2,\n" +
            "  1,\n" +
            "  ((2 --> {\"x\":\"z\"})),\n" +
            "--{\n" +
            "----\"x\": \"y\"\n" +
            "--}\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,{\"x\":\"y\"}]", "[2,1,{\"x\":\"z\"}]").diff);
    }

    @Test
    public void testNestedArrays() throws IOException {
        assertEquals(
            "[\n" +
            "++2,\n" +
            "  1,\n" +
            "  ((2 --> {\"x\":\"z\"})),\n" +
            "  {\n" +
            "    \"x\": \"y\"\n" +
            "  } --> [3,4,5],\n" +
            "--[\n" +
            "----1,\n" +
            "----2\n" +
            "--]\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,{\"x\":\"y\"},[1,2]]", "[2,1,{\"x\":\"z\"},[3,4,5]]").diff);
    }

    @Test
    public void testArrayMove() throws IOException {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "++3,\n" +
            "  2,\n" +
            "--3,\n" +
            "  2,\n" +
            "++2\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3,2]", "[1,3,2,2,2]").diff);
    }

    @Test
    public void testArrayMoveCompact() throws IOException {
        assertEquals(
            "[1,++3,2,--3,2,++2]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3,2]", "[1,3,2,2,2]",
                Verbosity.LOCAL_CONTEXT_VERBOSITY, FormatRecommendation.COMPACT, 0).diff);
    }

    @Test
    public void testExactlyIdentical() throws IOException {
        assertTrue(
            new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\"}", "{\"x\":\"y\"}").identical);
    }

    @Test
    public void testIdenticalOutOfOrder() throws IOException {
        assertTrue(
            new JsonSemanticDiffer().semanticDiff("{\"x\":\"y\", \"z\":\"w\"}",
                "{\"z\":\"w\", \"x\":\"y\"}").identical);
    }

    @Test
    public void testIdenticalWithWhitespaceDifferencesAndArray() throws IOException {
        assertTrue(
            new JsonSemanticDiffer().semanticDiff("{\"x\": [1,2], \"z\":\"w\"}",
                "{\"z\":\"w\", \"x\":            [1,               2]}").identical);
    }

    @Test
    public void testIncludeAllIdentical() throws IOException {
        assertEquals(
            "[\n" +
            "  1,\n" +
            "  2,\n" +
            "  3,\n" +
            "  4,\n" +
            "  5,\n" +
            "  6,\n" +
            "  7\n" +
            "]",
            new JsonSemanticDiffer().semanticDiff("[1,2,3,4,5,6,7]", "[1,2,3,4,5,6,7]",
                Verbosity.COMPLETE_VERBOSITY, FormatRecommendation.PRETTY, 0).diff
        );
    }

    @Test
    public void testIncludeAllSomeChanges() throws IOException {
        assertEquals(
            "{\n" +
            "  \"foo\": \"bar\",\n" +
            "  \"qux\": \"quux\",\n" +
            "  \"x\": ((\"y\" --> \"z\"))\n" +
            "}",
            new JsonSemanticDiffer()
                .semanticDiff("{\"x\":\"y\", \"foo\":\"bar\", \"qux\":\"quux\"}",
                    "{\"x\":\"z\", \"foo\":\"bar\", \"qux\":\"quux\"}",
                    Verbosity.COMPLETE_VERBOSITY, FormatRecommendation.PRETTY, 0).diff);
    }

    @Test
    public void testIncludeChangesOnlyIdentical() throws IOException {
        assertEquals(
            "[\n" +
            "]",
            new JsonSemanticDiffer()
                .semanticDiff("[1,2,3,4,5,6,7]", "[1,2,3,4,5,6,7]", Verbosity.CHANGES_ONLY_VERBOSITY,
                    FormatRecommendation.PRETTY, 0).diff
        );
    }

    @Test
    public void testIncludeChangesOnlySomeChanges() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());

        vm.setDisplayName("some-display-name");
        final String j2 = gson.toJson(vm.build());

        assertEquals("{\n" +
                "++\"displayName\": \"some-display-name\"\n" +
                "}",
            new JsonSemanticDiffer().semanticDiff(j1, j2, Verbosity.CHANGES_ONLY_VERBOSITY,
                FormatRecommendation.PRETTY, 0).diff);
    }

    @Test
    public void testIncludeChangesOnlyObjectAddition() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());

        vm.addCommoditySoldList(
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setKey("bar")
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(20.0)
                .setCapacity(30.0)
        );
        final String j2 = gson.toJson(vm.build());

        assertEquals("{\n" +
                "  \"commoditySoldList\": [\n" +
                "++++{\n" +
                "++++++\"capacity\": 30.0,\n" +
                "++++++\"commodityType\": {\n" +
                "++++++++\"key\": \"bar\",\n" +
                "++++++++\"type\": 53 [[VMEM]]\n" +
                "++++++},\n" +
                "++++++\"used\": 20.0\n" +
                "++++}\n" +
                "  ]\n" +
                "}",
            new JsonSemanticDiffer().semanticDiff(j1, j2, Verbosity.CHANGES_ONLY_VERBOSITY,
                FormatRecommendation.PRETTY, 0).diff);
    }

    @Test
    public void testIncludeChangesOnlyObjectCompact() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());

        vm.addCommoditySoldList(
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setKey("bar")
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(20.0)
                .setCapacity(30.0)
        );
        final String j2 = gson.toJson(vm.build());

        assertEquals("{\"commoditySoldList\":[++{\"capacity\":30.0,\"commodityType\":{\"key\":\"bar\",\"type\":53[[VMEM]]},\"used\":20.0}]}",
            new JsonSemanticDiffer().semanticDiff(j1, j2, Verbosity.CHANGES_ONLY_VERBOSITY,
                FormatRecommendation.COMPACT, 0).diff);
    }

    @Test
    public void testIncludeChangesOnlyObjectCompactAddMaxQuantity() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());

        vm.getCommoditySoldListBuilderList().get(0)
            .getHistoricalUsedBuilder().setMaxQuantity(12.9);
        final String j2 = gson.toJson(vm.build());

        assertEquals("{\"commoditySoldList\":[{\"commodityType\":{\"key\":\"foo\",\"type\":26[[VCPU]]},++\"historicalUsed\":{\"maxQuantity\":12.9}}]}",
            new JsonSemanticDiffer().semanticDiff(j1, j2, Verbosity.CHANGES_ONLY_VERBOSITY,
                FormatRecommendation.COMPACT, 0).diff);
    }

    @Test
    public void testIncludeChangesOnlyObjectRemoval() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());

        vm.setDisplayName("some-display-name");
        final String j2 = gson.toJson(vm.build());

        assertEquals("{\n" +
                "++\"displayName\": \"some-display-name\"\n" +
                "}",
            new JsonSemanticDiffer().semanticDiff(j1, j2, Verbosity.CHANGES_ONLY_VERBOSITY,
                FormatRecommendation.PRETTY, 0).diff);
    }

    @Test
    public void testDiffDoesNotGenerateNullPointer() throws IOException {
        final String a = "[\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_LATENCY\",\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": true,\n" +
            "        \"storageLatencyData\": {\n" +
            "            \"supportsStorageLatency\": false\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::7505d8de-c1fe-11e3-9d19-40f2e963e430\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::f5c81d9f-6bd4-11e4-87c1-c8bc68aeba66\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::ff8504f0-c206-11e3-8c5f-40f2e96403a8\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_CLUSTER\",\n" +
            "        \"key\": \"free_storage_cluster\",\n" +
            "        \"capacity\": 1.0E9\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_ACCESS\",\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": true,\n" +
            "        \"storageAccessData\": {\n" +
            "            \"supportsStorageIOPS\": false\n" +
            "        }\n" +
            "    }\n" +
            "]";

        final String b = "[\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_LATENCY\",\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": true,\n" +
            "        \"storageLatencyData\": {\n" +
            "            \"supportsStorageLatency\": false\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::7505d8de-c1fe-11e3-9d19-40f2e963e430\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::c560c06e-b99f-11e1-8828-5cf3fcfce578\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::f5c81d9f-6bd4-11e4-87c1-c8bc68aeba66\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"DSPM_ACCESS\",\n" +
            "        \"key\": \"PhysicalMachine::ff8504f0-c206-11e3-8c5f-40f2e96403a8\",\n" +
            "        \"used\": 1.0,\n" +
            "        \"capacity\": 1.0E9,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": false\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_CLUSTER\",\n" +
            "        \"key\": \"free_storage_cluster\",\n" +
            "        \"capacity\": 1.0E9\n" +
            "    },\n" +
            "    {\n" +
            "        \"commodityType\": \"STORAGE_ACCESS\",\n" +
            "        \"used\": 0.0,\n" +
            "        \"active\": true,\n" +
            "        \"resizable\": true,\n" +
            "        \"storageAccessData\": {\n" +
            "            \"supportsStorageIOPS\": false\n" +
            "        }\n" +
            "    }\n" +
            "]";

        assertEquals("[\n" +
            "++{\n" +
            "++++\"active\": true,\n" +
            "++++\"capacity\": 1.0E9,\n" +
            "++++\"commodityType\": \"DSPM_ACCESS\",\n" +
            "++++\"key\": \"PhysicalMachine::c560c06e-b99f-11e1-8828-5cf3fcfce578\",\n" +
            "++++\"resizable\": false,\n" +
            "++++\"used\": 1.0\n" +
            "++},\n" +
            "  {\n" +
            "    \"commodityType\": \"STORAGE_ACCESS\",\n" +
            "++++\"used\": 0.0,\n" +
            "    ... 3 additional fields ...\n" +
            "  },\n" +
            "  ... 5 additional fields ...\n" +
            "]", new JsonSemanticDiffer().semanticDiff(a, b).diff);
    }

    @Test
    public void testTopologyDtoEntityAndCommodityTypes() throws IOException {
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
            .setOid(1234L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setKey("foo")
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setUsed(20.0)
                    .setCapacity(30.0)
            );

        final Gson gson = ComponentGsonFactory.createGson();
        final String j1 = gson.toJson(vm.build());
        final String j2 = gson.toJson(vm
            .setAnalysisSettings(AnalysisSettings.newBuilder().setCloneable(false))
            .build());

        assertEquals("{\n" +
            "++\"analysisSettings\": {\n" +
            "++++\"cloneable\": false\n" +
            "++},\n" +
            "  \"commoditySoldList\": [\n" +
            "    {\n" +
            "      \"capacity\": 30.0,\n" +
            "      \"commodityType\": {\n" +
            "        \"key\": \"foo\",\n" +
            "        \"type\": 26 [[VCPU]]\n" +
            "      },\n" +
            "      \"used\": 20.0\n" +
            "    }\n" +
            "  ],\n" +
            "  \"entityType\": 10 [[VIRTUAL_MACHINE]],\n" +
            "  \"oid\": \"1234\"\n" +
            "}", new JsonSemanticDiffer().semanticDiff(j1, j2).diff);
    }

    @Test
    public void testPreambleOnlyEmptyDiff() throws IOException {
        final DiffOutput output = new JsonSemanticDiffer()
            .semanticDiff("{\"x\":\"y\", \"foo\":\"bar\", \"qux\":\"quux\"}",
                "{\"x\":\"z\", \"foo\":\"bar\", \"qux\":\"quux\"}", Verbosity.PREAMBLE_ONLY_VERBOSITY,
                FormatRecommendation.PRETTY, 0);

        assertFalse(output.identical);
        assertEquals("", output.diff);
    }
}