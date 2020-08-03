package com.vmturbo.matrix.component;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.matrix.component.OverlayVertex.VertexID;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.matrix.component.external.WeightClass;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentityData;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO.Protocol;

/**
 * Matrix tests.
 */
public class CommunicationMatrixTest {
    /**
     * The temp folder.
     */
    @Rule
    public TemporaryFolder tempFiles = new TemporaryFolder();

    /**
     * The center OID.
     */
    static final long CENTER_OID = Long.MAX_VALUE;

    /**
     * Sets things up.
     */
    @Before
    public void setUp() {
        System.setProperty("vmt.matrix.updatecycle", "0");
    }

    /**
     * NOOP for now.
     */
    @After
    public void tearDown() {
    }

    private static @Nonnull EntityIdentityData endpoint(final @Nonnull String ipAddress,
                                                        final int port) {
        return EntityIdentityData.newBuilder().setIpAddress(ipAddress).setPort(port).build();
    }

    private static @Nonnull FlowDTO edge(final double flow,
                                         final int latency,
                                         final Protocol protocol,
                                         final EntityIdentityData source,
                                         final EntityIdentityData sink) {
        return edge(flow, latency, (long)(flow / 2), (long)(flow / 2), protocol, source, sink);
    }

    private static @Nonnull FlowDTO edge(final double flow,
                                         final int latency,
                                         final long tx,
                                         final long rx,
                                         final Protocol protocol,
                                         final EntityIdentityData source,
                                         final EntityIdentityData sink) {
        return FlowDTO.newBuilder().setFlowAmount((long)flow).setLatency(latency)
                      .setTransmittedAmount(tx).setReceivedAmount(rx)
                      .setProtocol(protocol).setSourceEntityIdentityData(source)
                      .setDestEntityIdentityData(sink).build();
    }

    /**
     * Generates IP from index.
     *
     * @param prefix The prefix.
     * @param index  The index.
     * @return The IP address.
     */
    static @Nonnull String ipFromIndex(final @Nonnull String prefix, final int index) {
        int x = index;
        int z = x % 256;
        x -= z;
        x /= 255;
        int y = x % 256;
        int x1 = x - y;
        x1 /= 255;
        x1 %= 256;
        return prefix + "." + x1 + "." + y + "." + z;
    }

    /**
     * Constructs a list of edges.
     *
     * @param count         The count. Must be less than 255 * 255
     * @param sourceIPStart The starting source IP index (when constructing IP address, we use
     *                      that as starting number which will get increased). If {@code -1},
     *                      then use static address.
     * @param sinkPort      The sink port.
     * @param sinkIPStart   The starting sink IP index (same as source in use with exception to
     *                      the {@code -1} case).
     * @param latency       The latency.
     * @param flow          The flow.
     * @param protocol      The protocol.
     * @return The list of edges.
     */
    static List<FlowDTO> constructEdges(final int count,
                                        final int sourceIPStart,
                                        final int sinkPort,
                                        final int sinkIPStart,
                                        final int latency,
                                        final double flow,
                                        final Protocol protocol) {
        List<FlowDTO> edges = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String sourceIPAddr = "1.1.1.1";
            if (sourceIPStart > 0) {
                sourceIPAddr = ipFromIndex("1", sourceIPStart + i);
            }
            EntityIdentityData source = endpoint(sourceIPAddr, 0);
            EntityIdentityData sink = endpoint(ipFromIndex("2", sinkIPStart + i),
                                               sinkPort);
            edges.add(edge(flow, latency, protocol, source, sink));
        }
        return edges;
    }

    /**
     * Tests 2 to 1.
     */
    @Test
    public void testTwoToOne() {
        EntityIdentityData source1 = endpoint("10.11.10.1", 0);
        EntityIdentityData source2 = endpoint("10.11.10.2", 0);
        EntityIdentityData sink1 = endpoint("10.11.10.3", 80);
        EntityIdentityData sink2 = endpoint("10.11.10.3", 8080);
        FlowDTO edge1 = edge(11, 12, Protocol.TCP, source1, sink1);
        FlowDTO edge2 = edge(11, 12, Protocol.TCP, source2, sink2);
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(Arrays.asList(edge1, edge2));
        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
    }

    /**
     * Retiurns the set of uniq VPOds for the matrix.
     *
     * @param matrix The matrix.
     * @return The set of unique VPoDs.
     */
    static @Nonnull Set<MatrixVPod> getUniqueVPods(final @Nonnull CommunicationMatrix matrix) {
        return new HashSet<>(matrix.overlayNetwork_.state_.pods_.values());
    }

    /**
     * Simple test.
     */
    @Test
    public void testSimple() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(1, 1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
    }

    /**
     * VPod merge test.
     */
    @Test
    public void testMerge() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        List<FlowDTO> list = constructEdges(2, 1, 8080, 1, 1, 10,
                                            Protocol.UDP);
        matrix.update(list);
        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(2, pods.size());

        // Connect from the sink of the first edge to the source of the second edge.
        EntityIdentityData source = endpoint("1.0.0.2", 0);
        EntityIdentityData sink = endpoint("2.0.0.1", 8080);
        FlowDTO edge =
            edge(11, 12, Protocol.TCP, source, sink);
        matrix.overlayNetwork_.modifyFlow(matrix.overlayNetwork_.state_, edge);
        pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
        Assert.assertEquals(3, pods.iterator().next().getEdges().size());
    }

    /**
     * Tests invalid underlay DTO.
     */
    @Test
    public void testOverlayInvalidDTO() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        EntityIdentityData source = endpoint("fe80::b..3e1:2c1", 0);
        EntityIdentityData sink = endpoint("192.168.100.5", 80);
        FlowDTO dto = edge(1., 1, Protocol.TCP, source, sink);
        matrix.overlayNetwork_.modifyFlow(matrix.overlayNetwork_.state_, dto);
        Assert.assertEquals(0, matrix.overlayNetwork_.state_.edges_.size());
        source = endpoint("192.168.100.1", 0);
        dto = edge(1., 1, Protocol.TCP, source, sink);
        matrix.overlayNetwork_.modifyFlow(matrix.overlayNetwork_.state_, dto);
        Assert.assertEquals(1, matrix.overlayNetwork_.state_.edges_.size());
    }

    /**
     * Simple modify test.
     */
    @Test
    public void testSimpleWithModify() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(1, 1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
        matrix.overlayNetwork_.modifyFlow(matrix.overlayNetwork_.state_,
                                          constructEdges(1, 1, 8080, 1, 1, 10,
                                                         Protocol.TCP)
                                              .get(0));
        Set<MatrixVPod> podsAfter = getUniqueVPods(matrix);
        Assert.assertEquals(1, podsAfter.size());
        Assert.assertEquals(pods, podsAfter);
    }

    /**
     * Test with GC.
     */
    @Test
    public void testWithGC() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(1, 1, 80, 1, 1, 1,
                                     Protocol.TCP));

        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());

        matrix.update(constructEdges(3, 100, 80, 101, 2, 11,
                                     Protocol.TCP));

        pods = getUniqueVPods(matrix);
        Assert.assertEquals(3, pods.size());

    }

    /**
     * Constellation network topology test.
     */
    @Test
    public void testConstellation() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(100, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));

        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
    }

    /**
     * Tests get one VPoD.
     */
    @Test
    public void testGetOneVpod() {
        List<FlowDTO> edges = constructEdges(10, -1, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(edges);
        Collection<Collection<String>> vpods = matrix.getVpods();
        Assert.assertEquals(1, vpods.size());
        Assert.assertEquals(11, vpods.iterator().next().size());
    }

    /**
     * Tests get a VPoD.
     */
    @Test
    public void testGetVpod() {
        List<FlowDTO> edges = constructEdges(100, 100, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(edges);
        Collection<Collection<String>> vpods = matrix.getVpods();
        Assert.assertEquals(100, vpods.size());
        for (Collection<String> vpod : vpods) {
            Assert.assertEquals(2, vpod.size());
        }
    }

    /**
     * Checks all the edges.
     * For every edge, we have a check which consists of last digit from source and sink IP
     * addresses. The IP addresses are assumed to be: "1.1.1.x".
     * That way we can verify that all vertices are connected correctly using all the correct edges.
     *
     * @param edges  The edges.
     * @param checks The checks.
     */
    private void checkVertices(final @Nonnull Collection<OverlayEdge> edges,
                               final @Nonnull String[] checks) {
        Assert.assertEquals(edges.size(), checks.length);
        Set<String> checkSet = new HashSet<>(Arrays.asList(checks));
        Map<String, Boolean> results = new HashMap<>();
        for (final OverlayEdge edge : edges) {
            final String sourceIP = edge.getSource().getNormalizedIpAddress();
            final String sinkIP = edge.getSink().getNormalizedIpAddress();
            final String key = sourceIP.charAt(sourceIP.length() - 1) + "->" +
                               sinkIP.charAt(sinkIP.length() - 1);
            // Make sure we don't check one edge more than once.
            Assert.assertTrue(checkSet.contains(key));
            Assert.assertFalse(results.containsKey(key));
            results.put(key, Boolean.TRUE);
        }
        // Yes, we hit all the edges.
        Assert.assertEquals(results.keySet(), checkSet);
        // Check whether all flags are still there.
        results.forEach((k, v) -> Assert.assertTrue(v));
    }

    /**
     * Test the following scenario:
     * <pre>
     * Now:
     * V1 -> V2
     * V3 -> V4
     *
     * Was:
     * V2 -> V5
     * V5 -> V3
     * V5 -> V6
     *
     * So, if we don't include V2 -> V5 and V5 -> V3, we will lose V2 -> V5 -> V3 link,
     * which allows us to form V1 -> V2 -> V5 -> V3 -> V4 VPoD.
     * The V5 -> V6 will not be ignored, as we need to make sure we handle multiple probes
     * covering different network segments.
     * </pre>
     */
    @Test
    public void testFading() {
        final CommunicationMatrix matrix = new CommunicationMatrix();
        final EntityIdentityData v1ToS = endpoint("1.1.1.1", 0);
        final EntityIdentityData v2ToS = endpoint("1.1.1.2", 0);
        final EntityIdentityData v2ToT = endpoint("1.1.1.2", 443);
        final EntityIdentityData v3ToS = endpoint("1.1.1.3", 0);
        final EntityIdentityData v3ToT = endpoint("1.1.1.3", 80);
        final EntityIdentityData v4ToT = endpoint("1.1.1.4", 8080);
        final EntityIdentityData v5ToS = endpoint("1.1.1.5", 0);
        final EntityIdentityData v5ToT = endpoint("1.1.1.5", 80);
        final EntityIdentityData v6ToT = endpoint("1.1.1.6", 8080);

        // Check vertices.
        OverlayVertex mv3ToS = new OverlayVertex(v3ToS.getIpAddress(), v3ToS.getPort());
        OverlayVertex mv3ToT = new OverlayVertex(v3ToT.getIpAddress(), v3ToT.getPort());
        Assert.assertNotEquals(mv3ToS, mv3ToT);
        Map<OverlayVertex, Boolean> vertices = new HashMap<>();
        vertices.put(mv3ToS, Boolean.TRUE);
        Assert.assertNull(vertices.get(mv3ToT));

        // Construct "WAS".
        FlowDTO edgeFrom2To5 = edge(4., 10, Protocol.TCP, v2ToS, v5ToT);
        FlowDTO edgeFrom5To3 = edge(4., 10, Protocol.TCP, v5ToS, v3ToT);
        FlowDTO edgeFrom5To6 = edge(4., 10, Protocol.TCP, v5ToS, v6ToT);

        matrix.update(Arrays.asList(edgeFrom2To5, edgeFrom5To3, edgeFrom5To6));
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(3, matrix.overlayNetwork_.state_.edges_.size());
        // Expecting: V2 -> V5 -> V3 and V5 -> V6, so one VPoD (connected through V5).
        checkVertices(matrix.overlayNetwork_.state_.edges_, new String[]{"2->5", "5->3", "5->6"});

        // Construct "NOW"
        FlowDTO edgeFrom1To2 = edge(2., 10, Protocol.TCP, v1ToS, v2ToT);
        FlowDTO edgeFrom3To4 = edge(2., 10, Protocol.TCP, v3ToS, v4ToT);
        matrix.update(Arrays.asList(edgeFrom1To2, edgeFrom3To4));
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(5, matrix.overlayNetwork_.state_.edges_.size());
        // Check all the edges.
        // Expecting: V1 -> V2 -> V5 -> V3 -> V4  and V5 -> V6
        checkVertices(matrix.overlayNetwork_.state_.edges_,
                      new String[]{"1->2", "2->5", "5->3", "3->4", "5->6"});

        // Re-apply. We should see all the old edges fade-in.
        edgeFrom1To2 = edge(2., 10, Protocol.TCP, v1ToS, v2ToT);
        edgeFrom3To4 = edge(2., 10, Protocol.TCP, v3ToS, v4ToT);
        matrix.update(Arrays.asList(edgeFrom1To2, edgeFrom3To4));
        Assert.assertEquals(2, getUniqueVPods(matrix).size());
        Assert.assertEquals(2, matrix.overlayNetwork_.state_.edges_.size());
        // Expecting: V1 -> V2 and V3 ->V4
        checkVertices(matrix.overlayNetwork_.state_.edges_, new String[]{"1->2", "3->4"});
    }

    /**
     * Tests star network topology edge modification.
     */
    @Test
    public void testConstellationEdgeMod() {
        final int edgesCount = 10000;
        List<FlowDTO> edges = constructEdges(edgesCount, -1, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        // One loop for verification.
        for (int i = 0; i < 10; i++) {
            matrix.update(edges);
            Set<MatrixVPod> pods = getUniqueVPods(matrix);
            Assert.assertEquals(1, pods.size());
            Assert.assertEquals(10 * edges.size(), pods.iterator().next().getPodFlow(), 0.0001D);
        }
    }

    /**
     * Stress tests star network topology edge modification.
     */
    @Test
    public void testStressConstellationEdgeMod() {
        final int edgesCount = 100000;
        List<FlowDTO> edges = constructEdges(edgesCount, -1, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(edges);

        long start = System.currentTimeMillis();
        matrix.update(edges);
        System.out.printf("Mod: %s edges: %dms.\n",
                          NumberFormat.getNumberInstance(Locale.US)
                                      .format(matrix.overlayNetwork_.state_.edges_.size()),
                          +(System.currentTimeMillis() - start));
    }

    /**
     * Only for readability.
     *
     * @param ipAddress IP Address.
     * @return Normalized IP address.
     */
    private String normIP(final @Nonnull String ipAddress) {
        return OverlayVertex.normalizeIPAddress(ipAddress);
    }

    /**
     * Tests IP normalization.
     */
    @Test
    public void testIPNormalization() {
        Assert.assertEquals(normIP("1.1.1.1"), normIP("1.1.1.1"));
        Assert.assertNotEquals(normIP("1.1.1.1"), normIP("1.1.1.2"));
        // The following are per https://en.wikipedia.org/wiki/IPv6 :
        // For convenience, an IPv6 address may be abbreviated to shorter notations by
        // application of the following rules:
        // One or more leading zeroes from any groups of hexadecimal digits are removed; this is
        // usually done to either all or none of the leading zeroes. For example, the group 0042
        // is converted to 42.
        // Consecutive sections of zeroes are replaced with a double colon (::). The double colon
        // may only be used once in an address, as multiple use would render the address
        // indeterminate. RFC 5952 recommends that a double colon not be used to denote an
        // omitted single section of zeroes.
        Assert.assertEquals(normIP("2001:0db8:0000:0000:0000:ff00:0042:8329"),
                            normIP("2001:db8:0:0:0:ff00:42:8329"));
        Assert.assertEquals(normIP("2001:0db8:0000:0000:0000:ff00:0042:8329"),
                            normIP("2001:db8::ff00:42:8329"));
        Assert.assertEquals(normIP("2001:db8:0:0:0:ff00:42:8329"),
                            normIP("2001:db8::ff00:42:8329"));
    }

    /**
     * Clone test.
     */
    @Test
    public void testClone() {
        final int edgesCount = 100;
        List<FlowDTO> edges = constructEdges(edgesCount, -1, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        // One loop for verification.
        matrix.update(edges);
        matrix.resetUnderlay();
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateDpod(new HashSet<>(Arrays.asList(10L, 20L)));
        Set<MatrixVPod> pods = getUniqueVPods(matrix);
        Assert.assertEquals(1, pods.size());
        Assert.assertEquals(10 * edges.size(), pods.iterator().next().getPodFlow(), 0.0001D);
        // This one will be ignored.
        matrix.setCapacities(null, new double[]{10., 25., 3., 4.}, new double[]{1., 1., 1., 1.});
        matrix.setCapacities(10L, new double[]{1., 2., 3., 4.}, new double[]{1., 1., 1., 1.});

        // Check the copy.
        MatrixInterface mi = matrix.copy();
        CommunicationMatrix cm = (CommunicationMatrix)mi;
        Assert.assertEquals(cm.overlayNetwork_.state_.edges_,
                            matrix.overlayNetwork_.state_.edges_);
        Assert.assertArrayEquals(cm.underlayNetwork_.entities_
                                     .get(new VolatileLong(10L)).capacities_,
                                 matrix.underlayNetwork_.entities_
                                     .get(new VolatileLong(10L)).capacities_,
                                 0.000001);
        // All Java objects must be newly constructed.
        // Edges:
        matrix.overlayNetwork_.state_.edges_
            .forEach(es -> cm.overlayNetwork_.state_.edges_
                               .forEach(et -> Assert.assertNotSame(es, et)));
        // Vertices
        matrix.overlayNetwork_.state_.vertices_
            .keySet()
            .forEach(vs -> cm.overlayNetwork_
                               .state_.vertices_.keySet()
                                                .forEach(vt -> Assert.assertNotSame(vs, vt)));

        Assert.assertEquals(cm.overlayNetwork_.state_.pods_.keySet(),
                            matrix.overlayNetwork_.state_.pods_.keySet());
        Set<MatrixVPod> podsSource = getUniqueVPods(matrix);
        Set<MatrixVPod> podsCopy = getUniqueVPods(cm);
        Assert.assertEquals(podsSource.size(), podsCopy.size());
        // Iterate over all VPoDs, and eliminate every copy VPoD having the same edges as the
        // source. In the end, the copy should be empty.
        for (MatrixVPod vPodSource : podsSource) {
            for (MatrixVPod vPodCopy : podsCopy) {
                if (vPodSource.getEdges().equals(vPodCopy.getEdges())) {
                    podsCopy.remove(vPodCopy);
                    break;
                }
            }
        }
        Assert.assertTrue(podsCopy.isEmpty());
    }

    /**
     * Tests constellation network topology edge copy.
     */
    @Test
    public void testStressConstellationEdgeCopy() {
        final int edgesCount = 100000;
        List<FlowDTO> edges = constructEdges(edgesCount, -1, 8080, 1, 1, 10,
                                             Protocol.TCP);
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(edges);
        for (int i = 0; i < edgesCount; i++) {
            matrix.populateUnderlay(1000000 + i + 1, 10000000L);
        }
        for (int i = 0; i < edgesCount; i++) {
            String ip = ipFromIndex("2", 1 + i);
            matrix.setEndpointOID(i + 1, ip);
        }
        for (int i = 0; i < edgesCount; i++) {
            matrix.place(i + 1, 1000000 + i + 1);
        }

        long start = System.currentTimeMillis();
        MatrixInterface mi = matrix.copy();
        Assert.assertEquals(((CommunicationMatrix)mi).overlayNetwork_.state_.edges_.size(),
                            matrix.overlayNetwork_.state_.edges_.size());
        System.out.printf("Copy: %s edges: %dms.\n",
                          NumberFormat.getNumberInstance(Locale.US)
                                      .format(matrix.overlayNetwork_.state_.edges_.size()),
                          +(System.currentTimeMillis() - start));
    }

    /**
     * Tests vertex flows.
     */
    @Test
    public void testVertexFlows() {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        Assert.assertTrue(matrix.isEmpty());
        matrix.update(constructEdges(3, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        matrix.populateUnderlay(1L, 1000L);
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateUnderlay(40L, 1000L);
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(110L, "1.1.1.10");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        matrix.place(110L, 10L);
        matrix.place(101L, 1L);
        matrix.place(201L, 20L);
        matrix.place(202L, 30L);
        matrix.place(203L, 202L);
        Assert.assertFalse(matrix.isEmpty());
        Assert.assertFalse(matrix.getEndpointFlows(110L).length > 0);
        // We have this one.
        Assert.assertEquals(0, matrix.getEndpointFlows(null).length);
        double[] flows = matrix.getEndpointFlows(101L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(30., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        // Run projected flows
        flows = matrix.getProjectedFlows(101L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(30., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        // Run projected flows
        flows = matrix.getProjectedFlows(201L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(10., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        // Run projected flows for Container 203L on top of VM 202L
        flows = matrix.getProjectedFlows(203L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(10., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        flows = matrix.overlayNetwork_.getEndpointFlows(null, null, matrix.underlayNetwork_);
        Assert.assertEquals(0, flows.length);
    }

    /**
     * Tests non-populated vertex flows.
     */
    @Test
    public void testVertexFlowsNotPopulated() {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(2, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        // We have this one.
        double[] flows = matrix.getEndpointFlows(101L);
        Assert.assertFalse(flows.length > 0);
    }

    /**
     * Tests partially populated vertex flows.
     */
    @Test
    public void testVertexFlowsPartiallyPopulated() {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(2, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        matrix.populateUnderlay(1L, 1000L);
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateUnderlay(40L, 1000L);
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.place(101L, 1L);
        matrix.place(201L, 10L);
        matrix.place(202L, 20L);
        // We have this one.
        Assert.assertEquals(0, matrix.getProjectedFlows(null, 40L).length);
        double[] flows = matrix.getProjectedFlows(101L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(20., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
    }

    private long stressFlowsLong(final @Nonnull CommunicationMatrix matrix) {
        final long start = System.currentTimeMillis();
        // We have this one.
        // Run projected flows
        // Since we add 100,000 edges, we have a constellation of 1 vertex with 99,999 other
        // vertices. Each edge has a flow of 1 for ease of calculation.
        double[] flows = matrix.getProjectedFlows(CENTER_OID, 1000001L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(1., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(matrix.overlayNetwork_.state_.edges_.size() - 1,
                            flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        return System.currentTimeMillis() - start;
    }

    private @Nonnull CommunicationMatrix constructMatrixForFlowTests(final int count) {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(count, -1, 8080, 1, 1, 1,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        final long underlayStart = 1000000L;
        for (int i = 0; i < count; i++) {
            matrix.populateUnderlay(underlayStart + i + 1, 999L);
        }
        for (int i = 0; i < count; i++) {
            String ip = ipFromIndex("2", 1 + i);
            matrix.setEndpointOID(i + 1, ip);
        }
        for (int i = 0; i < count; i++) {
            matrix.place(i + 1, underlayStart + i + 1);
        }

        matrix.setEndpointOID(CENTER_OID, "1.1.1.1");
        matrix.place(CENTER_OID, 999L);
        Assert.assertEquals(matrix.overlayNetwork_.state_.edges_.size(),
                            count);
        for (OverlayVertex vertex : matrix.overlayNetwork_.state_.vertices_.keySet()) {
            Assert.assertNotNull(vertex.oid_);
            Assert.assertNotEquals(0L, vertex.oid_.getValue());
        }
        return matrix;
    }

    /**
     * Tests exclusion - center in a star network topology.
     */
    @Test
    public void testExcludeCenter() {
        int count = 10;
        CommunicationMatrix matrix = constructMatrixForFlowTests(count);
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
        // After this, no flows remain.
        Set<String> ips = new HashSet<>();
        ips.add("1.1.1.1");
        matrix.exclude(ips);
        Assert.assertEquals(count, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
    }

    /**
     * Tests exclusion - two endpoints in a star network topology.
     */
    @Test
    public void testExcludeTwo() {
        int count = 100;
        CommunicationMatrix matrix = constructMatrixForFlowTests(count);
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
        // After this, no flows remain.
        Set<String> ips = new HashSet<>();
        ips.add("2.0.0.1");
        ips.add("2.0.0.2");
        matrix.exclude(ips);
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
    }

    /**
     * Tests exclusion - two endpoints in a star network topology.
     * Use large amount of endpoints.
     */
    @Test
    public void testExcludeTwoLargeCount() {
        int count = 100000;
        CommunicationMatrix matrix = constructMatrixForFlowTests(count);
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
        // After this, no flows remain.
        Set<String> ips = new HashSet<>();
        ips.add("2.0.0.1");
        ips.add("2.0.0.2");
        matrix.exclude(ips);
        Assert.assertEquals(count - 2, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
    }

    /**
     * Tests no exclusions.
     */
    @Test
    public void testExcludeNone() {
        int count = 3;
        CommunicationMatrix matrix = constructMatrixForFlowTests(count);
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
        for (OverlayVertex vertex : matrix.overlayNetwork_.state_.vertices_.keySet()) {
            if (vertex.getNormalizedIpAddress().equals("1.1.1.1")) {
                Assert.assertEquals(3, vertex.getNeighbours().size());
            }
        }
        // After this, no flows remain.
        Set<String> ips = new HashSet<>();
        matrix.exclude(ips);
        for (OverlayVertex vertex : matrix.overlayNetwork_.state_.vertices_.keySet()) {
            if (vertex.getNormalizedIpAddress().equals("1.1.1.1")) {
                Assert.assertEquals(3, vertex.getNeighbours().size());
            }
        }
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(count, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(count + 1, matrix.overlayNetwork_.state_.vertices_.size());
    }

    /**
     * Tests exclusion for VPoD.
     */
    @Test
    public void testExcludeVpod() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        EntityIdentityData v1ToS = endpoint("1.1.1.1", 0);
        EntityIdentityData v2ToS = endpoint("1.1.1.2", 0);
        EntityIdentityData v2ToT = endpoint("1.1.1.2", 443);
        EntityIdentityData v3ToS = endpoint("1.1.1.3", 0);
        EntityIdentityData v3ToT = endpoint("1.1.1.3", 80);
        EntityIdentityData v4ToT = endpoint("1.1.1.4", 8080);
        EntityIdentityData v5ToS = endpoint("1.1.1.5", 0);
        EntityIdentityData v5ToT = endpoint("1.1.1.5", 80);
        EntityIdentityData v6ToT = endpoint("1.1.1.6", 8080);

        FlowDTO edgeFrom1To2 = edge(2., 10, Protocol.TCP, v1ToS, v2ToT);
        FlowDTO edgeFrom3To4 = edge(2., 10, Protocol.TCP, v3ToS, v4ToT);
        FlowDTO edgeFrom2To5 = edge(4., 10, Protocol.TCP, v2ToS, v5ToT);
        FlowDTO edgeFrom5To3 = edge(4., 10, Protocol.TCP, v5ToS, v3ToT);
        FlowDTO edgeFrom5To6 = edge(4., 10, Protocol.TCP, v5ToS, v6ToT);

        // V1 -> V2 -> V5 -> V3 -> V4
        //               |-> V6
        //
        matrix.update(Arrays.asList(edgeFrom1To2, edgeFrom3To4, edgeFrom2To5, edgeFrom5To3, edgeFrom5To6));
        Assert.assertEquals(1, getUniqueVPods(matrix).size());
        Assert.assertEquals(5, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(9, matrix.overlayNetwork_.state_.vertices_.size());

        // OIDs.
        matrix.setEndpointOID(1, "1.1.1.1");
        matrix.setEndpointOID(2, "1.1.1.2");
        matrix.setEndpointOID(3, "1.1.1.2");
        matrix.setEndpointOID(4, "1.1.1.3");
        matrix.setEndpointOID(5, "1.1.1.3");
        matrix.setEndpointOID(6, "1.1.1.4");
        matrix.setEndpointOID(7, "1.1.1.5");
        matrix.setEndpointOID(8, "1.1.1.5");
        matrix.setEndpointOID(9, "1.1.1.6");

        // Remove V5
        Set<String> ips = new HashSet<>();
        ips.add("1.1.1.5");
        matrix.exclude(ips);

        // Expect
        // V1 -> V2
        // V3 -> V4
        // V6
        Assert.assertEquals(3, getUniqueVPods(matrix).size());
        Assert.assertEquals(5, matrix.overlayNetwork_.state_.edges_.size());
        Assert.assertEquals(9, matrix.overlayNetwork_.state_.vertices_.size());

        int[] results = {0, 0, 0, 0, 0, 0};
        for (OverlayVertex vertex : matrix.overlayNetwork_.state_.vertices_.keySet()) {
            switch (vertex.getNormalizedIpAddress()) {
                case "1.1.1.1":
                    results[0] += vertex.getNeighbours().size();
                    break;
                case "1.1.1.2":
                    results[1] += vertex.getNeighbours().size();
                    break;
                case "1.1.1.3":
                    results[2] += vertex.getNeighbours().size();
                    break;
                case "1.1.1.4":
                    results[3] += vertex.getNeighbours().size();
                    break;
                case "1.1.1.5":
                    results[4] += vertex.getNeighbours().size();
                    break;
                case "1.1.1.6":
                    results[5] += vertex.getNeighbours().size();
                    break;
            }
        }
        // Check all of them to be present exactly once.
        Assert.assertEquals(1, results[0]);
        Assert.assertEquals(2, results[1]);
        Assert.assertEquals(2, results[2]);
        Assert.assertEquals(1, results[3]);
        Assert.assertEquals(3, results[4]);
        Assert.assertEquals(1, results[5]);
    }

    /**
     * Tests stress vertex flows.
     */
    @Test
    public void testStressVertexFlowsLongForDebug() {
        stressFlowsLong(constructMatrixForFlowTests(3));
    }

    /**
     * Tests stress vertex flows large.
     */
    @Test
    public void testStressVertexFlowsLong() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(200000);
        System.out.printf("Flows lots: %s edges: %.02fms.\n",
                          NumberFormat.getNumberInstance(Locale.US)
                                      .format(matrix.overlayNetwork_.state_.edges_.size()),
                          (double)stressFlowsLong(matrix));
    }

    /**
     * Tests stress vertex flows small.
     */
    @Test
    public void testStressVertexFlowsShort() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(100000);
        long start = System.currentTimeMillis();
        // We have this one.
        // Run projected flows
        // Since we add 100,000 edges, we have a constellation of 1 vertex with 99,999 other
        // vertices. Each edge has a flow of 1 for ease of calculation.
        // Small flow. We aren't talking to the guy already there.
        double[] flows = matrix.getProjectedFlows(11L, 1000040L);
        System.out.printf("Flows small: %s edges: %dms.\n",
                          NumberFormat.getNumberInstance(Locale.US)
                                      .format(matrix.overlayNetwork_.state_.edges_.size()),
                          +(System.currentTimeMillis() - start));
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(1., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
    }

    /**
     * Tests singleton matrix.
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testMatrixSingleton() throws Exception {
        Assert.assertNotNull(TheMatrix.instance());
        // Load the Holder.
        // We need this for the code coverage.
        Class<?>[] classes = TheMatrix.class.getDeclaredClasses();
        Assert.assertNotNull(classes);
        Assert.assertEquals(1, classes.length);
        Assert.assertTrue(classes[0].isMemberClass());
        Constructor<?> constructor = classes[0].getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object o = constructor.newInstance((Object[])null);
        Assert.assertNotNull(o);
    }

    /**
     * Tests edges.
     */
    @Test
    public void testEdge() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        OverlayEdge edge = new OverlayEdge(1, 10, 7, 3,
                                           source, sink);
        Assert.assertEquals(7, edge.getTx());
        Assert.assertEquals(3, edge.getRx());
    }

    /**
     * Tests accumulated data.
     */
    @Test
    public void testAccumulatedData() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        // We use flow/2 for both tx and rx for every edge.
        matrix.update(constructEdges(2, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            Assert.assertEquals(10, edge.getAccumulatedData());
        }
        matrix.update(constructEdges(2, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            Assert.assertEquals(20, edge.getAccumulatedData());
        }
    }

    /**
     * Tests vertex ID.
     */
    @Test
    public void testVertexID() {
        VertexID id1 = new VertexID("1.1.1.1", 10);
        VertexID id2 = new VertexID("1.1.1.1", 30);
        VertexID id3 = new VertexID("1.1.1.1", 0);
        VertexID id4 = new VertexID("1.1.1.2", 10);
        VertexID id5 = new VertexID("fe80::1%lo0", 443);
        Assert.assertEquals(id1, id1);
        Assert.assertNotEquals(id1, id3);
        Assert.assertNotEquals(id2, id3);
        Assert.assertNotEquals(id1, id2);
        Assert.assertNotEquals(id1, id4);
        Assert.assertNotEquals(id1, "1.1.1.1");
        Assert.assertNotEquals(id1, null);
        Assert.assertEquals("1.1.1.1:10", id1.toString());
        Assert.assertEquals("[fe80::1%lo0]:443", id5.toString());
    }

    /**
     * Tests empty flows update.
     */
    @Test
    public void testEmptyFlowsUpdate() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(Collections.emptyList());
    }

    /**
     * Tests named matrix.
     */
    @Test
    public void testTheMatrixNamed() {
        Long oldTopoId = 123L;
        Assert.assertFalse(TheMatrix.instance(oldTopoId).isPresent());
        Assert.assertTrue(TheMatrix.CACHE_.isEmpty());
        // No side effects.
        TheMatrix.clearInstance(oldTopoId);
        Assert.assertFalse(TheMatrix.instance(oldTopoId).isPresent());
        // Set the instance.
        Long newTopoId = 456L;
        TheMatrix.setInstance(newTopoId, new CommunicationMatrix());
        Assert.assertEquals(1, TheMatrix.CACHE_.size());
        Assert.assertTrue(TheMatrix.instance(newTopoId).isPresent());
        TheMatrix.clearInstance(newTopoId);
        Assert.assertFalse(TheMatrix.instance(newTopoId).isPresent());
        Assert.assertTrue(TheMatrix.CACHE_.isEmpty());
    }

    /**
     * Tests same IP vertices.
     */
    @Test
    public void testMultiVertexSameIp() {
        Set<OverlayVertex> set = new HashSet<>();
        set.add(new OverlayVertex("1.1.1.1", 0));
        set.add(new OverlayVertex("1.1.1.1", 1));
        set.add(new OverlayVertex("1.1.1.1", 2));
        Assert.assertEquals(3, set.size());
    }

    /**
     * Tests wrong dump.
     *
     * @throws IOException Expected.
     */
    @Test(expected = IOException.class)
    public void testDumpWrong() throws IOException {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.dump("/dev/zero/myfile.topology");
    }

    /**
     * Tests wrong restore.
     *
     * @throws IOException Expected.
     */
    @Test(expected = IOException.class)
    public void testRestoreWrong() throws IOException {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.restore("/dev/null/myfile.topology");
    }

    /**
     * Tests dump and restore.
     *
     * @throws IOException In case something is actually wrong with the infrastructure.
     */
    @Test
    public void testDumpAndRestore() throws IOException {
        CommunicationMatrix matrix = constructMatrixForFlowTests(10000);
        final File file = tempFiles.newFile();
        String fileName = file.getCanonicalPath();
        // Dump the data.
        matrix.dump(fileName);
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        matrixRestored.restore(fileName);
        // Checks
        Assert.assertEquals(matrix.overlayNetwork_.state_.edges_,
                            matrixRestored.overlayNetwork_.state_.edges_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.vertices_,
                            matrixRestored.overlayNetwork_.state_.vertices_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.pods_.keySet(),
                            matrixRestored.overlayNetwork_.state_.pods_.keySet());
        Assert.assertEquals(matrix.overlayNetwork_.state_.oidLookup_,
                            matrixRestored.overlayNetwork_.state_.oidLookup_);
        // We convert to TreeMap for a comparison so that we don't fail because of a different
        // restored order.
        Assert.assertEquals(new TreeMap<>(matrix.underlayNetwork_.entities_),
                            new TreeMap<>(matrixRestored.underlayNetwork_.entities_));
        Assert.assertEquals(new TreeMap<>(matrix.consumerToProvider_),
                            new TreeMap<>(matrixRestored.consumerToProvider_));
        Assert.assertEquals(matrix.getMapper().seToOid_, matrix.getMapper().seToOid_);
        Assert.assertEquals(matrix.getMapper().oidToSe_, matrix.getMapper().oidToSe_);
    }

    /**
     * Tests export/import.
     *
     * @throws IOException If there is an error.
     */
    @Test
    public void testExportImport() throws IOException {
        CommunicationMatrix matrix = constructMatrixForFlowTests(10000);
        matrix.setCapacities(1000015L, new double[]{11., 12., 13., 14.},
                             new double[]{1., 2., 3., 0.});
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.start(component);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
            }
        });
        // Checks
        Assert.assertEquals(matrix.overlayNetwork_.state_.edges_,
                            matrixRestored.overlayNetwork_.state_.edges_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.vertices_,
                            matrixRestored.overlayNetwork_.state_.vertices_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.pods_.keySet(),
                            matrixRestored.overlayNetwork_.state_.pods_.keySet());
        Assert.assertEquals(matrix.overlayNetwork_.state_.oidLookup_,
                            matrixRestored.overlayNetwork_.state_.oidLookup_);
        // We convert to TreeMap for a comparison so that we don't fail because of a different
        // restored order.
        Assert.assertEquals(new TreeMap<>(matrix.underlayNetwork_.entities_),
                            new TreeMap<>(matrixRestored.underlayNetwork_.entities_));
        Assert.assertEquals(new TreeMap<>(matrix.consumerToProvider_),
                            new TreeMap<>(matrixRestored.consumerToProvider_));
        Assert.assertEquals(matrix.getMapper().seToOid_, matrix.getMapper().seToOid_);
        Assert.assertEquals(matrix.getMapper().oidToSe_, matrix.getMapper().oidToSe_);
    }

    /**
     * Tests export/import with non-placed endpoints.
     *
     * @throws IOException If something is wrong.
     */
    @Test
    public void testExportImportNoPlacement() throws IOException {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(10, -1, 8080, 1, 1, 1,
                                     Protocol.TCP));
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.start(component);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
            }
        });
        // Checks
        Assert.assertEquals(matrix.overlayNetwork_.state_.edges_,
                            matrixRestored.overlayNetwork_.state_.edges_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.vertices_,
                            matrixRestored.overlayNetwork_.state_.vertices_);
        Assert.assertEquals(matrix.overlayNetwork_.state_.pods_.keySet(),
                            matrixRestored.overlayNetwork_.state_.pods_.keySet());
        Assert.assertEquals(matrix.overlayNetwork_.state_.oidLookup_,
                            matrixRestored.overlayNetwork_.state_.oidLookup_);
        // We convert to TreeMap for a comparison so that we don't fail because of a different
        // restored order.
        Assert.assertEquals(new TreeMap<>(matrix.underlayNetwork_.entities_),
                            new TreeMap<>(matrixRestored.underlayNetwork_.entities_));
        Assert.assertEquals(new TreeMap<>(matrix.consumerToProvider_),
                            new TreeMap<>(matrixRestored.consumerToProvider_));
        Assert.assertEquals(matrix.getMapper().seToOid_, matrix.getMapper().seToOid_);
        Assert.assertEquals(matrix.getMapper().oidToSe_, matrix.getMapper().oidToSe_);
    }

    /**
     * Tests export/import bad start stage.
     */
    @Test(expected = IllegalStateException.class)
    public void testExportImportFailNextOnStart() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(100);
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.next(null);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
            }
        });
    }

    /**
     * Tests export/import double finish.
     */
    @Test()
    public void testExportImportDoubleFinish() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(100);
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.start(component);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
                importer.finish();
            }
        });
    }

    /**
     * Tests export/import double start.
     */
    @Test(expected = IllegalStateException.class)
    public void testExportImportFailDoubleStart() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(100);
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.start(MatrixInterface.Component.OVERLAY);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
            }
        });
    }

    /**
     * Tests export/import start errors.
     */
    @Test(expected = IllegalStateException.class)
    public void testExportImportFailStartInProcess() {
        CommunicationMatrix matrix = constructMatrixForFlowTests(100);
        // Restore the data.
        CommunicationMatrix matrixRestored = new CommunicationMatrix();
        MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        //
        // Dump the data and restore it right away, thus simulating transfer over the wire..
        //
        matrix.exportMatrix(new MatrixInterface.Codec() {
            @Override public void start(@Nonnull MatrixInterface.Component component)
                throws IllegalStateException {
                importer.start(component);
                importer.start(component);
            }

            @Override public <T extends AbstractMessage> void next(@Nonnull T msg)
                throws IllegalStateException {
                importer.next(msg);
            }

            @Override public void finish() throws IllegalStateException {
                importer.finish();
            }
        });
    }

    /**
     * Tests place load.
     */
    @Test
    public void testPlaceLoad() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(3, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        matrix.populateUnderlay(1L, 1000L);
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateUnderlay(40L, 1000L);
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(110L, "1.1.1.10");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        double sum = 0L;
        final int maxRounds = Integer.getInteger("matrix.test.perf.rounds", 10);
        final long cycles = 1000000L;
        // Warm up.
        for (long i = 0; i < cycles; i++) {
            matrix.place(110L, 10L);
            matrix.place(101L, 1L);
            matrix.place(201L, 20L);
            matrix.place(202L, 30L);
            matrix.place(203L, 202L);
        }
        // The actual test.
        for (int rounds = 0; rounds < maxRounds; rounds++) {
            long start = System.currentTimeMillis();
            for (long i = 0; i < cycles; i++) {
                matrix.place(110L, 10L);
                matrix.place(101L, 1L);
                matrix.place(201L, 20L);
                matrix.place(202L, 30L);
                matrix.place(203L, 202L);
            }
            sum += System.currentTimeMillis() - start;
        }
        System.out.println(cycles + " cycles of 5 place() calls took " + sum / maxRounds
                           + "ms. on average of " + maxRounds + " rounds.");
    }

    /**
     * Tests price function.
     */
    @Test
    public void testPrice() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(3, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        matrix.populateUnderlay(1L, 1000L);
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateUnderlay(40L, 1000L);
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(110L, "1.1.1.10");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        matrix.place(110L, 10L);
        matrix.place(101L, 1L);
        matrix.place(201L, 20L);
        matrix.place(202L, 30L);
        matrix.place(203L, 202L);
        matrix.setCapacities(40L, new double[]{11., 12., 13., 14.}, new double[]{1., 2., 3., 0.});
        Assert.assertFalse(matrix.getEndpointFlows(110L).length > 0);
        // We have this one.
        double[] flows = matrix.getProjectedFlows(101L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(30., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        // Calculate price
        // 11. + 24. + 56. = 91
        // (30 / 13) / 3 = 0.7692307692 -> 1 - = 0.2307692308 -> 30 / 13 * 3 / square -> 14.4444444.
        Assert.assertEquals(14.4444444, matrix.calculatePrice(101L, 40L), 0.001);
        // The infinity
        matrix.setCapacities(40L, new double[]{.1, .2, .3, .4}, new double[]{1., 2., 3., 0.});
        Assert.assertFalse(matrix.getEndpointFlows(110L).length > 0);
        // We have this one.
        flows = matrix.getProjectedFlows(101L, 40L);
        Assert.assertTrue(flows.length > 0);
        Assert.assertEquals(0., flows[WeightClass.BLADE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.SWITCH.ordinal()], 0.0001);
        Assert.assertEquals(30., flows[WeightClass.SITE.ordinal()], 0.0001);
        Assert.assertEquals(0., flows[WeightClass.CROSS_SITE.ordinal()], 0.0001);
        // Calculate price to get positive infinity.
        Assert.assertEquals(Double.POSITIVE_INFINITY, matrix.calculatePrice(101L, 40L), 0.001);
    }

    /**
     * Tests existing capacities.
     */
    @Test
    public void testNonExistingCapacities() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(3, -1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        matrix.populateUnderlay(1L, 1000L);
        matrix.populateUnderlay(10L, 1000L);
        matrix.populateUnderlay(20L, 1000L);
        matrix.populateUnderlay(30L, 1000L);
        matrix.populateUnderlay(40L, 1000L);
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(110L, "1.1.1.10");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        matrix.place(110L, 10L);
        matrix.place(101L, 1L);
        matrix.place(201L, 20L);
        matrix.place(202L, 30L);
        matrix.place(203L, 202L);

        Optional<double[]> o = matrix.underlayNetwork_.getCapacities(
            new VolatileLong(10000L));
        Assert.assertFalse(o.isPresent());
        Assert.assertEquals(0, matrix.getEndpointFlows(null).length);
        Assert.assertEquals(0d, matrix.calculatePrice(null, 40L), 0.0001);
        Assert.assertEquals(0d, matrix.calculatePrice(101L, null), 0.0001);
        Assert.assertEquals(0d, matrix.calculatePrice(101L, 40L), 0.0001);
        Assert.assertEquals(0d, matrix.calculatePrice(111101L, 40L), 0.0001);
    }

    /**
     * Tests wrong capacities.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongCapacities() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.setCapacities(101L, new double[]{1., 2., 3.}, new double[]{1., 1., 1., 1.});
    }

    /**
     * Tests wrong utilization thresholds.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongUtilThresholds() {
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.setCapacities(101L, new double[]{1., 2., 3., 4}, new double[]{1., 1., 1.});
    }

    /**
     * Tests single-homes endpoint.
     */
    @Test
    public void testSingleHomed() {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(3, 1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(202L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        Set<MatrixVPod> vPods = getUniqueVPods(matrix);
        Assert.assertEquals(3, vPods.size());
    }

    /**
     * Tests multi-homed endpoints.
     */
    @Test
    public void testMultiHomed() {
        // We test the modification above.
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(3, 1, 8080, 1, 1, 10,
                                     Protocol.TCP));
        matrix.setEndpointOID(101L, "1.1.1.1");
        matrix.setEndpointOID(201L, "2.0.0.1");
        matrix.setEndpointOID(201L, "2.0.0.2");
        matrix.setEndpointOID(203L, "2.0.0.3");
        Set<MatrixVPod> vPods = getUniqueVPods(matrix);
        Assert.assertEquals(2, vPods.size());
        // Check VPoDs.
        for (MatrixVPod vPod : vPods) {
            if (vPod.getEdges().size() > 1) {
                for (OverlayEdge edge : vPod.getEdges()) {
                    boolean matches = "2.0.0.1".equals(edge.getSink().getNormalizedIpAddress()) ||
                                      "2.0.0.2".equals(edge.getSink().getNormalizedIpAddress());
                    Assert.assertTrue(matches);
                }
            }
        }
        // Check Oids.
        Assert.assertEquals(2, matrix.overlayNetwork_.state_.oidLookup_.get(new VolatileLong(201L))
                                                                       .size());
        // Update the underlay to hit the multi-ip per oid scenario.
        matrix.place(201L, 1L);
        matrix.overlayNetwork_.state_.oidLookup_.get(new VolatileLong(201L)).forEach(
            v -> Assert.assertEquals(1L, v.underlayOid_.getValue()));
    }

    /**
     * Tests volatile long.
     */
    @Test
    public void testVolatileLong() {
        VolatileLong vl1 = new VolatileLong(10L);
        VolatileLong vl2 = new VolatileLong(11L);
        VolatileLong vl3 = new VolatileLong(10L);
        Assert.assertEquals(-1, vl1.compareTo(vl2));
        Assert.assertEquals(1, vl2.compareTo(vl1));
        //noinspection EqualsWithItself
        Assert.assertEquals(0, vl1.compareTo(vl1));
        Assert.assertEquals(0, vl1.compareTo(vl3));
        Assert.assertEquals("10", vl1.toString());
    }

    /**
     * Tests vertex set.
     */
    @Test
    public void testVertexSet() {
        OverlayNetworkGraph.VertexSet vs1 = new OverlayNetworkGraph.VertexSet();
        OverlayNetworkGraph.VertexSet vs2 = new OverlayNetworkGraph.VertexSet();
        Assert.assertEquals(vs1, vs2);
        vs1.add(new OverlayVertex("1.1.1.1", 10));
        vs2.add(new OverlayVertex("1.1.1.1", 10));
        Assert.assertEquals(vs1, vs2);
        vs1.add(new OverlayVertex("1.1.1.1", 20));
        vs1.add(new OverlayVertex("1.1.1.2", 30));
        Assert.assertNotEquals(vs1, vs2);
        Assert.assertNotEquals(vs1, "1.1.1.1");
        OverlayVertex ov = new OverlayVertex("1.1.1.1", 10);
        Assert.assertEquals(Sets.newHashSet(ov).hashCode(), vs2.hashCode());
    }

    /**
     * Tests underlay struct.
     */
    @Test
    public void testStruct() {
        UnderlayNetworkGraph.Struct struct = new UnderlayNetworkGraph.Struct(1L,
                                                                             Sets.newHashSet(10L));
        Assert.assertEquals(Objects.hash(1L, Sets.newHashSet(10L)), struct.hashCode());
    }
}
