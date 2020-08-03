package com.vmturbo.matrix.component;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.AbstractMessage;

import com.vmturbo.common.protobuf.topology.ncm.MatrixDTO;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.matrix.component.external.WeightClass;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * The {@link CommunicationMatrix} implements a communication matrix component.
 * The main part is the collection of all the sub-graphs. Each such sub-graph represents
 * a fully connected set of endpoints connected by an overlay network.
 * Every newly created edge will be created with the {@link WeightClass#SITE} weight class.
 */
@ThreadSafe
public class CommunicationMatrix implements MatrixInterface {
    /**
     * The VM overlay network graph.
     */
    @VisibleForTesting OverlayNetworkGraph overlayNetwork_;

    /**
     * The VM overlay network graph.
     */
    @VisibleForTesting UnderlayNetworkGraph underlayNetwork_;

    /**
     * The Consumer to Underlay provider OID map.
     */
    @VisibleForTesting Map<VolatileLong, VolatileLong> consumerToProvider_;

    /**
     * The associated OID mapper.
     */
    private ServiceEntityOIDMapper mapper_;

    /**
     * The JSON builder.
     */
    private static final Gson GSON_ = new GsonBuilder()
                                          .enableComplexMapKeySerialization()
                                          .setLongSerializationPolicy(
                                              LongSerializationPolicy.STRING)
                                          .setPrettyPrinting()
                                          .create();

    /**
     * The static value for keys.
     * Must be used one at a time.
     */
    private static final VolatileLong KEY_USE = new VolatileLong(-1L);

    /**
     * The serialization character set.
     */
    private static final Charset CHARSET_ = StandardCharsets.UTF_8;

    /**
     * The dump/restore file name suffixes.
     */
    private enum DumpSuffixes {
        OVERLAY_MAP,
        OVERLAY,
        UNDERLAY,
        CPMAP,
        MAPPER;

        /**
         * Returns the suffix.
         *
         * @return The suffix.
         */
        @Nonnull String suffix() {
            return name().toLowerCase() + ".json";
        }
    }

    /**
     * The hoover.
     */
    @VisibleForTesting final Hoover hoover_ = new Hoover();

    /**
     * Constructs the communication matrix.
     */
    CommunicationMatrix() {
        this(new OverlayNetworkGraph(), new UnderlayNetworkGraph(),
             new HashMap<>(), new ServiceEntityOIDMapper());
    }

    /**
     * Constructs the communication matrix.
     *
     * @param overlayNetwork     The overlay graph.
     * @param underlayNetwork    The underlay graph.
     * @param consumerToProvider The consumer-to-provider map.
     * @param mapper             The entity OID mapper.
     */
    private CommunicationMatrix(final @Nonnull OverlayNetworkGraph overlayNetwork,
                                final @Nonnull UnderlayNetworkGraph underlayNetwork,
                                final @Nonnull Map<VolatileLong, VolatileLong> consumerToProvider,
                                final @Nonnull ServiceEntityOIDMapper mapper) {
        overlayNetwork_ = overlayNetwork;
        underlayNetwork_ = underlayNetwork;
        consumerToProvider_ = consumerToProvider;
        mapper_ = mapper;
    }

    /**
     * Obtains deep copy of the Matrix.
     *
     * @return The deep copy of this matrix.
     */
    @Override
    public synchronized @Nonnull MatrixInterface copy() {
        final Map<VolatileLong, VolatileLong> map = new HashMap<>(consumerToProvider_);
        return new CommunicationMatrix(overlayNetwork_.copy(), underlayNetwork_.copy(), map,
                                       mapper_.copy());
    }

    /**
     * Serializes the matrix into the DTO.
     *
     * @param exporter The exporter.
     */
    public synchronized void exportMatrix(final @Nonnull Codec exporter) {
        // Overlay
        exporter.start(Component.OVERLAY);
        overlayNetwork_.exportGraph(exporter);
        exporter.finish();
        // Underlay
        exporter.start(Component.UNDERLAY);
        underlayNetwork_.exportGraph(exporter);
        exporter.finish();
        // Consumer to provider.
        exporter.start(Component.CONSUMER_2_PROVIDER);
        consumerToProvider_.forEach((k, v) -> {
            MatrixDTO.ConsumerToProvider.Builder node = MatrixDTO.ConsumerToProvider.newBuilder();
            node.setConsumer(k.getValue());
            node.setProvider(v.getValue());
            exporter.next(node.build());
        });
        exporter.finish();
    }

    /**
     * Imports the Matrix from the DTO.
     * Will return the importer, which then can be used to import the matrix.
     * The importer will be successful iff all components it expects are received.
     * It is up to the caller to deal with the timeouts or missed messages,
     *
     * @return The importer.
     */
    public @Nonnull Codec getMatrixImporter() {
        return new Importer();
    }

    /**
     * The {@link Importer} implements a Codec.
     * It will process one component at a time.
     */
    private class Importer implements Codec {
        /**
         * The components list.
         */
        private Set<Component> components = new HashSet<>();

        /**
         * The current component.
         */
        private Component currentComponent;

        /**
         * The in-process state.
         */
        private boolean inProcess;

        // The component-specific data structures.
        // We only have 3 for now, so create them here, don't get fancy.
        // If we get more or get extensible, provide an external interface.

        /**
         * Overlay.
         */
        private List<MatrixDTO.OverlayEdge> dtoEdges = new ArrayList<>();

        /**
         * Underlay.
         */
        private Map<Long, MatrixDTO.UnderlayStruct> underlayMap = new HashMap<>();

        /**
         * The consumer_to_provider map.
         */
        private Map<Long, Long> consumer2Provider = new HashMap<>();

        /**
         * Constructs the importer.
         */
        private Importer() {
            components.add(Component.OVERLAY);
            components.add(Component.UNDERLAY);
            components.add(Component.CONSUMER_2_PROVIDER);
            inProcess = false;
        }

        /**
         * Start the export.
         *
         * @param component The matrix component.
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        public void start(final @Nonnull Component component) throws IllegalStateException {
            if (inProcess) {
                throw new IllegalStateException(
                    "Out-of-order start message for a component " + component.name());
            }
            if (!components.contains(component)) {
                throw new IllegalStateException(
                    "Repeated start message for a component " + component.name());
            }
            // Remove it.
            components.remove(component);
            inProcess = true;
            currentComponent = component;
        }

        /**
         * The next chunk.
         *
         * @param <T> The message type.
         * @param msg The message.
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        public <T extends AbstractMessage> void next(final @Nonnull T msg)
            throws IllegalStateException {
            if (!inProcess) {
                throw new IllegalStateException(
                    "Out-of-order next message for a component");
            }
            // A bit ugly, but efficient.
            switch (currentComponent) {
                case OVERLAY:
                    dtoEdges.add((MatrixDTO.OverlayEdge)msg);
                    break;
                case UNDERLAY:
                    MatrixDTO.UnderlayStruct struct = (MatrixDTO.UnderlayStruct)msg;
                    underlayMap.put(struct.getOid(), struct);
                    break;
                case CONSUMER_2_PROVIDER:
                    MatrixDTO.ConsumerToProvider ctp = (MatrixDTO.ConsumerToProvider)msg;
                    consumer2Provider.put(ctp.getConsumer(), ctp.getProvider());
                    break;
            }
        }

        /**
         * Finish the export.
         *
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        public void finish() throws IllegalStateException {
            // We might have never actually started.
            if (!inProcess) {
                return;
            }
            // A bit ugly, but efficient.
            switch (currentComponent) {
                case OVERLAY:
                    overlayNetwork_.importGraph(dtoEdges);
                    break;
                case UNDERLAY:
                    underlayNetwork_.importGraph(underlayMap);
                    break;
                case CONSUMER_2_PROVIDER:
                    consumer2Provider.forEach((k, v) -> consumerToProvider_
                                                            .put(new VolatileLong(k),
                                                                 new VolatileLong(v)));
                    break;
            }
            inProcess = false;
        }
    }

    /**
     * Dumps the content of the Matrix to a file.
     *
     * @param fileName The file to dump the data to.
     * @throws IOException In case of an error persisting matrix.
     */
    @Override
    public synchronized void dump(final @Nonnull String fileName) throws IOException {
        // Overlay.
        try (FileOutputStream out = new FileOutputStream(
            fileName + DumpSuffixes.OVERLAY_MAP.suffix(),
            false);
             OutputStreamWriter writer = new OutputStreamWriter(out, CHARSET_);
             BufferedWriter bw = new BufferedWriter(writer, 32768)) {
            final Map<Integer, List<VertexStore>> vertices = new TreeMap<>();
            overlayNetwork_.state_.vertices_
                .keySet()
                .forEach(k -> {
                    VertexStore vertexStore = new VertexStore();
                    vertexStore.ip = k.getNormalizedIpAddress();
                    vertexStore.neighbors = k.getNeighbours().keySet().stream()
                                             .map(OverlayVertex::getNormalizedIpAddress)
                                             .collect(Collectors.toList());
                    vertices.computeIfAbsent(vertexStore.neighbors.size(),
                                             kk -> new ArrayList<>()).add(vertexStore);
                });
            CommunicationMatrix.GSON_.toJson(vertices, bw);
        }
        try (FileOutputStream out = new FileOutputStream(
            fileName + DumpSuffixes.OVERLAY.suffix(),
            false);
             OutputStreamWriter writer = new OutputStreamWriter(out, CHARSET_);
             BufferedWriter bw = new BufferedWriter(writer, 32768)) {
            CommunicationMatrix.GSON_.toJson(overlayNetwork_.state_.edges_, bw);
        }
        // Underlay.
        try (FileOutputStream out = new FileOutputStream(
            fileName + DumpSuffixes.UNDERLAY.suffix(),
            false);
             OutputStreamWriter writer = new OutputStreamWriter(out, CHARSET_);
             BufferedWriter bw = new BufferedWriter(writer, 32768)) {
            CommunicationMatrix.GSON_.toJson(underlayNetwork_, bw);
        }
        // Consumer to provider map.
        try (FileOutputStream out = new FileOutputStream(
            fileName + DumpSuffixes.CPMAP.suffix(),
            false);
             OutputStreamWriter writer = new OutputStreamWriter(out, CHARSET_);
             BufferedWriter bw = new BufferedWriter(writer, 32768)) {
            CommunicationMatrix.GSON_.toJson(consumerToProvider_, bw);
        }
        // UUID to OID map.
        try (FileOutputStream out = new FileOutputStream(
            fileName + DumpSuffixes.MAPPER.suffix(),
            false);
             OutputStreamWriter writer = new OutputStreamWriter(out, CHARSET_);
             BufferedWriter bw = new BufferedWriter(writer, 32768)) {
            CommunicationMatrix.GSON_.toJson(mapper_, bw);
        }
    }

    /**
     * Constructs vertex from the raw JSON object.
     *
     * @param map The raw JSON object.
     * @return The vertex.
     */
    @SuppressWarnings("unchecked")
    private @Nonnull OverlayVertex constructRestoredVertex(final @Nonnull Map<String, Object> map) {
        OverlayVertex v = new OverlayVertex(map.get("normalizedIpAddress_").toString(),
                                            (int)Double.parseDouble(map.get("port_").toString()));
        long o = Long.parseLong(((Map<String, String>)map.get("oid_")).get("value"));
        long u = Long.parseLong(((Map<String, String>)map.get("underlayOid_")).get("value"));
        v.setOid(new VolatileLong(o));
        v.setUnderlayOid(new VolatileLong(u));
        return v;
    }

    /**
     * Constructs an edge from the raw JSON object.
     *
     * @param map The raw JSON object.
     * @return The edge.
     */
    @SuppressWarnings("unchecked")
    private @Nonnull OverlayEdge constructRestoredEdge(final @Nonnull Map<?, ?> map) {
        long latency = Long.parseLong(map.get("latency_").toString());
        double flow = Double.parseDouble(map.get("flow_").toString());
        long tx = Long.parseLong(map.get("tx_").toString());
        long rx = Long.parseLong(map.get("rx_").toString());
        long accumulatedData = Long.parseLong(map.get("accumulatedData_").toString());
        OverlayVertex source = constructRestoredVertex((Map<String, Object>)map.get("source_"));
        OverlayVertex sink = constructRestoredVertex((Map<String, Object>)map.get("sink_"));
        OverlayEdge edge = new OverlayEdge(latency, flow, tx, rx, source, sink);
        edge.accumulateData(accumulatedData);
        return edge;
    }

    /**
     * Restores the content of the Matrix from a file.
     *
     * @param fileName The file to restore the data from.
     * @throws IOException In case of an error restoring matrix.
     */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized void restore(final @Nonnull String fileName) throws IOException {
        // Overlay.
        try (FileInputStream in = new FileInputStream(
            fileName + DumpSuffixes.OVERLAY.suffix());
             InputStreamReader reader = new InputStreamReader(in, CHARSET_)) {
            // By using map, we avoid errors related to the class hierarchy, namely the abstract
            // classes.
            final Set<OverlayEdge> edges = ((Set<Map<?, ?>>)GSON_.fromJson(reader, Set.class))
                                               .stream().map(this::constructRestoredEdge)
                                               .collect(Collectors.toSet());
            overlayNetwork_.restore(edges);
            edges.forEach(edge -> {
                overlayNetwork_.setEndpointOID(edge.getSource().getOid(),
                                               edge.getSource().getNormalizedIpAddress());
                overlayNetwork_.setEndpointOID(edge.getSink().getOid(),
                                               edge.getSink().getNormalizedIpAddress());
            });
        }
        // Underlay.
        try (FileInputStream in = new FileInputStream(
            fileName + DumpSuffixes.UNDERLAY.suffix());
             InputStreamReader reader = new InputStreamReader(in, CHARSET_)) {
            underlayNetwork_ = GSON_.fromJson(reader, UnderlayNetworkGraph.class);
        }
        // Consumer to provider map.
        try (FileInputStream in = new FileInputStream(fileName + DumpSuffixes.CPMAP.suffix());
             InputStreamReader reader = new InputStreamReader(in, CHARSET_)) {
            final Type type = new TypeToken<Map<VolatileLong, VolatileLong>>() {
            }.getType();
            consumerToProvider_ = GSON_.fromJson(reader, type);
        }
        // UUID to OID map.
        try (FileInputStream in = new FileInputStream(fileName + DumpSuffixes.MAPPER.suffix());
             InputStreamReader reader = new InputStreamReader(in, CHARSET_)) {
            mapper_ = GSON_.fromJson(reader, ServiceEntityOIDMapper.class);
        }
    }

    /**
     * Excludes the supplied IP addresses from the Matrix.
     * Please note that this is an expensive operation, so should be called for all known IP
     * addresses.
     * The consumer/provider and the underlay graph are unaffected.
     *
     * @param ips The set of IP addresses to be excluded.
     */
    @Override
    public synchronized void exclude(final @Nonnull Set<String> ips) {
        // Take care of the overlay network.
        overlayNetwork_ = overlayNetwork_.copyExcluding(ips);
    }

    /**
     * Returns the associated SEtoOID mapper.
     *
     * @return The associated SEtoOID mapper.
     */
    @Override
    public @Nonnull ServiceEntityOIDMapper getMapper() {
        return mapper_;
    }

    /**
     * Add all flows.
     * We first add all the new flows, then re-apply the old flows which we don't have now.
     * We identify the flow by pair of its vertices.
     * When we re-apply a previous flow, we fade its flow amount.
     *
     * @param flows The flow DTOs.
     */
    @Override
    public synchronized void update(@Nonnull Collection<CommonDTO.FlowDTO> flows) {
        hoover_.stop();
        overlayNetwork_.update(flows);
        hoover_.start();
    }

    /**
     * Resets underlay to be reused by the post-processor.
     */
    @Override
    public synchronized void resetUnderlay() {
        consumerToProvider_.clear();
        underlayNetwork_.reset();
    }

    /**
     * Populates the underlay network.
     * The OIDs reflect current topology, so that they can be updated as many times as needed,
     * for as long as the corresponding {@link #place(long, long)} call is also made before
     * anything is queried.
     *
     * @param consumerOID The consumer OID.
     * @param providerOID The provider OID.
     */
    @Override
    public synchronized void populateUnderlay(final long consumerOID, final long providerOID) {
        underlayNetwork_.populateUnderlying(consumerOID, providerOID);
    }

    /**
     * Populates the DPoD for the underlying network.
     *
     * @param oids The OIDs of Physical Hosts/Switches/Routers belonging to a single DPoD.
     */
    @Override
    public synchronized void populateDpod(final @Nonnull Set<Long> oids) {
        underlayNetwork_.populateDpod(oids);
    }

    /**
     * Returns the collection of all VPoD.
     * Each VPoD is represented by a collection of the IP addresses.
     *
     * @return The collection of VPoDs.
     */
    @Override
    public synchronized @Nonnull Collection<Collection<String>> getVpods() {
        return overlayNetwork_.getVpods();
    }

    /**
     * Links the Service Entity represented by an OID with the endpoint.
     * We update both current and update state in the overlay graph.
     * We can afford doing this any time we want to reflect the current state of Topology,
     * since we take a "current snapshot of Topology" at the time of the call.
     * The care must be taken to ensure {@link #place(long, long)} and
     * {@link #populateUnderlay(long, long)} calls are also made as needed.
     *
     * @param oid The consumer oid.
     * @param ip  The consumer IP address.
     */
    @Override
    public synchronized void setEndpointOID(final long oid, final @Nonnull String ip) {
        // Update the consumer overlay network.
        overlayNetwork_.setEndpointOID(new VolatileLong(oid), ip);
    }

    /**
     * Traverse to the bottom of the provider chain.
     * For Containers it would be:
     * Container -> VM -> PM.
     * For VMs it would be:
     * VM -> PM.
     * Reason: This method allows us to avoid autoboxing in some invocations.
     * Note! In case the value for the providerOID is not found, the providerOID is returned.
     *
     * @param providerOID The provider OID.
     * @return The underlay OID.
     */
    private @Nonnull VolatileLong getUnderlayProvider(final @Nonnull VolatileLong providerOID) {
        VolatileLong underlayOID = providerOID;
        VolatileLong prev = underlayOID;
        while (prev != null) {
            underlayOID = prev;
            prev = consumerToProvider_.get(underlayOID);
        }
        return underlayOID;
    }

    /**
     * Checks whether Matrix is empty.
     *
     * @return {@code true} iff the Matrix is empty.
     */
    public synchronized boolean isEmpty() {
        return overlayNetwork_.isEmpty();
    }

    /**
     * Place consumer on a provider.
     * Due to the fact that the underlay provider is resolved immediately, the order of calling
     * this method is important: PMs -> VMs -> Containers.
     * The meaning is that the provider has to be initially placed before its consumer.
     *
     * @param consumerOID The consumer OID.
     * @param providerOID The provider OID.
     */
    @Override
    public synchronized void place(final long consumerOID, final long providerOID) {
        // If the endpoint OID hasn't been set, skip the entire thing.
        // Save on autoboxing to be performed several times.
        if (!overlayNetwork_.isOidPresent(KEY_USE.setValue(consumerOID))) {
            return;
        }
        // The theory here (supported by the micro-benchmark), is that
        // HashMap.put() is x2 - x2.5 more expensive than HashMap.get().
        // In addition to that, hashCode recalculation for Long is expensive.
        // So, by caching the hash code, we save time as well. We can get as
        // little as x3.2 and as much as x4.9 performance boost.
        // And also, we assume that the consumer has already been placed into the
        // consumerToProvider_ on the first move. So, subsequent moves should be cheaper.
        final VolatileLong v = consumerToProvider_.get(KEY_USE.setValue(consumerOID));
        if (v != null) {
            v.setValue(providerOID);
        } else {
            // Resolve the provider completely.
            VolatileLong vlUnderlay = getUnderlayProvider(KEY_USE.setValue(providerOID));
            consumerToProvider_.put(new VolatileLong(consumerOID), vlUnderlay.copyOf());
        }
        VolatileLong pOid = consumerToProvider_.get(KEY_USE.setValue(providerOID));
        VolatileLong provider = (pOid == null) ? new VolatileLong(providerOID) : pOid;
        overlayNetwork_.updateVertexUnderlay(KEY_USE.setValue(consumerOID), provider);
    }

    /**
     * Returns the projected flows per weight class for a given endpoint to be located in a new
     * location.
     * @param oid      The OID of the entity.
     * @param location The projected location.
     * @return result. Empty if no flows for an endpoint.
     */
    @Override
    public synchronized @Nonnull double[] getProjectedFlows(final @Nonnull Long oid,
                                                            final @Nullable Long location) {
        if (oid == null || location == null) {
            return OverlayNetworkGraph.EMPTY_FLOWS;
        }
        return overlayNetwork_.getEndpointFlows(
            new VolatileLong(oid),
            (location == null) ? null : new VolatileLong(location),
            underlayNetwork_);
    }

    /**
     * Returns the flows per weight class for a give endpoint.
     *
     * @param oid The OID of the entity.
     * @return result. Empty if no flows for an endpoint.
     */
    @Override
    public synchronized @Nonnull double[] getEndpointFlows(final @Nonnull Long oid) {
        if (oid == null) {
            return OverlayNetworkGraph.EMPTY_FLOWS;
        }
        VolatileLong providerOID = consumerToProvider_.get(KEY_USE.setValue(oid));
        if (providerOID == null) {
            return OverlayNetworkGraph.EMPTY_FLOWS;
        }
        // We need the underlay OID, so that we can compute the distance properly.
        return getProjectedFlows(oid, providerOID.value);
    }

    /**
     * Calculates sum of prices of all flows for a given consumer
     * based on its neighbors' distances from given provider.
     *
     * @param oid      Consumer oid
     * @param location provider oid where consumer is trying to be placed
     * @return The sum of prices of all flows of given consumer on provider.
     */
    @Override
    public synchronized double calculatePrice(final @Nonnull Long oid,
                                              final @Nonnull Long location) {
        // Sanity check.
        if (oid == null || location == null) {
            return 0d;
        }
        double priceSum = 0D;
        // The behavior of the getUnderlayProvider() is such that in case value is not found,
        // the key is returned.
        VolatileLong vlKey = consumerToProvider_.getOrDefault(KEY_USE.setValue(location),
                                                              new VolatileLong(location));
        final double[] flows = overlayNetwork_.getEndpointFlows(KEY_USE.setValue(oid), vlKey,
                                                                underlayNetwork_);
        if (flows.length == 0) {
            // If flows are not found in interface,
            // return 0 price to not affect normal market operation.
            return 0d;
        }
        final Optional<double[]> capacitiesInfo = underlayNetwork_.getCapacities(vlKey);
        if (!capacitiesInfo.isPresent()) {
            // If capacity of seller not found in interface,
            // return 0 price to not affect normal market operation.
            return 0d;
        }
        final double[] capacities = capacitiesInfo.get();
        for (int i = flows.length - 1; i >= 0; i--) {
            final double value = flows[i] / capacities[i];
            if (value >= 1D) {
                priceSum = Double.POSITIVE_INFINITY;
                break;
            }
            final double minusValue = 1D - value;
            priceSum += (value / (minusValue * minusValue));
        }
        return priceSum;
    }

    /**
     * Sets the capacities for a provider.
     *
     * @param oid            The provider OID.
     * @param capacities     The capacities.
     * @param utilThresholds The utilization thresholds.
     */
    @Override
    public synchronized void setCapacities(final @Nonnull Long oid,
                                           final @Nonnull double[] capacities,
                                           final double[] utilThresholds) {
        if (oid == null) {
            return;
        }
        underlayNetwork_.setCapacities(new VolatileLong(oid), capacities, utilThresholds);
    }

    /**
     * The cleaner class.
     */
    @VisibleForTesting class Hoover {
        /**
         * The running thread.
         */
        @VisibleForTesting
        Thread thread_;

        /**
         * Start the cleanup.
         */
        synchronized void start() {
            // The cleanup cycle. By default it is 10 minutes, so it equals to the update cycle.
            // The reason for a separate setting is ease of testing.
            final long updateCycle = Long.getLong("vmt.matrix.cleanupcycle",
                                                  TimeUnit.MINUTES.toMillis(10));
            // The cleanup thread is already on.
            if (thread_ != null) {
                return;
            }
            // Start the cleanup thread.
            thread_ = new Thread(() -> {
                // First, sleep double time, so that we allow probes to do their job.
                // and update the matrix.
                try {
                    Thread.sleep(updateCycle * 2);
                } catch (InterruptedException e) {
                    // We are interrupted, return.
                    return;
                }
                // Until we empty the overlay graph, update with no flows periodically.
                while (!overlayNetwork_.isEmpty()) {
                    synchronized (CommunicationMatrix.this) {
                        overlayNetwork_.update(Collections.emptyList());
                    }
                    // Sleep until the next time.
                    try {
                        Thread.sleep(updateCycle);
                    } catch (InterruptedException e) {
                        // We are interrupted, return.
                        return;
                    }
                }
                // If the overlay is empty, clear everything.
                synchronized (CommunicationMatrix.this) {
                    if (overlayNetwork_.isEmpty()) {
                        overlayNetwork_.reset();
                        underlayNetwork_.reset();
                    }
                }
            }, "The matrix cleanup thread");
            thread_.start();
        }

        /**
         * Stop the cleanup.
         */
        synchronized void stop() {
            if (thread_ != null) {
                thread_.interrupt();
                thread_ = null;
            }
        }
    }

    /**
     * The vertex store for storing vertices in JSON format.
     * We need it because of the circular references between vertices, resulting from V1 -> V2,
     * meaning V2 -> V1.
     */
    private static class VertexStore {
        // The ip is stored into JSON, so not directly referenced.
        @SuppressWarnings("unused")
        private String ip;
        private List<String> neighbors;
    }
}
