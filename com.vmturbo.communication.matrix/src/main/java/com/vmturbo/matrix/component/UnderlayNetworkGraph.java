package com.vmturbo.matrix.component;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.ncm.MatrixDTO;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.matrix.component.external.WeightClass;

/**
 * The {@link UnderlayNetworkGraph} implements the lowest level of the network layers - Physical
 * Hosts/Switches/Routers.
 */
@NotThreadSafe class UnderlayNetworkGraph {
    /**
     * The empty set.
     */
    private static final Set<Long> EMPTY_DPOD = Collections.emptySet();

    /**
     * The entities.
     */
    @VisibleForTesting Map<VolatileLong, Struct> entities_ = new HashMap<>();

    /**
     * The static value for keys.
     * Must be used one at a time.
     */
    private static final VolatileLong KEY_USE = new VolatileLong(-1L);

    /**
     * Populates the underlying network.
     *
     * @param consumerOID The consumer OID.
     * @param providerOID The provider OID.
     */
    void populateUnderlying(final long consumerOID, final long providerOID) {
        entities_.put(new VolatileLong(consumerOID), new Struct(providerOID, EMPTY_DPOD));
    }

    /**
     * Resets underlay to be reused by the post-processor.
     */
    void reset() {
        entities_ = new HashMap<>();
    }

    /**
     * Sets the capacities for an provider.
     *
     * @param oid            The OID.
     * @param capacities     The capacities.
     * @param utilThresholds The utilization thresholds.
     */
    void setCapacities(final @Nonnull VolatileLong oid, final @Nonnull double[] capacities,
                       final double[] utilThresholds) {
        if (capacities.length != utilThresholds.length ||
            capacities.length != OverlayVertex.WEIGHT_CLASSES_SIZE) {
            throw new IllegalArgumentException("Capacities and utilization thresholds lengths" +
                                               " must match.");
        }
        final double[] array = new double[capacities.length];
        for (int i = 0; i < capacities.length; i++) {
            array[i] = capacities[i] * utilThresholds[i];
            // Guard against unexpected.
            if (array[i] <= 0) {
                array[i] = 1D;
            }
        }
        final Struct struct = entities_.get(oid);
        if (struct != null) {
            struct.capacities_ = array;
        }
    }

    /**
     * Returns capacities for a provider.
     *
     * @param oid The provider OID.
     * @return The capacities and utilization thresholds, or an empty array if provider is not
     * found.
     */
    @Nonnull Optional<double[]> getCapacities(final @Nonnull VolatileLong oid) {
        final Struct struct = entities_.get(oid);
        if (struct != null && struct.capacities_ != null) {
            return Optional.of(struct.capacities_);
        }
        return Optional.empty();
    }

    /**
     * Populates the DPoD for the underlying network.
     *
     * @param oids The OIDs of Physical Hosts/Switches/Routers belonging to a single DPoD.
     */
    void populateDpod(final @Nonnull Set<Long> oids) {
        oids.stream().map(v -> entities_.get(KEY_USE.setValue(v)))
            .filter(Objects::nonNull).forEach(v -> v.dpod_ = oids);
    }

    /**
     * Computes the distance between two entities represented by their OIDs.
     *
     * @param src The source entity.
     * @param dst The destination entity.
     * @return The distance, or {@link Optional#empty()} in case either source or destination are
     * unknown.
     */
    @Nullable WeightClass computeDistance(final @Nullable VolatileLong src,
                                          final @Nullable VolatileLong dst) {
        // Check the edge case, where by we might not have the OID for a vertex simply because
        // we don't control the VM/Raw hardware/Container which has this IP.
        // In this case we assume the SITE distance.
        if (src == null || dst == null) {
            return WeightClass.SITE;
        }
        // Check the easiest first.
        if (src.getValue() == dst.getValue()) {
            return WeightClass.BLADE;
        }
        // Do the heavier calculations.
        // Calculate and cache the weight class.
        // Check whether we have both sides.
        final Struct pairSrc = entities_.get(src);
        if (pairSrc == null) {
            return null;
        }
        // Start with switch, skip another lookup if we can.
        if (pairSrc.dpod_.contains(dst.value)) {
            return WeightClass.SWITCH;
        }
        // Deal with the rest.
        final Struct pairDst = entities_.get(dst);
        if (pairDst == null) {
            return null;
        } else if (pairSrc.dc_ != pairDst.dc_) {
            return WeightClass.CROSS_SITE;
        } else {
            return WeightClass.SITE;
        }
    }

    /**
     * Obtains deep copy of the Matrix.
     *
     * @return The deep copy of this matrix.
     */
    @Nonnull UnderlayNetworkGraph copy() {
        final UnderlayNetworkGraph graph = new UnderlayNetworkGraph();
        graph.entities_ = new HashMap<>();
        for (Map.Entry<VolatileLong, Struct> entry : entities_.entrySet()) {
            final Struct pair = entry.getValue();
            Struct newStruct;
            if (pair.dpod_.isEmpty()) {
                newStruct = new Struct(pair.dc_, EMPTY_DPOD);
            } else {
                newStruct = new Struct(pair.dc_, new HashSet<>(pair.dpod_));
            }
            if (pair.capacities_ != null) {
                newStruct.capacities_ = Arrays.copyOf(pair.capacities_, pair.capacities_.length);
            }
            graph.entities_.put(entry.getKey(), newStruct);
        }
        return graph;
    }

    /**
     * Exports the underlay network to the DTO.
     *
     * @param exporter The exporter.
     */
    void exportGraph(final @Nonnull MatrixInterface.Codec exporter) {
        entities_.forEach((k, v) -> {
            MatrixDTO.UnderlayStruct.Builder struct = MatrixDTO.UnderlayStruct.newBuilder();
            struct.setOid(k.value);
            struct.setDc(v.dc_);
            if (v.dpod_ != null) {
                struct.addAllDpod(v.dpod_);
            }
            if (v.capacities_ != null) {
                for (double c : v.capacities_) {
                    struct.addCapacities(c);
                }
            }
            exporter.next(struct.build());
        });
    }

    /**
     * Imports the underlay graph.
     *
     * @param map The underlay network.
     */
    void importGraph(final @Nonnull Map<Long, MatrixDTO.UnderlayStruct> map) {
        map.forEach((k, v) -> {
            Struct struct = new Struct(v.getDc(), new HashSet<>(v.getDpodList()));
            if (v.getCapacitiesCount() > 0) {
                struct.capacities_ = new double[v.getCapacitiesCount()];
                for (int i = 0; i < struct.capacities_.length; i++) {
                    struct.capacities_[i] = v.getCapacities(i);
                }
            }
            entities_.put(new VolatileLong(k), struct);
        });
    }

    /**
     * The internal structure.
     */
    @VisibleForTesting
    static class Struct {
        final long dc_;

        Set<Long> dpod_;

        double[] capacities_;

        Struct(final long dc, final @Nonnull Set<Long> dpod) {
            dc_ = dc;
            dpod_ = dpod;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            return o instanceof Struct &&
                   dc_ == ((Struct)o).dc_ && dpod_.equals(((Struct)o).dpod_);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Objects.hash(dc_, dpod_);
        }
    }
}
