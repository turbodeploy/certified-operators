package com.vmturbo.topology.processor.probeproperties;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Interface for storing probe properties.
 */
public interface ProbePropertyStore {
    /**
     * Get a map of all the probe properties in the system to their current values.  The map is returned
     * as a stream of map entries to facilitate optimizations.
     *
     * @return a map of all the probe properties in the system to their current values.
     */
    Stream<Entry<ProbePropertyKey, String>> getAllProbeProperties();

    /**
     * Get the names and values of probe properties specific to a probe.  The map is returned
     * as a stream of map entries to facilitate optimizations.
     *
     * @param probeId oid of the probe.
     * @return map from names to values of the probe properties specific to the probe.
     * @throws ProbeException probe does not exist.
     */
    Stream<Entry<String, String>> getProbeSpecificProbeProperties(long probeId) throws ProbeException;

    /**
     * Get the names and values of probe properties specific to a target.  The map is returned
     * as a stream of map entries to facilitate optimizations.
     *
     * @param probeId oid of the probe discovering the target.
     * @param targetId oid of the target.
     * @return map from names to values of the probe properties specific to the target.
     * @throws ProbeException probe does not exist.
     * @throws TargetStoreException target does not exist or is not related to the given probe.
     */
    Stream<Entry<String, String>> getTargetSpecificProbeProperties(long probeId, long targetId)
        throws ProbeException, TargetStoreException;

    /**
     * Get the value of a single probe property.
     *
     * @param key key that identifies the property.
     * @return the value of the property (empty if the property does not exist).
     * @throws ProbeException probe specified in the key does not exist.
     * @throws TargetStoreException target specified in the key does not exist or
     *                              is not related to the given probe.
     */
    Optional<String> getProbeProperty(ProbePropertyKey key)
        throws ProbeException, TargetStoreException;

    /**
     * Update all probe properties under a probe.  The whole table of probe properties will be replaced
     * with the new ones.
     *
     * @param probeId oid of the probe.
     * @param newProbeProperties names and values of the new probe properties.
     * @throws ProbeException probe does not exist.
     */
    void putAllProbeSpecificProperties(long probeId, Map<String, String> newProbeProperties)
        throws ProbeException;

    /**
     * Update all probe properties under a target.  The whole table of probe properties will be replaced
     * with the new ones.
     *
     * @param probeId oid of the probe discovering the target.
     * @param targetId oid of the target.
     * @param newProbeProperties names and values of the new probe properties.
     * @throws ProbeException probe does not exist.
     * @throws TargetStoreException target does not exist or is not related to the given probe.
     */
    void putAllTargetSpecificProperties(long probeId, long targetId, Map<String, String> newProbeProperties)
        throws ProbeException, TargetStoreException;

    /**
     * Update the value of a single probe property.  If the property does not exist, it will be created.
     *
     * @param key key that identifies the property.
     * @param value the new value of the property.
     * @throws ProbeException probe specified in the key does not exist.
     * @throws TargetStoreException target specified in the key does not exist or
     *                              is not related to the given probe.
     */
    void putProbeProperty(ProbePropertyKey key, String value) throws ProbeException, TargetStoreException;

    /**
     * Delete a single probe property.
     *
     * @param key key that identifies the property.
     * @throws ProbeException probe specified in the key does not exist.
     * @throws TargetStoreException target specified in the key does not exist or
     *                              is not related to the given probe.
     */
    void deleteProbeProperty(ProbePropertyKey key) throws ProbeException, TargetStoreException;
        // TODO: probably throw an exception when property does not exist

    /**
     * Identifies a specific probe property.
     */
    class ProbePropertyKey {
        /**
         * Probe id.
         */
        private final long probeId;

        /**
         * Target id: null for probe-specific probe properties.
         */
        private final Long targetId;

        /**
         * Property name
         */
        private final String name;

        /**
         * Creates a probe-specific probe property key.
         */
        public ProbePropertyKey(long probeId, @Nonnull String name) {
            this.probeId = probeId;
            this.targetId = null;
            this.name = Objects.requireNonNull(name);
        }

        /**
         * Creates a target-specific probe property key.
         */
        public ProbePropertyKey(long probeId, long targetId, @Nonnull String name) {
            this.probeId = probeId;
            this.targetId = targetId;
            this.name = Objects.requireNonNull(name);
        }

        public long getProbeId() {
            return probeId;
        }

        public Optional<Long> getTargetId() {
            return Optional.ofNullable(targetId);
        }

        @Nonnull
        public String getName() {
            return name;
        }

        public boolean isTargetSpecific() {
            return targetId != null;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ProbePropertyKey)) {
                return false;
            }
            final ProbePropertyKey other = (ProbePropertyKey)o;
            return
                probeId == other.probeId && Objects.equals(targetId, other.targetId) &&
                name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(probeId, targetId, name);
        }
    }
}
