package com.vmturbo.topology.processor.probeproperties;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.group.api.SettingMessages.SettingNotification;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
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
    @Nonnull
    Stream<Entry<ProbePropertyKey, String>> getAllProbeProperties();

    /**
     * Get the names and values of probe properties specific to a probe.  The map is returned
     * as a stream of map entries to facilitate optimizations.
     *
     * @param probeId oid of the probe.
     * @return map from names to values of the probe properties specific to the probe.
     * @throws ProbeException probe does not exist.
     */
    @Nonnull
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
    @Nonnull
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
    @Nonnull
    Optional<String> getProbeProperty(@Nonnull ProbePropertyKey key)
        throws ProbeException, TargetStoreException;

    /**
     * Update all probe properties under a probe.  The whole table of probe properties will be replaced
     * with the new ones.
     *
     * @param probeId oid of the probe.
     * @param newProbeProperties names and values of the new probe properties.
     * @throws ProbeException probe does not exist.
     */
    void putAllProbeSpecificProperties(long probeId, @Nonnull Map<String, String> newProbeProperties)
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
    void putAllTargetSpecificProperties(
            long probeId,
            long targetId,
            @Nonnull Map<String, String> newProbeProperties)
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
    void putProbeProperty(@Nonnull ProbePropertyKey key, @Nonnull String value)
        throws ProbeException, TargetStoreException;

    /**
     * Delete a single probe property.
     *
     * @param key key that identifies the property.
     * @throws ProbeException probe specified in the key does not exist.
     * @throws TargetStoreException target specified in the key does not exist or
     *                              is not related to the given probe.
     */
    void deleteProbeProperty(@Nonnull ProbePropertyKey key) throws ProbeException, TargetStoreException;

    /**
     * Builds a {@link SetProperties} message that contains all probe properties that relate to
     * a set of probes.  These include the probe-specific probe properties, as well as the target-specific
     * properties for all targets discovered by any of the probes.  This message is the one sent to a
     * mediation client responsible for that set of probes:
     * <ul>
     *     <li>when the client is registered</li>
     *     <li>when there is a probe property modification that affects the specific probe</li>
     * </ul>
     * <p/>
     * Note that currently each mediation client corresponds to exactly one probe.  This means that
     * the set of probes passed to the method is necessarily a singleton.  However, the practice of
     * one probe per mediation client is likely to change in the future, which is why the method takes
     * a set of probes as a parameter.
     *
     * @param probeIds the ids of all the probes. When empty, the method returns an empty message.
     * @return a {@link SetProperties} message ready to be sent to the mediation client.
     * @throws ProbeException one of the probes does not exist.
     * @throws TargetStoreException unexpected data found in the target store.
     */
    @Nonnull
    SetProperties buildSetPropertiesMessageForProbe(@Nonnull Collection<Long> probeIds)
        throws ProbeException, TargetStoreException;

    /**
     * Special case of {@link this#deleteProbeProperty(ProbePropertyKey)} for a single probe id.
     *
     * @param probeId the id of the probe.
     * @return a {@link SetProperties} message ready to be sent to the mediation client.
     * @throws ProbeException the probe does not exist.
     * @throws TargetStoreException unexpected data found in the target store.
     */
    @Nonnull
    default SetProperties buildSetPropertiesMessageForProbe(long probeId)
            throws ProbeException, TargetStoreException {
        return buildSetPropertiesMessageForProbe(Collections.singleton(probeId));
    }

    /**
     * React to the global policies change.
     *
     * @param settingChange change in the global policies values
     * @return true if settings change requires probe properties update
     */
    boolean updatePropertiesOnSettingsChange(@Nonnull SettingNotification settingChange);

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
