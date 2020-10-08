package com.vmturbo.topology.processor.probeproperties;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * A probe property store using the {@link KeyValueStore} class to persist data.<p/>
 * The class uses {@link ProbeStore} and {@link TargetStore} objects to validate ids that are passed to it.
 * The convention is adopted that information related to probes lies under the namespace
 * {@link ProbeStore#PROBE_KV_STORE_PREFIX} and that information related to targets lies under the namespace
 * {@link TargetStore#TARGET_KV_STORE_PREFIX}.<p/>
 * Given this convention, {@link KVBackedProbePropertyStore} stores either probe-specific or target-specific
 * probe properties under a sub-namespace {@link KVBackedProbePropertyStore#PROBE_PROPERTY_PREFIX}.  For
 * example, suppose that we have a probe property with name {@code name} specific to probe with id
 * {@code id}.  The value of the property will be stored in the key/value store under key
 * {@code ProbeStore.PROBE_KV_STORE_PREFIX + "/" + id + TargetStore.PROBE_PROPERTY_PREFIX + "/" + name}.
 * <p/>
 * Probe property names are assumed to not have illegal characters.  In particular, they only contain
 * alphanumeric characters, dots, and underscores.
 */
public class KVBackedProbePropertyStore implements ProbePropertyStore {
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final KeyValueStore kvStore;

    private static final String PROBE_PROPERTY_PREFIX = "probeproperties/";
    private static final String PROBE_SPECIFIC_PREFIX = "probe.";
    private static final String TARGET_SPECIFIC_PREFIX = "target.";

    private final Logger logger = LogManager.getLogger();

    /**
     * This maps probe names to maps of probe properties that are hard-coded, i.e., they are
     * always set.
     *
     * The maps are formatted so that they are ready to be added to a "set-properties"
     * mediation message, i.e., their keys conform to the format "probe.probeName.propertyName".
     */
    private static final Map<String, Map<String, String>> HARD_CODED_PROBE_PROPERTIES =
        ImmutableMap.<String, Map<String, String>>builder()
            // Disable saving the cost.usage.report for AWS and Azure. The SDK probes won't be
            // able to write the report to disk (because it's a read-only volume), so enabling
            // these will break discovery.
            .put(SDKProbeType.AWS_BILLING.getProbeType(), ImmutableMap.of("probe.AWS Billing.save.cost.usage.report", "true",
                    "probe.AWS Billing.tmp.diags.dir", "/tmp/diags/aws/billing/"))
            .put(SDKProbeType.AZURE.getProbeType(), ImmutableMap.of("probe.Azure.save.cost.usage.report", "false"))
            .build();

    /**
     * Create a probe property store.  Access to a probe store and a target store is given, so that
     * it is possible to make sanity checks (existence of probes and targets, correct probe / target
     * relations).
     *
     * @param probeStore probe store.
     * @param targetStore target store.
     * @param kvStore key value store where probe properties will be persisted.
     */
    public KVBackedProbePropertyStore(
            @Nonnull ProbeStore probeStore,
            @Nonnull TargetStore targetStore,
            @Nonnull KeyValueStore kvStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.kvStore = Objects.requireNonNull(kvStore);
    }

    @Override
    @Nonnull
    public synchronized Stream<Entry<ProbePropertyKey, String>> getAllProbeProperties() {
        final Stream.Builder<Entry<ProbePropertyKey, String>> result = Stream.builder();

        // for any probe, fetch probe properties
        for (long probeId : probeStore.getProbes().keySet()) {
            final String prefix = probeSpecific(probeId);
            final Map<String, String> probePropertiesAsNameValuePairs = kvStore.getByPrefix(prefix);
            for (Entry<String, String> e : probePropertiesAsNameValuePairs.entrySet()) {
                result.accept(
                    new AbstractMap.SimpleImmutableEntry<>(
                        new ProbePropertyKey(probeId, e.getKey().substring(prefix.length())),
                        e.getValue()));
            }
        }

        // for any target, fetch probe properties
        for (Target target : targetStore.getAll()) {
            final String prefix = targetSpecific(target.getId());
            final Map<String, String> probePropertiesAsNameValuePairs = kvStore.getByPrefix(prefix);
            for (Entry<String, String> e : probePropertiesAsNameValuePairs.entrySet()) {
                result.accept(
                    new AbstractMap.SimpleImmutableEntry<>(
                        new ProbePropertyKey(
                            target.getProbeId(),
                            target.getId(),
                            e.getKey().substring(prefix.length())),
                        e.getValue()));
            }
        }

        return result.build();
    }

    @Override
    @Nonnull
    public synchronized Stream<Entry<String, String>> getProbeSpecificProbeProperties(long probeId)
            throws ProbeException {
        validateProbeId(probeId);
        final String prefix = probeSpecific(probeId);
        return removePrefixFromMapKeys(kvStore.getByPrefix(prefix), prefix.length());
    }

    @Override
    @Nonnull
    public synchronized Stream<Entry<String, String>> getTargetSpecificProbeProperties(
            long probeId,
            long targetId) throws ProbeException, TargetStoreException {
        validateProbeId(probeId);
        validateTargetId(probeId, targetId);
        final String prefix = targetSpecific(targetId);
        return removePrefixFromMapKeys(kvStore.getByPrefix(prefix), prefix.length());
    }

    @Override
    @Nonnull
    public synchronized Optional<String> getProbeProperty(@Nonnull ProbePropertyKey key)
            throws ProbeException, TargetStoreException {
        validateIdsInPropertyKey(Objects.requireNonNull(key));
        return kvStore.get(convertProbePropertyKeyToKVStoreKey(key));
    }

    @Override
    public synchronized void putAllProbeSpecificProperties(
            long probeId,
            @Nonnull Map<String, String> newProbeProperties) throws ProbeException {
        // remove old probe properties under this probe
        for (String name :
                getProbeSpecificProbeProperties(probeId).map(Entry::getKey).collect(Collectors.toList())) {
            kvStore.removeKeysWithPrefix(probeSpecific(probeId, name));
        }

        // put the new probe properties under this probe
        for (Entry<String, String> nameValue : Objects.requireNonNull(newProbeProperties).entrySet()) {
            kvStore.put(probeSpecific(probeId, nameValue.getKey()), nameValue.getValue());
        }
    }

    @Override
    @Nonnull
    public synchronized SetProperties buildSetPropertiesMessageForProbe(@Nonnull Collection<Long> probeIds)
            throws ProbeException, TargetStoreException {
        final SetProperties.Builder resultBuilder = SetProperties.newBuilder();

        // put probe-specific probe properties into the map
        for (long probeId : probeIds) {
            // find probe
            final String probeName =
                probeStore.getProbe(probeId)
                    .map(MediationMessage.ProbeInfo::getProbeType)
                    .orElseThrow(() -> new ProbeException("Unknown probe id " + probeId));

            // first add any hard-coded probe properties for the probe
            final Map<String, String>
                hardCodedPropertiesForProbe = HARD_CODED_PROBE_PROPERTIES.get(probeName);
            if (hardCodedPropertiesForProbe != null) {
                resultBuilder.putAllProperties(hardCodedPropertiesForProbe);
            }

            // add probe properties - note that they get priority over hard-coded ones
            getProbeSpecificProbeProperties(probeId)
                .forEach(keyValuePair ->
                    resultBuilder.putProperties(
                        transformToMediationPropertyKey(true, probeName, keyValuePair.getKey()),
                        keyValuePair.getValue()));
        }

        // get all the relevant targets
        final Collection<Target> targets =
            Objects.requireNonNull(probeIds).stream()
                .flatMap(id -> targetStore.getProbeTargets(id).stream())
                .collect(Collectors.toList());

        // put target-specific probe properties into the map
        for (Target target : targets) {
            final long targetId = target.getId();
            final String targetIdFieldValue = target.getSerializedIdentifyingFields();
            if (targetIdFieldValue == null) {
                // should never happen
                logger.warn("Target with id " + targetId + " has no values for its identifying fields");
                continue;
            }
            getTargetSpecificProbeProperties(target.getProbeId(), targetId)
                .forEach(keyValuePair ->
                    resultBuilder.putProperties(
                        transformToMediationPropertyKey(false, targetIdFieldValue, keyValuePair.getKey()),
                        keyValuePair.getValue()));
        }

        return resultBuilder.build();
    }

    @Override
    public synchronized void putAllTargetSpecificProperties(
            long probeId,
            long targetId,
            @Nonnull Map<String, String> newProbeProperties) throws ProbeException, TargetStoreException {
        // remove old probe properties under this target
        for (String name :
                getTargetSpecificProbeProperties(probeId, targetId)
                    .map(Entry::getKey)
                    .collect(Collectors.toList())) {
            kvStore.removeKeysWithPrefix(targetSpecific(targetId, name));
        }

        // put the new probe properties under this target
        for (Entry<String, String> nameValue : Objects.requireNonNull(newProbeProperties).entrySet()) {
            kvStore.put(targetSpecific(targetId, nameValue.getKey()), nameValue.getValue());
        }
    }

    @Override
    public synchronized void putProbeProperty(@Nonnull ProbePropertyKey key, @Nonnull String value)
            throws ProbeException, TargetStoreException {
        validateIdsInPropertyKey(Objects.requireNonNull(key));
        kvStore.put(convertProbePropertyKeyToKVStoreKey(key), Objects.requireNonNull(value));
    }

    @Override
    public synchronized void deleteProbeProperty(@Nonnull ProbePropertyKey key)
            throws ProbeException, TargetStoreException {
        validateIdsInPropertyKey(Objects.requireNonNull(key));
        final String kvStoreKey = convertProbePropertyKeyToKVStoreKey(key);
        if (!kvStore.containsKey(kvStoreKey)) {
            throw new ProbeException("Probe property " + key.toString() + " does not exist");
        }
        kvStore.removeKeysWithPrefix(kvStoreKey);
    }

    /**
     * Transform a probe property key to a key that conforms to the conventions of
     * communication between the topology processor and the mediation clients.  The mediation probe
     * property key is constructed as follows.  The first part is:
     * <ul>
     *     <li>{@code "probe."} if this is a probe-specific probe property</li>
     *     <li>{@code "target."} if this is a target-specific probe property</li>
     * </ul>
     * The second part is the name of the probe or the name of the target, followed by a dot.
     * The third part is the name of the probe property.
     *
     * @param isProbeSpecific whether this a probe specific probe property.
     * @param probeOrTargetName probe or target name.
     * @param probePropertyName probe property name.
     * @return mediation key.
     */
    @Nonnull
    private String transformToMediationPropertyKey(
            boolean isProbeSpecific,
            @Nonnull String probeOrTargetName,
            @Nonnull String probePropertyName) {
        return
            new StringBuilder()
                .append(isProbeSpecific ? PROBE_SPECIFIC_PREFIX : TARGET_SPECIFIC_PREFIX)
                .append(probeOrTargetName)
                .append(".")
                .append(probePropertyName)
                .toString();
    }

    private void validateProbeId(long probeId) throws ProbeException {
        probeStore.getProbe(probeId).orElseThrow(() -> new ProbeException("Unknown probe " + probeId + ")"));
    }

    private void validateTargetId(long probeId, long targetId) throws ProbeException, TargetStoreException {
        final Target target =
            targetStore.getTarget(targetId).orElseThrow(
                () -> new TargetNotFoundException(targetId));
        if (target.getProbeId() != probeId) {
            throw new TargetStoreException("Target " + targetId + " is not discovered by probe " + probeId);
        }
    }

    private void validateIdsInPropertyKey(@Nonnull ProbePropertyKey probePropertyKey)
            throws ProbeException, TargetStoreException {
        validateProbeId(probePropertyKey.getProbeId());
        if (probePropertyKey.getTargetId().isPresent()) {
            validateTargetId(probePropertyKey.getProbeId(), probePropertyKey.getTargetId().get());
        }
    }

    @Nonnull
    private String probeSpecific(long id) {
        return probeSpecific(id, "");
    }

    /**
     * Creates a key or a namespace for the key value store that corresponds to a probe-specific
     * probe property.
     *
     * @param id the probe id.
     * @param name the probe property name.  If empty, a namespace is created instead.
     * @return key or namespace corresponding to this probe-specific property.
     */
    @Nonnull
    private String probeSpecific(long id, @Nonnull String name) {
        final StringBuilder resultBuilder = new StringBuilder();
        appendPrefix(id, ProbeStore.PROBE_KV_STORE_PREFIX, resultBuilder);
        return resultBuilder.append(name).toString();
    }

    @Nonnull
    private String targetSpecific(long id) {
        return targetSpecific(id, "");
    }

    /**
     * Creates a key or a namespace for the key value store that corresponds to a target-specific
     * probe property.
     *
     * @param id the target id.
     * @param name the probe property name.  If empty, a namespace is created instead.
     * @return key or namespace corresponding to this target-specific property.
     */
    @Nonnull
    private String targetSpecific(long id, @Nonnull String name) {
        final StringBuilder resultBuilder = new StringBuilder();
        appendPrefix(id, TargetStore.TARGET_KV_STORE_PREFIX, resultBuilder);
        return resultBuilder.append(name).toString();
    }

    /**
     * Converts a {@link ProbePropertyKey} object to the corresponding key for the key/value store.
     *
     * @param probePropertyKey a probe property key.
     * @return the corresponding key for the key/value store.
     */
    @Nonnull
    private String convertProbePropertyKeyToKVStoreKey(@Nonnull ProbePropertyKey probePropertyKey) {
        if (probePropertyKey.isTargetSpecific()) {
            return targetSpecific(probePropertyKey.getTargetId().get(), probePropertyKey.getName());
        } else {
            return probeSpecific(probePropertyKey.getProbeId(), probePropertyKey.getName());
        }
    }

    private void appendPrefix(long id, @Nonnull String prefix, @Nonnull StringBuilder builder) {
        builder.append(prefix).append(id).append("/").append(PROBE_PROPERTY_PREFIX);
    }

    @Nonnull
    private static Stream<Entry<String, String>> removePrefixFromMapKeys(
            @Nonnull Map<String, String> mapWithPrefixes, int prefixLength) {
        return
            mapWithPrefixes
                .entrySet()
                .stream()
                .map(e ->
                    new AbstractMap.SimpleImmutableEntry<>(
                        e.getKey().substring(prefixLength),
                        e.getValue()));
    }
}
