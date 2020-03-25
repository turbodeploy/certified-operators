package com.vmturbo.topology.processor.targets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target.InternalTargetInfo;

/**
 * A {@link TargetDao} implementation that saves target data as JSON objects to a {@link KeyValueStore}.
 */
public class KvTargetDao implements TargetDao {

    private static final Gson GSON = new GsonBuilder()
        .registerTypeAdapter(InternalTargetInfo.class, new EncryptingTargetInfoSerializer())
        .create();

    private static final Logger logger = LogManager.getLogger();

    private final KeyValueStore keyValueStore;

    private final ProbeStore probeStore;

    /**
     * Create a new {@link KvTargetDao}.
     *
     * @param keyValueStore The {@link KeyValueStore} to use for persistence.
     * @param probeStore The {@link ProbeStore} containing probe information.
     */
    public KvTargetDao(@Nonnull final KeyValueStore keyValueStore,
                @Nonnull final ProbeStore probeStore) {
        this.keyValueStore = keyValueStore;
        this.probeStore = probeStore;
    }

    @Override
    public List<Target> getAll() {
        final Map<String, String> persistedTargets = this.keyValueStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX);

        return persistedTargets.entrySet().stream()
            .map(entry -> {
                try {
                    final InternalTargetInfo internalInfo = GSON.fromJson(entry.getValue(), InternalTargetInfo.class);
                    final Target newTarget = new Target(internalInfo, probeStore);
                    addAccountDefEntryList(newTarget);
                    logger.debug("Retrieved existing target '{}' ({}) for probe {}.", newTarget.getDisplayName(),
                        newTarget.getId(), newTarget.getProbeId());
                    return newTarget;
                } catch (JsonSyntaxException | TargetDeserializationException e) {
                    // It may make sense to delete the offending key here,
                    // but choosing not to do that for now to keep
                    // the constructor read-only w.r.t. the keyValueStore.
                    logger.warn("Failed to deserialize target: {}.", entry.getKey());
                    return null;
                } catch (RuntimeException | TargetStoreException e) {
                    logger.error("Failed to deserialize target: " + entry.getKey(), e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    @Override
    public void store(final Target target) {
        keyValueStore.put(TargetStore.TARGET_KV_STORE_PREFIX + Long.toString(target.getId()), toJsonString(target));
    }

    @Override
    public void remove(final long targetId) {
        keyValueStore.removeKeysWithPrefix(TargetStore.TARGET_KV_STORE_PREFIX + Long.toString(targetId));
    }

    /**
     * When deserializing a target, we need to add the {@link AccountDefEntry} from the related
     * probe as this is need by the target to determine if group scope is needed when returning
     * the list of {@link com.vmturbo.platform.common.dto.Discovery.AccountValue}s.
     *
     * @param newTarget the {@link Target} that we just deserialized.
     * @throws TargetStoreException if the {@link ProbeInfo} is not in the probe store.
     */
    private void addAccountDefEntryList(final Target newTarget) throws TargetStoreException {
        ProbeInfo probeInfo = probeStore.getProbe(newTarget.getProbeId())
            .orElseThrow(() ->
                new TargetStoreException("Probe information not found for target with id "
                    + newTarget.getProbeId()));
        newTarget.setAccountDefEntryList(probeInfo.getAccountDefinitionList());
    }

    @VisibleForTesting
    String toJsonString(@Nonnull final Target target) {
        return GSON.toJson(target.getInternalTargetInfo());
    }

    /**
     * GSON adapter to serialize/deserialize {@link InternalTargetInfo}, encrypting and
     * decrypting secret fields as necessary.
     */
    private static class EncryptingTargetInfoSerializer extends TypeAdapter<InternalTargetInfo> {

        @Override
        public void write(JsonWriter out, InternalTargetInfo value) throws IOException {
            out.beginObject();
            out.name("secretFields");
            out.beginArray();
            for (final String field : value.secretFields) {
                out.value(CryptoFacility.encrypt(field));
            }
            out.endArray();
            out.endObject();

            out.beginObject();
            out.name("targetInfo");
            out.value(JsonFormat.printer().print(value.encrypt()));
            out.endObject();
        }

        @Override
        public InternalTargetInfo read(JsonReader in) throws IOException {
            final ImmutableSet.Builder<String> secretFieldBuilder = new ImmutableSet.Builder<>();
            in.beginObject();
            in.nextName();
            in.beginArray();
            while (in.hasNext()) {
                secretFieldBuilder.add(CryptoFacility.decrypt(in.nextString()));
            }
            in.endArray();
            in.endObject();

            in.beginObject();
            in.nextName();
            final String serializedTarget = in.nextString();
            final TargetInfo.Builder builder = TargetInfo.newBuilder();
            JsonFormat.parser().merge(serializedTarget, builder);
            in.endObject();
            final Set<String> sf = secretFieldBuilder.build();
            final InternalTargetInfo itf = new InternalTargetInfo(builder.build(), secretFieldBuilder.build());

            return new InternalTargetInfo(itf.decrypt(sf), sf);
        }
    }
}
