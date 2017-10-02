package com.vmturbo.topology.processor.targets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.AccountValue.PropertyValueList;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.impl.TargetInfoProtobufWrapper;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.AccountValueAdaptor;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * The Topology Processor's representation of a registered target.
 *
 * <p>Targets get added to the topology processor by users. A target
 * is the base unit of operation for a probe. For example, if the
 * probe is a VCenter probe, the target will be a VCenter instance.
 */
public class Target {
    private final long id;

    private final long probeId;

    /**
     * Account values given by the user when adding
     * this target.
     */
    private final List<AccountValue> mediationAccountVals;

    private final InternalTargetInfo info;

    private final TargetInfo noSecretDto;

    private final TargetInfo noSecretAnonymousDto;

    /**
     * Create a target from a string as returned by {@link Target#toJsonString()}.
     *
     * @param serializedTarget The string representing the serialized target.
     * @throws TargetDeserializationException If failed to de-serialize the target.
     */
    public Target(@Nonnull final String serializedTarget) throws TargetDeserializationException {
        this.info = InternalTargetInfo.fromJsonString(serializedTarget);
        this.id = info.targetInfo.getId();
        this.probeId = info.targetInfo.getSpec().getProbeId();

        noSecretDto = removeSecretAccountVals(info.targetInfo, info.secretFields);
        noSecretAnonymousDto = removeSecretAnonymousAccountVals(info.targetInfo,
                                                                info.secretFields);

        final ImmutableList.Builder<AccountValue> accountValBuilder = new ImmutableList.Builder<>();
        info.targetInfo.getSpec().getAccountValueList().stream()
                .map(this::targetAccountValToMediationAccountVal)
                .forEach(accountValBuilder::add);
        mediationAccountVals = accountValBuilder.build();
    }

    /**
     * The target specification.
     * @return the target specification, including target id, probe id, account values.
     */
    public TargetSpec getSpec() {
        return info.targetInfo.getSpec();
    }

    Target(final @Nonnull IdentityProvider identityProvider,
           final @Nonnull ProbeStore probeStore,
           final @Nonnull TargetSpec inputSpec) throws InvalidTargetException {
        this(Objects.requireNonNull(identityProvider, "Identity provider should not be null")
                        .getTargetId(inputSpec), probeStore, inputSpec, true);
    }

    @VisibleForTesting
    public Target(long targetId,
           final @Nonnull ProbeStore probeStore,
           final @Nonnull TargetSpec inputSpec,
           boolean validateAccountValues) throws InvalidTargetException {
        Objects.requireNonNull(probeStore);
        Objects.requireNonNull(inputSpec);

        // Validate the spec first, before doing more
        // extra work.
        if (validateAccountValues) {
            AccountValueVerifier.validate(inputSpec, probeStore);
        }

        this.id = targetId;
        this.probeId = inputSpec.getProbeId();

        final ImmutableList.Builder<AccountValue> accountValBuilder = new ImmutableList.Builder<>();
        inputSpec.getAccountValueList().stream()
                .map(this::targetAccountValToMediationAccountVal)
                .forEach(accountValBuilder::add);

        mediationAccountVals = accountValBuilder.build();

        final TargetInfo targetInfo = TargetInfo.newBuilder()
                .setId(id)
                .setSpec(TargetSpec.newBuilder()
                    .setProbeId(probeId)
                    .addAllAccountValue(inputSpec.getAccountValueList()))
                .build();

        final ImmutableSet.Builder<String> secretFieldBuilder = new ImmutableSet.Builder<>();

        if (validateAccountValues) {
            final ProbeInfo probeInfo = probeStore.getProbe(probeId).get();
            probeInfo.getAccountDefinitionList()
                            .stream()
                            .map(AccountValueAdaptor::wrap)
                            .filter(ad -> ad.isSecret())
                            .map(ad -> ad.getName())
                            .forEach(secretFieldBuilder::add);
        }
        final Set<String> secretFields = secretFieldBuilder.build();

        noSecretDto = removeSecretAccountVals(targetInfo, secretFields);
        noSecretAnonymousDto = removeSecretAnonymousAccountVals(targetInfo,
                                                                secretFields);

        info = new InternalTargetInfo(targetInfo, secretFields);
    }

    /**
     * Create a new {@link Target} with its fields updated according to the collection
     * of updated fields. Fields not present are retained from this target.
     *
     * @param updatedFields The fields on the target to update.
     * @param probeStore The store containing the collection of known probes.
     * @return A new target with fields updated from the collection.
     * @throws InvalidTargetException When the updated target is invalid.
     */
    public Target withUpdatedFields(@Nonnull final Collection<TopologyProcessorDTO.AccountValue> updatedFields,
                                    @Nonnull final ProbeStore probeStore)
        throws InvalidTargetException {
        TargetInfo targetInfo = info.targetInfo;

        final TargetSpec newSpec = TargetSpec.newBuilder().setProbeId(probeId)
            .addAllAccountValue(
                mergeUpdatedAccountValues(targetInfo.getSpec().getAccountValueList(), updatedFields))
            .build();

        return new Target(getId(), probeStore, newSpec, true);
    }

    public com.vmturbo.topology.processor.api.TargetInfo createTargetInfo() {
        return new TargetInfoProtobufWrapper(noSecretDto);
    }

    /**
     * Build a merged collection of account values. Account values from the updated collection
     * overwrite values from the original collection. When no updatedAccountValue is present,
     * the originalAccountValue is retained.
     *
     * @param originalAccountValues The collection of original account values.
     * @param updatedAccountValues The collection of updated account values.
     * @return The merged collection of values.
     */
    private Collection<TopologyProcessorDTO.AccountValue> mergeUpdatedAccountValues(
        @Nonnull Collection<TopologyProcessorDTO.AccountValue> originalAccountValues,
        @Nonnull Collection<TopologyProcessorDTO.AccountValue> updatedAccountValues) {

        Map<String, TopologyProcessorDTO.AccountValue> accountValueMap =
            Stream.concat(originalAccountValues.stream(), updatedAccountValues.stream())
                .collect(Collectors.toMap(
                    TopologyProcessorDTO.AccountValue::getKey,
                    Function.identity(),
                    // Specify a merge function that overwrites the existing value with the updated value.
                    (existingAccountValue, updatedAccountValue) -> updatedAccountValue));

        return accountValueMap.values();
    }

    private AccountValue targetAccountValToMediationAccountVal(TopologyProcessorDTO.AccountValue targetVal) {
        return AccountValue.newBuilder()
                .setKey(targetVal.getKey())
                .setStringValue(targetVal.getStringValue())
                .addAllGroupScopePropertyValues(
                        targetVal.getGroupScopePropertyValuesList().stream()
                            .map(targetValList -> PropertyValueList.newBuilder()
                                        .addAllValue(targetValList.getValueList())
                                        .build())
                            .collect(Collectors.toList()))
                .build();
    }

    private TargetInfo removeSecretAccountVals(@Nonnull final TargetInfo info,
                                               @Nonnull final Set<String> secretVals) {
        return TargetInfo.newBuilder()
                .setId(info.getId())
                .setSpec(TargetSpec.newBuilder()
                    .setProbeId(info.getSpec().getProbeId())
                    .addAllAccountValue(
                        info.getSpec().getAccountValueList().stream()
                            .filter(val -> !secretVals.contains(val.getKey()))
                            .collect(Collectors.toList())))
                .build();
    }

    /**
     * Removes secret fields and anonymizes the ones that need it.
     *
     * @param info The target info.
     * @param secretVals The secret field keys.
     * @return The sanitized target info.
     */
    private TargetInfo removeSecretAnonymousAccountVals(@Nonnull final TargetInfo info,
                                                        @Nonnull final Set<String> secretVals) {
        TopologyProcessorDTO.AccountValue username = TopologyProcessorDTO.AccountValue
                .newBuilder().setKey("username").setStringValue("target_user").build();
        TopologyProcessorDTO.AccountValue address = TopologyProcessorDTO.AccountValue
                .newBuilder().setKey("address").setStringValue("target_address").build();
        List<TopologyProcessorDTO.AccountValue> accountValues = new ArrayList<>();
        for (TopologyProcessorDTO.AccountValue val : info.getSpec().getAccountValueList()) {
            if ("username".equals(val.getKey())) {
                accountValues.add(username);
            } else if ("address".equals(val.getKey())) {
                accountValues.add(address);
            } else if (!secretVals.contains(val.getKey())) {
                accountValues.add(val);
            }
        }
        return TargetInfo.newBuilder()
                         .setId(info.getId())
                         .setSpec(TargetSpec.newBuilder().setProbeId(info.getSpec().getProbeId())
                                            .addAllAccountValue(accountValues))
                         .build();
    }

    /**
     * Retrieve the OID of the target.
     *
     * @return The OID of the target.
     */
    public long getId() {
        return id;
    }

    /**
     * Retrieve the OID of the probe the target is attached to.
     *
     * @return The OID of the probe.
     */
    public long getProbeId() {
        return probeId;
    }

    /**
     * Retrieve the account values used to connect
     * to the target. These account values are necessary
     * for all operations on the target (discovery,
     * action execution, validation).
     *
     * @return The list of {@link AccountValue} objects.
     */
    public List<AccountValue> getMediationAccountVals() {
        return mediationAccountVals;
    }

    @Nonnull
    public TargetInfo getNoSecretDto() {
        return noSecretDto;
    }

    @Nonnull
    public TargetInfo getNoSecretAnonymousDto() {
        return noSecretAnonymousDto;
    }

    /**
     * Serializes the target to a JSON string that's compatible with {@link Target#Target(String)}.
     *
     * @return The JSON string representing the target.
     */
    public String toJsonString() {
        return info.toJsonString();
    }

    /**
     * Internal decorated {@link TargetInfo} that keeps track
     * of {@link AccountValue}s that are supposed to be secret.
     */
    @Immutable
    private static class InternalTargetInfo {
        private static final Gson GSON = new GsonBuilder()
            // Need to be able to serialize AccountValue protos.
            .registerTypeAdapter(InternalTargetInfo.class, new InternalTargetInfoAdapter())
            .create();

        final TargetInfo targetInfo;

        final Set<String> secretFields;

        InternalTargetInfo(@Nonnull final TargetInfo targetInfo,
                           @Nonnull final Set<String> secretFields) {
            this.targetInfo = targetInfo;
            this.secretFields = secretFields;
        }

        /**
         * Returns the encrypted version of the {@see TargetInfo}.
         *
         * @return The encrypted version of the {@see TargetInfo}.
         */
        @Nonnull TargetInfo encrypt() {
            List<TopologyProcessorDTO.AccountValue> values = new ArrayList<>();
            targetInfo.getSpec().getAccountValueList().stream().forEach(av -> {
                if (secretFields.contains(av.getKey())) {
                    values.add(TopologyProcessorDTO.AccountValue.newBuilder()
                                                                .addAllGroupScopePropertyValues(
                                                                        av.getGroupScopePropertyValuesList())
                                                                .setKey(av.getKey()).setStringValue(
                                    CryptoFacility.encrypt(av.getStringValue())).build());
                } else {
                    values.add(av);
                }
            });
            return TargetInfo.newBuilder().setId(targetInfo.getId())
                             .setSpec(TargetSpec.newBuilder()
                                                .setProbeId(targetInfo.getSpec().getProbeId())
                                                .addAllAccountValue(values)).build();
        }

        /**
         * Returns the decrypted version of the {@see TargetInfo}.
         *
         * @return The decrypted version of the {@see TargetInfo}.
         */
        @Nonnull TargetInfo decrypt(final @Nonnull Set<String> secretFields) {
            List<TopologyProcessorDTO.AccountValue> values = new ArrayList<>();
            targetInfo.getSpec().getAccountValueList().stream().forEach(av -> {
                if (secretFields.contains(av.getKey())) {
                    values.add(TopologyProcessorDTO.AccountValue.newBuilder()
                                                                .addAllGroupScopePropertyValues(
                                                                        av.getGroupScopePropertyValuesList())
                                                                .setKey(av.getKey()).setStringValue(
                                    CryptoFacility.decrypt(av.getStringValue())).build());
                } else {
                    values.add(av);
                }
            });
            return TargetInfo.newBuilder().setId(targetInfo.getId())
                             .setSpec(TargetSpec.newBuilder()
                                                .setProbeId(targetInfo.getSpec().getProbeId())
                                                .addAllAccountValue(values)).build();
        }

        @Nonnull
        String toJsonString() {
            return GSON.toJson(this);
        }

        @Nonnull
        static InternalTargetInfo fromJsonString(@Nonnull final String serializedString) throws TargetDeserializationException {
            try {
                return GSON.fromJson(serializedString, InternalTargetInfo.class);
            } catch (Exception e) {
                throw new TargetDeserializationException(e);
            }
        }
    }

    /**
     * GSON adapter to serialize {@link InternalTargetInfo}.
     */
    private static class InternalTargetInfoAdapter extends TypeAdapter<InternalTargetInfo> {
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
            final TargetInfo info = builder.build();
            final Set<String> sf = secretFieldBuilder.build();
            final InternalTargetInfo itf = new InternalTargetInfo(builder.build(), secretFieldBuilder.build());

            return new InternalTargetInfo(itf.decrypt(sf), sf);
        }
    }
}
