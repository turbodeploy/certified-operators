package com.vmturbo.topology.processor.targets;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.crosstier.common.TargetUtil;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.AccountValue.PropertyValueList;
import com.vmturbo.platform.common.dto.Discovery.TargetLinkInfoDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo.Builder;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.AccountValueAdaptor;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;

/**
 * The Topology Processor's representation of a registered target.
 *
 * <p>Targets get added to the topology processor by users. A target
 * is the base unit of operation for a probe. For example, if the
 * probe is a VCenter probe, the target will be a VCenter instance.
 */
@Immutable
public class Target implements ProbeStoreListener {
    private static Logger logger = Logger.getLogger(Target.class);
    public final static String EMPTY_CHANNEL = "";
    private final long id;
    private final long probeId;

    /**
     * Account values given by the user when adding this target.
     */
    private final List<AccountValue> mediationAccountVals;

    private InternalTargetInfo info;

    private TargetInfo noSecretDto;

    private TargetInfo noSecretAnonymousDto;

    private ProbeInfo probeInfo;

    private List<com.vmturbo.platform.common.dto.Discovery.AccountDefEntry> accountDefEntryList;

    private final String serializedIdentifyingFields;

    /**
     * Flag to indicate whether this target has a group scope defined.
     */
    private boolean hasGroupScope = false;

    private final Clock clock;

    /**
     * Create a target from an existing {@link InternalTargetInfo}.
     *
     * @param internalTargetInfo The {@link InternalTargetInfo}.
     * @param probeStore The probe store instance.
     * @param clock clock to use to track time.
     * @throws InvalidTargetException if probe not found in probe store
     */
    public Target(@Nonnull final InternalTargetInfo internalTargetInfo,
            @Nonnull final ProbeStore probeStore, @Nonnull final Clock clock) throws InvalidTargetException {
        this.id = internalTargetInfo.targetInfo.getId();
        this.probeId = internalTargetInfo.targetInfo.getSpec().getProbeId();
        this.clock = clock;

        ProbeInfo probeInfo = probeStore.getProbe(probeId).orElseThrow(() ->
            new InvalidTargetException("No probe found in store for probe ID " + probeId));


        final ImmutableList.Builder<AccountValue> accountValBuilder = new ImmutableList.Builder<>();
        internalTargetInfo.targetInfo.getSpec().getAccountValueList().stream()
            .map(this::targetAccountValToMediationAccountVal)
            .forEach(accountValBuilder::add);
        mediationAccountVals = accountValBuilder.build();

        refreshProbeInfo(probeInfo, true, internalTargetInfo.targetInfo.getSpec());
        this.serializedIdentifyingFields = generateSerializedIdentifiers(internalTargetInfo,
                probeInfo);
    }

    /**
     * The target specification.
     * @return the target specification, including target id, probe id, account values.
     */
    public TargetSpec getSpec() {
        return info.targetInfo.getSpec();
    }

    /**
     * Create a target instance with the given information of parameters.
     *
     * @param targetId The target id assigned from identity store.
     * @param probeStore The probe store instance.
     * @param inputSpec The target spec which contain information of the target.
     * @param validateAccountValues Boolean to know if we need to validate the account value.
     * @param updateLastEditTime Boolean to know if we need to update last edit time for the target or not.
     * @param clock clock to use to track time.
     * @throws InvalidTargetException If creating target failed.
     */
    public Target(long targetId, final @Nonnull ProbeStore probeStore,
            final @Nonnull TargetSpec inputSpec, boolean validateAccountValues,
            final boolean updateLastEditTime, @Nonnull final Clock clock)
            throws InvalidTargetException {
        Objects.requireNonNull(probeStore);
        Objects.requireNonNull(inputSpec);
        Objects.requireNonNull(clock);

        this.clock = clock;

        // Validate the spec first, before doing more extra work.
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

        final TargetSpec.Builder targetSpec = TargetSpec.newBuilder().setProbeId(probeId)
            .addAllAccountValue(inputSpec.getAccountValueList())
            .addAllDerivedTargetIds(inputSpec.getDerivedTargetIdsList())
            .putAllParentLinks(inputSpec.getParentLinksMap());

        if (updateLastEditTime) {
            targetSpec.setLastEditTime(LocalDateTime.now(clock)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli());
        } else if (inputSpec.hasLastEditTime()) {
            targetSpec.setLastEditTime(inputSpec.getLastEditTime());
        }

        if (inputSpec.hasLastEditingUser()) {
            targetSpec.setLastEditingUser(inputSpec.getLastEditingUser());
        }

        targetSpec.setIsHidden(inputSpec.getIsHidden());
        targetSpec.setReadOnly(inputSpec.getReadOnly());
        if (inputSpec.hasCommunicationBindingChannel()) {
            targetSpec.setCommunicationBindingChannel(inputSpec.getCommunicationBindingChannel());
        }

        final ProbeInfo probeInfo = probeStore.getProbe(probeId).orElseThrow(() ->
            new InvalidTargetException("No probe found in store for probe ID " + probeId));

        refreshProbeInfo(probeInfo, validateAccountValues, targetSpec.build());
        this.serializedIdentifyingFields = generateSerializedIdentifiers(info, probeInfo);
    }

    /**
     * Create a {@link TargetSpec} builder from a given {@link TargetInfo},  Collection of
     * {@link AccountValue}s and Collection of derived target Ids.
     *
     * @param targetInfo A {@link TargetInfo}.
     * @param values Collection of {@link AccountValue}s.
     * @param optionalChannel optional communication binding channel for the target
     * @return A {@link TargetSpec} builder.
     */
    private static TargetSpec.Builder createTargetSpecBuilder(
                                        @Nonnull TargetInfo targetInfo,
                                        @Nonnull Collection<TopologyProcessorDTO.AccountValue> values,
                                        @Nonnull Optional<String> optionalChannel) {
        final TargetSpec.Builder targetSpec = TargetSpec.newBuilder()
            .setProbeId(targetInfo.getSpec().getProbeId())
            .addAllAccountValue(values)
            .setIsHidden(targetInfo.getSpec().getIsHidden())
            .setReadOnly(targetInfo.getSpec().getReadOnly())
            .addAllDerivedTargetIds(targetInfo.getSpec().getDerivedTargetIdsList())
            .putAllParentLinks(targetInfo.getSpec().getParentLinksMap());

        if (targetInfo.getSpec().hasLastEditingUser()) {
            targetSpec.setLastEditingUser(targetInfo.getSpec().getLastEditingUser());
        }
        if (targetInfo.getSpec().hasLastEditTime()) {
            targetSpec.setLastEditTime(targetInfo.getSpec().getLastEditTime());
        }

        return updateBindingChannel(optionalChannel, targetInfo, targetSpec);
    }

    /**
     * Update the channel of the target with the provided one. If the provided one is an empty
     * string, unset the value. If no value is provided, use the existing value
     *
     * @param optionalChannel new binding channel
     * @param existingInfo existing information for the target
     * @param targetSpec new {@link TargetSpec} builder
     * @return the builder for the {@link TargetSpec}
     */
    private static TargetSpec.Builder updateBindingChannel(@Nonnull Optional<String> optionalChannel,
                                      @Nonnull TargetInfo existingInfo,
                                      @Nonnull TargetSpec.Builder targetSpec) {
        if (optionalChannel.isPresent()) {
            String channel = optionalChannel.get();
            if (!channel.equals(EMPTY_CHANNEL)) {
                targetSpec.setCommunicationBindingChannel(channel);
            }
        } else if (existingInfo.getSpec().hasCommunicationBindingChannel()) {
            targetSpec.setCommunicationBindingChannel(existingInfo.getSpec().getCommunicationBindingChannel());
        }
        return  targetSpec;
    }

    /**
     * Check if there is a group scope defined in the probe's list of account definition entries.
     *
     * @param accountDefs {@link List} of
     * {@link com.vmturbo.platform.common.dto.Discovery.AccountDefEntry} for the probe that will
     *                                discover this target.
     * @return true if there is a group scope defined among the account definitions; otherwise,
     * false.
     */
    private boolean checkForGroupScope(List<Discovery.AccountDefEntry> accountDefs) {
        return accountDefs.stream()
                .filter(Discovery.AccountDefEntry::hasCustomDefinition)
                .map(Discovery.AccountDefEntry::getCustomDefinition)
                .anyMatch(customAcctDefEntry -> customAcctDefEntry.hasGroupScope() ||
                    customAcctDefEntry.hasEntityScope());
    }

    /**
     * Create a new {@link Target} with its fields updated according to the collection
     * of updated fields. Fields not present are retained from this target.
     *
     * @param updatedFields The fields on the target to update.
     * @param probeStore The store containing the collection of known probes.
     * @param communicationBindingChannel the channel over which the target will communicate.
     * @return A new target with fields updated from the collection.
     * @throws InvalidTargetException When the updated target is invalid.
     */
    public Target withUpdatedFields(@Nonnull final Collection<TopologyProcessorDTO.AccountValue> updatedFields,
            @Nonnull final ProbeStore probeStore, Optional<String> communicationBindingChannel,
            @Nullable final String lastEditingUser)
        throws InvalidTargetException {
        TargetInfo targetInfo = info.targetInfo;
        ProbeInfo probeInfo = probeStore.getProbe(targetInfo.getSpec().getProbeId())
            .orElseThrow(() -> new InvalidTargetException("ProbeInfo not found for probe with ID "
                + targetInfo.getSpec().getProbeId() + " for target ID " + targetInfo.getId()));

        // Create a set of account definition keys that represent numeric or boolean fields.  We
        // treat empty or null string values for these fields as signifying that the account value
        // should be removed.  If we didn't remove it, we would have trouble later trying to parse
        // them into numeric or boolean values when the probe received them.
        Set<String> numericAndBooleanFieldKeys =
            AccountValueVerifier.getNumericAndBooleanFieldKeys(probeInfo.getAccountDefinitionList());

        // Filter out any account values that are boolean or numeric type and have empty values.
        // These represent fields the user has removed from the list of account values.
        Collection<TopologyProcessorDTO.AccountValue> filteredMergedAccountVals =
            mergeUpdatedAccountValues(targetInfo.getSpec().getAccountValueList(), updatedFields)
                .stream()
                .filter(acctVal -> StringUtils.isNotEmpty(acctVal.getStringValue())
                    || !numericAndBooleanFieldKeys.contains(acctVal.getKey()))
                .collect(Collectors.toList());

        final TargetSpec.Builder newSpec = createTargetSpecBuilder(targetInfo,
                filteredMergedAccountVals, communicationBindingChannel);
        if (lastEditingUser != null) {
            newSpec.setLastEditingUser(lastEditingUser);
        }
        // if we update derive target then we don't need to populate/update last edit time
        final boolean updateLastEditTime = newSpec.hasLastEditTime();
        return new Target(getId(), probeStore, newSpec.build(), true, updateLastEditTime,
                clock);
    }

    /**
     * Create a new {@link Target} with its updated derived Target's IDs.
     *
     * @param derivedTargetsIds List of derived target's IDs to be set.
     * @param probeStore The store containing the collection of known probes.
     * @return A new target with its updated derived Target's IDs.
     * @throws InvalidTargetException When the updated target is invalid.
     */
    public Target withUpdatedDerivedTargetIds(@Nonnull final List<Long> derivedTargetsIds,
                                              @Nonnull final ProbeStore probeStore)
        throws InvalidTargetException {

        final TargetSpec newSpec = info.targetInfo.getSpec().toBuilder()
                .clearDerivedTargetIds()
                .addAllDerivedTargetIds(derivedTargetsIds)
                .build();
        return new Target(getId(), probeStore, newSpec, true, false, clock);
    }

    public Target withUpdatedParentLinks(@Nonnull final Map<Long, TargetLinkInfoDTO> parentLinkMap,
                                         @Nonnull final ProbeStore probeStore) throws InvalidTargetException {

        final TargetSpec newSpec = info.targetInfo.getSpec().toBuilder()
                .clearParentLinks()
                .putAllParentLinks(parentLinkMap)
                .build();

        return new Target(getId(), probeStore, newSpec, true, false, clock);
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
        final TargetSpec.Builder targetSpec = createTargetSpecBuilder(
                info, info.getSpec().getAccountValueList()
                        .stream()
                        .filter(val -> !secretVals.contains(val.getKey()))
                        .collect(Collectors.toList()),
                Optional.empty());
        TargetInfo.Builder targetInfoBuilder = TargetInfo.newBuilder().setId(info.getId())
                .setSpec(targetSpec);
        if (info.hasDisplayName()) {
            targetInfoBuilder.setDisplayName(info.getDisplayName());
        }
        return targetInfoBuilder.build();
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

        final TargetSpec.Builder targetSpec =
            createTargetSpecBuilder(info, accountValues, Optional.empty());
        TargetInfo.Builder targetInfoBuilder = TargetInfo.newBuilder().setId(info.getId())
                .setSpec(targetSpec);
        if (info.hasDisplayName()) {
            targetInfoBuilder.setDisplayName(info.getDisplayName());
        }
        return targetInfoBuilder.build();
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
     * Return whether the Target is hidden.
     *
     * @return whether the Target is hidden.
     */
    public boolean isHidden() {
        return noSecretDto.getSpec().getIsHidden();
    }

    /**
     * Compute a display name for a target, for a given probe.
     *
     * @param probeInfo the probe info of the targets probe type
     * @param accountValues a list of the target account values
     * @param secretFields a list of secret fields which we don't want to show in the display name
     * @return display name
     */
    @Nonnull
    private static String computeDisplayName(ProbeInfo probeInfo,
                                     List<TopologyProcessorDTO.AccountValue> accountValues,
                                     Set<String> secretFields) {

        // If there are fields with "isTargetDisplayName" attribute - use them as display name
        List<String> displayNameFields = probeInfo.getAccountDefinitionList().stream()
                .filter(Discovery.AccountDefEntry::getIsTargetDisplayName)
                .map(AccountValueAdaptor::wrap)
                .map(AccountDefEntry::getName)
                .collect(Collectors.toList());

        // No field is marked as isTargetDisplayName - use identifierFields
        if (displayNameFields.isEmpty()) {
            displayNameFields = probeInfo.getTargetIdentifierFieldList();
        }

        if (displayNameFields.size() == 1) {
            for (TopologyProcessorDTO.AccountValue accountValue : accountValues) {
                if (accountValue.getKey().equals(displayNameFields.get(0))) {
                    return accountValue.getStringValue();
                }
            }
        } else {
            final Map<String, String> accountValuesMap = accountValues.stream()
                    .filter(TopologyProcessorDTO.AccountValue::hasStringValue)
                    .collect(Collectors.toMap(TopologyProcessorDTO.AccountValue::getKey,
                            TopologyProcessorDTO.AccountValue::getStringValue));
            try {
                // Concatenate all relevant fields
                return TargetUtil.getTargetId(probeInfo.getAccountDefinitionList(),
                        accountValuesMap, displayNameFields, TargetNameException::new);
            } catch (TargetNameException e) {
                logger.warn(String.format("Failed to compute target display name from identifierFields." +
                        "probe type %s", probeInfo.getProbeType()), e);
            }
        }

        // If TargetUtil.getTargetId failed, concatenate all non-secret fields
        return accountValues.stream()
                .filter(av -> av.hasStringValue() && !secretFields.contains(av.getKey()))
                .map(TopologyProcessorDTO.AccountValue::getStringValue)
                .collect(Collectors.joining("-"));
    }

    /**
     * Compute a display name for a target, for a given probe.
     * If the probe info was found, call computeDisplayName with the probe info and secret fields.
     * Else, use the first account value as display name.
     *
     * @param targetSpec the target specifications
     * @param probeStore the probe store from which to retrieve the probe info
     * @return the target display name
     */
    @Nonnull
    public static String computeDisplayName(TargetSpec targetSpec,
                                            final @Nonnull ProbeStore probeStore) {
        return probeStore.getProbe(targetSpec.getProbeId()).map(probeInfo -> {
            final ImmutableSet.Builder<String> secretFieldBuilder = new ImmutableSet.Builder<>();
            probeInfo.getAccountDefinitionList()
                    .stream()
                    .map(AccountValueAdaptor::wrap)
                    .filter(AccountDefEntry::isSecret)
                    .map(AccountDefEntry::getName)
                    .forEach(secretFieldBuilder::add);
            return computeDisplayName(probeInfo, targetSpec.getAccountValueList(), secretFieldBuilder.build());
        }).orElse(targetSpec.getAccountValueList().get(0).getStringValue());
    }

    @Nonnull
    public String getDisplayName() {
        if (info.targetInfo.hasDisplayName()) {
            return info.targetInfo.getDisplayName();
        } else {
            // should not happen.
            return String.valueOf(this.id);
        }
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
     * Retrieve the probe info the target is attached to.
     * @return The probe info.
     */
    @Nonnull
    public ProbeInfo getProbeInfo() {
        return probeInfo;
    }

    /**
     * Retrieve the account values used to connect to the target. These account values are necessary
     * for all operations on the target (discovery, action execution, validation).  If there is a
     * group scope defined, return the list of account values with the group scope parameters
     * populated.  We wait until this method is called to populate the group scope, since the
     * group contents may be dynamic and change from one discovery to the next.
     *
     * @return The list of {@link AccountValue} objects.
     */
    public List<AccountValue> getMediationAccountVals(GroupScopeResolver groupScopeResolver) {
        if (!hasGroupScope) {
            return mediationAccountVals;
        }
        final SDKProbeType probeType = SDKProbeType.create(this.probeInfo.getProbeType());
        return groupScopeResolver.processGroupScope(probeType, mediationAccountVals,
                    accountDefEntryList);
    }

    @Nonnull
    public TargetInfo getNoSecretDto() {
        return noSecretDto;
    }

    @Nonnull
    public TargetInfo getNoSecretAnonymousDto() {
        return noSecretAnonymousDto;
    }

    @Nonnull
    public String getSerializedIdentifyingFields() {
        return this.serializedIdentifyingFields;
    }

    /**
     * Get the {@link InternalTargetInfo} containing information about secret fields.
     *
     * @return The {@link InternalTargetInfo}.
     */
    InternalTargetInfo getInternalTargetInfo() {
        return info;
    }

    /**
     * Internal decorated {@link TargetInfo} that keeps track
     * of {@link AccountValue}s that are supposed to be secret.
     */
    @Immutable
    static class InternalTargetInfo {
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
            final TargetSpec.Builder targetSpec =
                    createTargetSpecBuilder(targetInfo, values, Optional.empty());
            TargetInfo.Builder targetInfoBuilder = TargetInfo.newBuilder().setId(targetInfo.getId())
                    .setSpec(targetSpec);
            if (targetInfo.hasDisplayName()) {
                targetInfoBuilder.setDisplayName(targetInfo.getDisplayName());
            }
            return targetInfoBuilder.build();
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
            final TargetSpec.Builder targetSpec =
                    createTargetSpecBuilder(targetInfo, values, Optional.empty());
            TargetInfo.Builder targetInfoBuilder = TargetInfo.newBuilder().setId(targetInfo.getId())
                    .setSpec(targetSpec);
            if (targetInfo.hasDisplayName()) {
                targetInfoBuilder.setDisplayName(targetInfo.getDisplayName());
            }
            return targetInfoBuilder.build();
        }
    }

    private static String generateSerializedIdentifiers(InternalTargetInfo info,
                                                        ProbeInfo probeInfo) throws InvalidTargetException {
        final Map<String, String> accountValuesMap = info.targetInfo.getSpec().getAccountValueList()
            .stream()
            .filter(TopologyProcessorDTO.AccountValue::hasStringValue)
            .collect(Collectors.toMap(TopologyProcessorDTO.AccountValue::getKey,
                TopologyProcessorDTO.AccountValue::getStringValue));


            return TargetUtil.getTargetId(probeInfo.getAccountDefinitionList(),
                accountValuesMap, probeInfo.getTargetIdentifierFieldList(), InvalidTargetException::new);

    }

    @Override
    public String toString() {
        return getDisplayName() + "(" + id + ")";
    }

    @Override
    public void onProbeRegistered(final long probeId, final ProbeInfo newProbeInfo) {
        if (probeId == this.probeId && !this.probeInfo.equals(newProbeInfo)) {
            refreshProbeInfo(newProbeInfo, true, info.targetInfo.getSpec());
        }
    }

    /**
     * When a probe registers itself with a new {@link ProbeInfo} we need to update all fields
     * in the target that are affected by fields in the {@link ProbeInfo}.
     *
     * @param probeInfo The {@link ProbeInfo}.
     * @param validateAccountValues If true, validate account values.
     * @param targetSpec The {@link TargetSpec}.
     */
    private void refreshProbeInfo(@Nonnull final ProbeInfo probeInfo,
            final boolean validateAccountValues, @Nonnull final TargetSpec targetSpec) {
        this.probeInfo = probeInfo;
        accountDefEntryList = probeInfo.getAccountDefinitionList();

        final ImmutableSet.Builder<String> secretFieldBuilder = new ImmutableSet.Builder<>();
        hasGroupScope = checkForGroupScope(accountDefEntryList);
        if (validateAccountValues) {
            probeInfo.getAccountDefinitionList()
                .stream()
                .map(AccountValueAdaptor::wrap)
                .filter(AccountDefEntry::isSecret)
                .map(AccountDefEntry::getName)
                .forEach(secretFieldBuilder::add);
        }
        final Set<String> secretFields = secretFieldBuilder.build();

        final String targetDisplayName = computeDisplayName(this.probeInfo,
                targetSpec.getAccountValueList(), secretFields);

        final Builder targetInfoBuilder = TargetInfo.newBuilder()
                .setId(id)
                .setSpec(targetSpec)
                .setDisplayName(targetDisplayName);

        final TargetInfo targetInfo = targetInfoBuilder.build();

        noSecretDto = removeSecretAccountVals(targetInfo, secretFields);
        noSecretAnonymousDto = removeSecretAnonymousAccountVals(targetInfo, secretFields);

        info = new InternalTargetInfo(targetInfo, secretFields);
        if (!info.targetInfo.hasDisplayName()) {
            logger.error("Empty target display name for target id " + this.id);
        }
    }
}
