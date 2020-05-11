package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue.PropertyValueList;
import com.vmturbo.topology.processor.probes.AccountValueAdaptor;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Utility class for account values validation.
 */
public class AccountValueVerifier {

    /**
     * Specific verifiers for predefined account values.
     */
    private static Map<PredefinedAccountDefinition, Function<AccountValue, String>> verifiers;

    static {
        final Map<PredefinedAccountDefinition, Function<AccountValue, String>> verifiersMap =
                        new EnumMap<>(PredefinedAccountDefinition.class);
        final Function<AccountValue, String> noValidation = (value) -> null;
        verifiersMap.put(PredefinedAccountDefinition.Username, noValidation);
        verifiersMap.put(PredefinedAccountDefinition.Password, noValidation);
        verifiersMap.put(PredefinedAccountDefinition.UseSSL, noValidation);
        verifiersMap.put(PredefinedAccountDefinition.ScopedVms,
                        (av) -> verifyScopedField(PredefinedAccountDefinition.ScopedVms, av));
        verifiers = Collections.unmodifiableMap(verifiersMap);
    }

    private AccountValueVerifier() {}

    /**
     * Performs account entry validation.
     *
     * @param inputField input field to validate
     * @param allFields all input fields in one map
     * @param entryWrapped account definition wrapper
     * @param accountDefinition account definition itself
     * @return string representation of validation errors, or {@code null} if no errors found
     */
    private static String validateAccountEntry(final AccountValue inputField,
            @Nonnull Map<String, AccountValue> allFields, @Nonnull AccountDefEntry entryWrapped,
            @Nonnull final Discovery.AccountDefEntry accountDefinition) {
        Objects.requireNonNull(accountDefinition);
        if (inputField == null) {
            try {
                return isMandatory(entryWrapped, allFields) ?
                        String.format("Missing mandatory field %s", entryWrapped.getName()) : null;
            } catch (InvalidTargetException e) {
                return String.format("Failed detecting mandatity of a field \"%s\": %s",
                        entryWrapped.getName(), e.getErrors());
            }
        }
        switch (accountDefinition.getDefinitionCase()) {
            case CUSTOM_DEFINITION:
                return verifyCustom(accountDefinition.getCustomDefinition(), inputField);
            case PREDEFINED_DEFINITION:
                return verifyPredefined(accountDefinition.getPredefinedDefinition(), inputField);
            default:
                return "Malformed account definition. Could not verify field "
                                + inputField.getKey();
        }
    }

    private static boolean isMandatory(@Nonnull AccountDefEntry accountDefEntry,
            @Nonnull Map<String, AccountValue> accountValues) throws InvalidTargetException {
        if (!accountDefEntry.isRequired()) {
            return false;
        } else if (!accountDefEntry.getDependencyField().isPresent()) {
            return true;
        } else {
            final AccountValue dependencyField =
                    accountValues.get(accountDefEntry.getDependencyField().get().getFirst());
            if (dependencyField == null) {
                throw new InvalidTargetException(String.format(
                        "Malformed account definition. Dependency field \"%s\" is absent",
                        accountDefEntry.getDependencyField().get().getFirst()));
            }
            return Pattern.compile(accountDefEntry.getDependencyField().get().getSecond())
                    .matcher(dependencyField.getStringValue())
                    .matches();
        }
    }

    private static String verifyScopedField(PredefinedAccountDefinition scopedProperty,
                    AccountValue av) {
        if (!av.getStringValue().isEmpty()) {
            return String.format(
                            "Field %s is a group scope property. It should not have string value set",
                            av.getKey());
        }
        int counter = 0;
        for (PropertyValueList scopedVm : av.getGroupScopePropertyValuesList()) {
            if (scopedVm.getValueCount() != scopedProperty.getGroupScopeFields().size()) {
                return String.format(
                                "Group scope object %d is malformed. Expected %d fields, but found %d instead",
                                counter, scopedProperty.getGroupScopeFields().size(),
                                scopedVm.getValueCount());
            }
            counter++;
        }
        return null;
    }

    private static String verifyPredefined(String predefinedDefinition, AccountValue value) {
        final PredefinedAccountDefinition definition;
        try {
            definition = PredefinedAccountDefinition.valueOf(predefinedDefinition);
        } catch (IllegalArgumentException ex) {
            return String.format("Predefined field type %s not found: %s", predefinedDefinition,
                            ex.getMessage());
        }
        final Function<AccountValue, String> verifier = Objects.requireNonNull(
                        verifiers.get(definition),
                        "Verifier not found for predefined type " + predefinedDefinition);
        return verifier.apply(value);
    }

    private static String verifyCustom(CustomAccountDefEntry entry, AccountValue inputField) {
        final String accountField = inputField.getStringValue();
        if (!accountField.isEmpty() && entry.hasVerificationRegex()) {
            final Pattern p = Pattern.compile(entry.getVerificationRegex(), Pattern.DOTALL);
            if (!p.matcher(accountField).matches()) {
                return String.format("Value %s doesn't match verification regex.", accountField);
            }
        }
        return null;
    }

    /**
     * Validate the target spec.
     *
     * @param spec Spec for the target.
     * @param probeStore Store containing the probe the target applies to.
     * @throws InvalidTargetException If the target spec is not valid.
     */
    public static void validate(@Nonnull final TopologyProcessorDTO.TargetSpec spec,
                    @Nonnull final ProbeStore probeStore) throws InvalidTargetException {
        final Optional<ProbeInfo> info = probeStore.getProbe(spec.getProbeId());
        if (!info.isPresent()) {
            // Probe not registered. Should there be a different exception/return here?
            throw new InvalidTargetException(
                            String.format("Probe %s not registered.", spec.getProbeId()));
        }

        final Map<String, TopologyProcessorDTO.AccountValue> inputFields =
                        spec.getAccountValueList().stream().collect(Collectors.toMap(
                                        TopologyProcessorDTO.AccountValue::getKey, Function.identity()));
        final Map<Discovery.AccountDefEntry, AccountDefEntry> entries =
                        info.get().getAccountDefinitionList().stream().collect(Collectors.toMap(
                                        Function.identity(), AccountValueAdaptor::wrap));

        final Set<String> acceptedAccountFields = entries.values().stream().map(AccountDefEntry::getName)
                        .collect(Collectors.toSet());

        final Set<String> mandatorySecretFields = info.get().getAccountDefinitionList().stream()
                        .filter(Discovery.AccountDefEntry::getMandatory)
                        .map(AccountValueAdaptor::wrap)
                        .filter(AccountDefEntry::isSecret)
                        .map(AccountDefEntry::getName)
                        .collect(Collectors.toSet());

        final List<String> fieldErrors = new ArrayList<>();
        // Check for input fields that the probe doesn't recognize.
        fieldErrors.addAll(inputFields.keySet().stream()
                        .filter(name -> !acceptedAccountFields.contains(name))
                        .map(name -> "Unknown field: " + name).collect(Collectors.toList()));
        // Check for mandatory secret fields that the input fields doesn't contain.
        fieldErrors.addAll(mandatorySecretFields.stream()
                        .filter(secretField -> !inputFields.keySet().contains(secretField))
                        .map(name -> "Unknown secret field: " + name).collect(Collectors.toList()));
        // Check that the input fields that the probe DOES recognize are valid and well-formed.
        fieldErrors.addAll(entries.entrySet().stream()
                        .map(entry -> validateAccountEntry(
                                        inputFields.get(entry.getValue().getName()),
                                        inputFields,
                                        entry.getValue(), entry.getKey()))
                        .filter(Objects::nonNull).collect(Collectors.toList()));
        if (!fieldErrors.isEmpty()) {
            throw new InvalidTargetException(fieldErrors);
        }
    }
}
