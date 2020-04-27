package com.vmturbo.topology.processor.api.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.ProbeInfo;

/**
 * REST API classes (DTO part) for probe operations.
 */
public class ProbeRESTApi {
    /**
     * User-visible parts of the {@link AccountDefEntry} proto-generated class, extracted mostly for
     * Swagger integration for the REST API.
     */
    public static final class AccountField implements AccountDefEntry {
        @ApiModelProperty(value = "Internal field name.", required = true)
        private final String name;

        @ApiModelProperty(value = "Name to display to users.", required = true)
        private final String displayName;

        @ApiModelProperty(required = true)
        private final String description;

        @ApiModelProperty(
                        value = "If true, this field is required for operations (e.g. adding target)",
                        required = true)
        private final Boolean required;

        @ApiModelProperty(
                        value = "If true, this field is hidden in any query operations",
                        required = true)
        private final Boolean secret;

        @ApiModelProperty(
                        value = "Type of value, that should be stored in this field",
                        required = true)
        private final AccountFieldValueType valueType;
        @ApiModelProperty(
                value = "Default value to be used as a field value, if field is not specified.",
                required = false)
        private final String defaultValue;

        @ApiModelProperty(
                value = "Determines potential values for this field. if nonempty, field must be one"
                        + " of the included values. If empty, any value can be used.",
                required = false)
        private final List<String> allowedValues;

        @ApiModelProperty(
            value = "Verification regex to validate a field",
            required = false)
        private final String verificationRegexp;


        @ApiModelProperty(value = "Dependency field configuration")
        private final Pair<String, String> dependencyField;

        /**
         * Protected constructor, suitable only for deserialization purposes.
         */
        protected AccountField() {
            this.name = null;
            this.displayName = null;
            this.description = null;
            this.required = null;
            this.secret = null;
            this.valueType = null;
            this.defaultValue = null;
            this.allowedValues = Collections.emptyList();
            this.verificationRegexp = null;
            this.dependencyField = null;
        }

        public AccountField(@Nonnull final String name, @Nonnull final String displayName,
                            @Nonnull final String description, final boolean required,
                            final boolean secret, AccountFieldValueType valueType,
                            @Nullable String defaultValue, @Nullable List<String> allowedValues,
                            final String verificationRegexp,
                            @Nullable Pair<String, String> dependencyField) {
            this.name = Objects.requireNonNull(name);
            this.displayName = Objects.requireNonNull(displayName);
            this.description = Objects.requireNonNull(description);
            this.required = required;
            this.secret = secret;
            this.valueType = valueType;
            this.defaultValue = defaultValue;
            this.allowedValues = allowedValues;
            this.verificationRegexp = verificationRegexp;
            this.dependencyField = dependencyField;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public boolean isRequired() {
            return required;
        }

        @Override
        public boolean isSecret() {
            return secret;
        }

        @Override
        public AccountFieldValueType getValueType() {
            return valueType;
        }

        @Override
        public String getVerificationRegex() {
            return verificationRegexp;
        }

        @Override
        @Nullable
        public String getDefaultValue() {
            return defaultValue;
        }

        @Override
        @Nullable
        public List<String> getAllowedValues() {
            return allowedValues;
        }

        @Nonnull
        @Override
        public Optional<Pair<String, String>> getDependencyField() {
            return Optional.ofNullable(dependencyField);
        }
    }

    /**
     * User-visible parts of the {@link ProbeInfo} proto-generated class, extracted mostly for
     * Swagger integration for the REST API.
     */
    public static final class ProbeDescription implements ProbeInfo {
        @ApiModelProperty(value = "The ID of the probe.", required = true)
        private final long id;

        @ApiModelProperty(value = "The category of the probe (Hypervisor, etc.).", required = true)
        private final String category;

        @ApiModelProperty(value = "The type of the probe (vCenter, HyperV, etc.).", required = true)
        private final String type;

        @ApiModelProperty(value = "Indicates if the probe will show in the UI",
            required = true)
        private final CreationMode creationMode;

        @ApiModelProperty(
                        value = "Input fields required for target management (IP, username, etc.).",
                        required = true)
        private List<AccountField> accountFields;

        @ApiModelProperty(
                        value = "Error description (if any) that occurred during probe operation.",
                        required = true)
        private final String error;

        @ApiModelProperty(value = "If non-null, the list of input fields, identifying the target.")
        private final List<String> identifyingFields;

        @ApiModelProperty(value = "Entity types and actions that can be applied to them.")
        private final List<ProbeActionCapability> actionPolicies;

        /**
         * Protected constructor, suitable only for deserialization purposes.
         */
        protected ProbeDescription() {
            this.id = -1;
            this.category = null;
            this.type = null;
            this.creationMode = CreationMode.STAND_ALONE;
            this.accountFields = null;
            this.error = null;
            this.identifyingFields = null;
            this.actionPolicies = null;
        }

        public ProbeDescription(final long probeId, @Nonnull final String type,
                @Nonnull final String category,
                @Nonnull final CreationMode creationMode,
                @Nonnull final List<AccountField> accountFields,
                @Nonnull final List<String> identifyingFields,
                @Nonnull final List<ProbeActionCapability> actionPolicies) {
            this.id = probeId;
            this.type = Objects.requireNonNull(type);
            this.category = Objects.requireNonNull(category);
            this.creationMode = creationMode;
            this.accountFields = accountFields;
            this.error = null;
            this.identifyingFields = identifyingFields;
            this.actionPolicies = ImmutableList.copyOf(Objects.requireNonNull(actionPolicies,
                    "Action policies shouldn't be null."));
        }

        /**
         * Constructor for empty ProbeDescription which will be created if
         * ProbeStore will not able to find ProbeInfo with the certain id.
         *
         * @param error provided error text
         */
        public ProbeDescription(@Nonnull final String error) {
            this.id = -1;
            this.category = null;
            this.type = null;
            this.creationMode = CreationMode.STAND_ALONE;
            this.accountFields = null;
            this.identifyingFields = null;
            this.error = Objects.requireNonNull(error);
            this.actionPolicies = null;
        }

        public List<AccountField> getAccountFields() {
            return accountFields;
        }

        public void setAccountFields(List<AccountField> accountFields) {
            this.accountFields = accountFields;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public String getCategory() {
            return Objects.requireNonNull(category, "category field is absent");
        }

        @Nonnull
        @Override
        public CreationMode getCreationMode() {
            return creationMode;
        }

        @Override
        public String getType() {
            return Objects.requireNonNull(type, "type field is absent");
        }

        public String getError() {
            return error;
        }

        @Override
        public List<AccountDefEntry> getAccountDefinitions() {
            return Collections.unmodifiableList(
                            Objects.requireNonNull(accountFields, "accountFields field is absent"));
        }

        @Override
        public List<String> getIdentifyingFields() {
            return identifyingFields;
        }

        /**
         * Returns actions that can be aplied to probe entities.
         *
         * @return list of action policies of probe
         */
        @Nullable
        public List<ProbeActionCapability> getActionPolicies() {
            return actionPolicies;
        }
    }

    /**
     * Response object for the GET call to /probe.
     */
    public static final class GetAllProbes {
        @ApiModelProperty(value = "List of all the regustered probes.", required = true)
        private final List<ProbeDescription> probes;

        protected GetAllProbes() {
            probes = null;
        }

        public GetAllProbes(@Nonnull final List<ProbeDescription> probes) {
            this.probes = Objects.requireNonNull(probes);
        }

        public List<ProbeDescription> getProbes() {
            return Objects.requireNonNull(probes, "probes field is absent");
        }
    }

}
