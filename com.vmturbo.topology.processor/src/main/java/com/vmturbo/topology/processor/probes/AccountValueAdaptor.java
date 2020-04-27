package com.vmturbo.topology.processor.probes;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;

/**
 * This class is used to adopt Protobuf representation of account definitions to a stardard way -
 * using {@link AccountDefEntry} interface.
 */
public class AccountValueAdaptor {
    /**
     * Default regular expression to validate a field.
     */
    static final String DEFAULT_STRING_REGEXP = ".*";

    /**
     * Hide constructor for utility class.
     */
    private AccountValueAdaptor() {}

    /**
     * Returns wrapper object, implementing {@link AccountDefEntry}, suitable for the input protobuf
     * representation.
     *
     * @param entry protobuf representation of account definition
     * @return new wrapper object.
     */
    public static AccountDefEntry wrap(Discovery.AccountDefEntry entry) {
        switch (entry.getDefinitionCase()) {
            case CUSTOM_DEFINITION:
                return new CustomFieldInfoGetter(entry);
            case PREDEFINED_DEFINITION:
                return new PredefinedInfoGetter(entry);
            default:
                throw new IllegalArgumentException("Could not recognize entry " + entry);
        }
    }

    /**
     * Represent abstract account definition info getter functionality.
     */
    private abstract static class AbstractInfoGetter implements AccountDefEntry {
        private final Discovery.AccountDefEntry entry;

        AbstractInfoGetter(Discovery.AccountDefEntry entry) {
            this.entry = entry;
        }

        @Override
        public boolean isRequired() {
            return entry.getMandatory();
        }

        @Nullable
        @Override
        public String getDefaultValue() {
            return entry.hasDefaultValue() ? entry.getDefaultValue() : null;
        }

        @Nullable
        @Override
        public List<String> getAllowedValues() {
            return entry.getAllowedValuesList();
        }

        @Nonnull
        @Override
        public Optional<Pair<String, String>> getDependencyField() {
            return entry.hasDependencyKey() ?
                    Optional.of(Pair.create(entry.getDependencyKey(), entry.getDependencyValue())) :
                    Optional.empty();
        }
    }

    /**
     * Account definitions info getter for custom values.
     */
    private static class CustomFieldInfoGetter extends AbstractInfoGetter {

        private final CustomAccountDefEntry customEntry;

        CustomFieldInfoGetter(Discovery.AccountDefEntry entry) {
            super(entry);
            customEntry = Objects.requireNonNull(entry.getCustomDefinition());
        }

        @Override
        public String getName() {
            return customEntry.getName();
        }

        @Override
        public boolean isSecret() {
            return customEntry.getIsSecret();
        }

        @Override
        public String getDescription() {
            return customEntry.getDescription();
        }

        @Override
        public String getDisplayName() {
            return customEntry.getDisplayName();
        }

        @Override
        public String getVerificationRegex() {
            return customEntry.getVerificationRegex();
        }

        @Override
        public AccountFieldValueType getValueType() {
            switch (customEntry.getFieldTypeCase()) {
                case GROUP_SCOPE:
                case ENTITY_SCOPE:
                    return AccountFieldValueType.GROUP_SCOPE;
                case PRIMITIVE_VALUE:
                    switch (customEntry.getPrimitiveValue()) {
                        case BOOLEAN:
                            return AccountFieldValueType.BOOLEAN;
                        case NUMERIC:
                            return AccountFieldValueType.NUMERIC;
                        case STRING:
                            return AccountFieldValueType.STRING;
                    }
                default:
                    throw new RuntimeException("FieldType is not set for entry " + customEntry);
            }
        }
    }

    /**
     * Account definition info getter for predefined fields.
     */
    private static class PredefinedInfoGetter extends AbstractInfoGetter {

        private final PredefinedAccountDefinition enumVal;

        PredefinedInfoGetter(Discovery.AccountDefEntry accountDefinition) {
            super(accountDefinition);
            enumVal = PredefinedAccountDefinition
                            .valueOf(accountDefinition.getPredefinedDefinition());
        }

        @Override
        public String getName() {
            return enumVal.getValueKey();
        }

        @Override
        public boolean isSecret() {
            return enumVal.isSecret();
        }

        @Override
        public String getDescription() {
            return enumVal.getDescription();
        }

        @Override
        public String getDisplayName() {
            return enumVal.getDisplayName();
        }

        @Override
        public String getVerificationRegex() {
            return DEFAULT_STRING_REGEXP;
        }


            @Override
        public AccountFieldValueType getValueType() {
            if (enumVal.getGroupScopeFields() != null) {
                return AccountFieldValueType.GROUP_SCOPE;
            }
            if (String.class.isAssignableFrom(enumVal.getValueClass())) {
                return AccountFieldValueType.STRING;
            }
            if (Boolean.class.isAssignableFrom(enumVal.getValueClass())) {
                return AccountFieldValueType.BOOLEAN;
            }
            if (Number.class.isAssignableFrom(enumVal.getValueClass())) {
                return AccountFieldValueType.NUMERIC;
            }
            throw new RuntimeException("Unknown field value class specified for enum " + enumVal
                            + ": " + enumVal.getValueClass());
        }
    }
}
