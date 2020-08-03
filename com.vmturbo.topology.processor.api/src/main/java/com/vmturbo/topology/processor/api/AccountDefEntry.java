package com.vmturbo.topology.processor.api;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Representation of account definition entry for a probe.
 */
public interface AccountDefEntry {

    /**
     * Returns name of the value field.
     *
     * @return internal name of the field
     */
    @Nonnull
    String getName();

    /**
     * Return display name to show in user interface places.
     *
     * @return name to display
     */
    @Nonnull
    String getDisplayName();

    /**
     * Description of what does this field mean.
     *
     * @return description of the field
     */
    @Nonnull
    String getDescription();

    /**
     * Return whether this field is required to be set.
     *
     * @return {@code true} if the field is required
     */
    boolean isRequired();

    /**
     * Returns whether the field is secret. Secret fields are hidden from users when they query for
     * all the fields of a target.
     *
     * @return whether the field is secret
     */
    boolean isSecret();

    /**
     * Returns type of the value, that is expected to be stored in the field.
     *
     * @return type of the valye to store
     */
    AccountFieldValueType getValueType();

    /**
     * A regular expression to validate a field. If the user's input matches the
     * regular expression, then the entry is valid.
     * For example, you can validate that the entered text is only numbers and period characters
     * for an IP address. To disable validation (allow any text), pass {@code ".*"}.
     *
     * @return description of the field
     */
    String getVerificationRegex();

    /**
     * Returns default value to use when this field's value is absent.
     *
     * @return default value
     */
    @Nullable
    String getDefaultValue();

    /**
     * Determines potential values for this field. if nonempty, field must be one of the included
     * values. If empty, any value can be used.
     *
     * @return List of strings, potentially empty, representing this field's allowed values.
     */
    @Nullable
    List<String> getAllowedValues();

    /**
     * Returns dependency field configuration. The 1st value is a dependency field key. The 2nd
     * value is a dependency field value matching expression
     *
     * <p>Value (the 2nd part of a pair) of a field referenced by {@code dependencyKey}
     * to enable (activate) this field. Value is specified using a regular expression.
     * This, specifying just a string will also work correctly.
     *
     * <p>If a referenced account value field is a boolean field, only {@code true} or
     * {@code false} are supported as values.
     *
     * <p>If a referenced account value is an enumeration ({@code allowedValues} is specified)
     * this value is only restricted to contain a subset of the allowed values united using a pipe:
     * {@code one|two|four}.
     *
     * @return dependency field if any
     */
    @Nonnull
    Optional<Pair<String, String>> getDependencyField();

}
