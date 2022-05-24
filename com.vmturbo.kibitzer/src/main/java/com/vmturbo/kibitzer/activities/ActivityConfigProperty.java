package com.vmturbo.kibitzer.activities;

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kibitzer.activities.KibitzerActivity.KibitzerActivityException;

/**
 * Class to represent a single property that's available for configuration of a {@link
 * KibitzerActivity}.
 *
 * @param <T> property value type
 */
public class ActivityConfigProperty<T> {
    private static final Logger logger = LogManager.getLogger();

    private final String name;
    private final Class<T> type;
    private String[] description = null;
    private boolean required = false;
    private T defaultValue = null;
    private Function<String, T> parser;
    private Predicate<T> validator = value -> true;
    private T value;

    ActivityConfigProperty(String name, Class<T> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    /**
     * Get a value for this property.
     *
     * @return configured value, if one has been provided, else the property default value
     */
    public T get() {
        return value != null ? value : defaultValue;
    }

    public void setDefault(T value) {
        this.defaultValue = value;
    }

    private void set(T value) {
        this.value = value;
    }

    /**
     * Configure a value for this property, by supplying a string-valued representation of the
     * value.
     *
     * @param valueString string representation of value
     * @return true if the property, false if there was a problem (will have been logged)
     */
    public boolean configure(String valueString) {
        try {
            T value = parser.apply(valueString);
            boolean valid = validator.test(value);
            if (valid) {
                set(value);
            } else {
                logger.error("Invalid value for property '{}'", getName());
            }
            return valid;
        } catch (Exception e) {
            logger.error("Failed to configure value for property '{}' from value string '{}'",
                    getName(), valueString, e);
            return false;
        }
    }

    /**
     * Add a description to this property instance.
     *
     * @param description the property description
     * @return this property
     */
    public ActivityConfigProperty<T> withDescription(String... description) {
        this.description = description;
        return this;
    }

    /**
     * Specify that this property is required.
     *
     * @return this property
     */
    public ActivityConfigProperty<T> required() {
        this.required = true;
        return this;
    }

    /**
     * Specify a default value for this property.
     *
     * @param defaultValue default value
     * @return this property
     */
    public ActivityConfigProperty<T> withDefault(T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    /**
     * Provide a parser that will produce a property value from a string representation.
     *
     * @param parser the parser, a function that produces a property type from a string
     * @return this property
     * @throws KibitzerActivityException if a parser has already been specified
     */
    public ActivityConfigProperty<T> withParser(Function<String, T> parser)
            throws KibitzerActivityException {
        if (this.parser == null) {
            this.parser = parser;
            return this;
        } else {
            throw new KibitzerActivityException(
                    String.format("Parser has already been supplied for property %s", name));
        }
    }

    /**
     * Provide a validator for this property, i.e. a {@link Predicate} that operates on the property
     * type.
     *
     * <p>Multiple validators can be provided by calling this method repeatedly. All provided
     * validators will be applied, in the order they are specified, and overall validation requires
     * that all validators individually pass.</p>
     *
     * @param validator validator for this property.
     * @param msg       a message to log if the validator fails
     * @return this property
     */
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public ActivityConfigProperty<T> withValidator(Predicate<T> validator, String msg) {
        Predicate<T> validatorToAdd =
                msg == null
                ? validator
                : (T value) -> {
                    boolean result = validator.test(value);
                    if (!result) {
                        logger.error("{}", msg);
                    }
                    return result;
                };
        this.validator = this.validator.and(validatorToAdd);
        return this;
    }

    public String[] getDescription() {
        return description;
    }

    public T getDefault() {
        return defaultValue;
    }

    public Class<T> getType() {
        return type;
    }
}
