package com.vmturbo.components.common;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.featureflags.FeatureFlags;

/**
 * Utility class with helper methods that can be useful in interpreting properties.
 */
public class PropertiesHelpers {
    private static final Logger logger = LogManager.getLogger();

    // Valid duration units for Duration objects.
    @VisibleForTesting
    static final Set<String> durationUnits =
            new HashSet<>(Arrays.asList("D", "H", "M", "S"));

    // Units that represent time (Hours, Minutes, Seconds).
    private static final Set<String> timeUnits = new HashSet<>(Arrays.asList("H", "M", "S"));

    private static final Map<ChronoUnit, String> chronoUnitToIdentifier =
            ImmutableMap.of(ChronoUnit.DAYS, "D", ChronoUnit.HOURS, "H", ChronoUnit.MINUTES, "M",
                    ChronoUnit.SECONDS, "S");

    private static <T> Predicate<T> not(Predicate<T> t) {
        return t.negate();
    }

    private static final Pattern validCharsPattern = Pattern.compile("[a-zA-Z0-9.]");

    /**
     * Converts a duration property to a {@link Duration}. This method allows the developer to
     * override the intended time unit of the property and have more flexibility to test duration
     * based features without waiting sometimes days for them to get triggered, while maintaining
     * backwards compatibility. For example, if a property named entityExpirationTimeDays has a raw
     * integer value, this gets converted to that many days. However, this method allows a developer
     * to assign a value of e.g. 3h20m, which will get converted to "PT3H20M", allowing for more
     * flexibility.
     *
     * <p>To guard against accidental usage of this functionality by the customer, we use a Feature
     * Flag. When the FF is set to false, the method throws an exception if the property value is
     * not a raw number.</p>
     *
     * <p>TODO: Support negative duration values.
     * Currently, this method does not support negative values in duration numbers. This is
     * because any non-alphanumeric character is ignored (including the minus sign). However, there
     * are cases of duration properties that support negative values, as well as durations like
     * 1d-1h which would translate as 23 hours. This method should be able to support both of the
     * above default value examples to be able to support wider use.</p>
     *
     * @param propertyName the name of the duration property.
     * @param propertyValue the value of the duration property.
     * @param propertyUnit the {@link ChronoUnit} of the duration property.
     * @return the property converted to a {@link Duration}.
     * @throws IllegalArgumentException in case the property value is invalid.
     */
    public static Duration parseDuration(String propertyName, String propertyValue,
            ChronoUnit propertyUnit) throws IllegalArgumentException {
        if (propertyValue == null || propertyValue.isEmpty()) {
            throw new IllegalArgumentException("Property " + propertyName + " must not be empty or null.");
        }

        // If Feature Flag is disabled, accept only a raw number as property value.
        if (!FeatureFlags.ENABLE_FLEXIBLE_DURATIONS.isEnabled() && !StringUtils.isNumeric(propertyValue)) {
            throw new IllegalArgumentException("Property " + propertyName + " must be a raw number.");
        }

        // In a normal scenario, the user defines the property value as a single number and the time
        // unit is indicated by the property name. In this case (when the property value is a pure
        // integer), we turn it into a duration with whatever time unit the property uses.
        if (StringUtils.isNumeric(propertyValue)) {
            // If time unit is hours, minutes or seconds, a "T" must occur before the time section.
            String durationPrefix =
                    timeUnits.contains(chronoUnitToIdentifier.get(propertyUnit)) ? "PT" : "P";
            return Duration.parse(
                    durationPrefix + propertyValue + chronoUnitToIdentifier.get(propertyUnit));
        }

        // Warn about ignoring any non-alphanumeric character except period (for fractional seconds)
        // and filter them out.
        if (!validCharsPattern.matcher(propertyName).matches()) {
            logger.warn("Ignoring any non-alphanumeric characters except period contained in {}",
                    propertyName);
            propertyValue = propertyValue.replaceAll("[^A-Za-z0-9.]", "");
        }

        BigDecimal[] numbers = Arrays.stream(propertyValue.split("[^0-9.]+"))
                .map(BigDecimal::new)
                .toArray(BigDecimal[]::new);
        String[] units = Arrays.stream(propertyValue.split("[0-9.]+"))
                .filter(not(String::isEmpty))
                .map(String::toUpperCase)
                .toArray(String[]::new);

        // If there is a number that is not followed by a unit.
        if (numbers.length != units.length) {
            throw new IllegalArgumentException("Each number must be followed by its time unit.");
        }

        for (String unit : units) {
            // If there are more than one time units next to each other.
            if (unit.length() > 1) {
                throw new IllegalArgumentException("Each time unit should be prefixed by a number");
            }
            // If a specified time unit is neither of "H", "D", "M", or "S".
            if (!durationUnits.contains(unit)) {
                throw new IllegalArgumentException(unit + " is not a valid duration unit.");
            }
        }

        // Map associating time units with their value.
        Map<String, BigDecimal> durations = new HashMap<>();
        for (int i = 0; i < units.length; i++) {
            if (durations.containsKey(units[i])) {
                throw new IllegalArgumentException(
                        "Property " + propertyName + " contains duplicate time unit: " + units[i]);
            }
            // only seconds are allowed to include decimal points
            if (numbers[i].scale() > 0 && !units[i].equals("S")) {
                throw new IllegalArgumentException(
                        "Fractional values are allowed only for seconds, not " + units[i]);
            }
            // seconds can have up to 9 fractional digits
            if (units[i].equals("S") && numbers[i].scale() > 9) {
                throw new IllegalArgumentException("Seconds may have no more than 9 fractional digits");
            }
            durations.put(units[i], numbers[i]);
        }

        // Every duration string must start with "P".
        String durationString = "P";
        if (durations.containsKey("D")) {
            durationString = durationString.concat(durations.get("D") + "D");
        }
        if (durations.containsKey("H") || durations.containsKey("M") || durations.containsKey("S")) {
            durationString = durationString.concat("T");
            if (durations.containsKey("H")) {
                durationString = durationString.concat(durations.get("H") + "H");
            }
            if (durations.containsKey("M")) {
                durationString = durationString.concat(durations.get("M") + "M");
            }
            if (durations.containsKey("S")) {
                durationString = durationString.concat(durations.get("S") + "S");
            }
        }
        try {
            Duration duration = Duration.parse(durationString);
            logger.info("{} is set to: {} seconds.", propertyName, duration.getSeconds());
            return duration;
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    propertyValue + " is not a valid duration (nDnHnMnS)");
        }
    }

    /**
     * Private constructor to avoid Java implicitly creating a default public constructor, thus
     * making sure that this utility class will never get instantiated.
     *
     * @throws UnsupportedOperationException in case the constructor is called.
     */
    private PropertiesHelpers() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Utility classes cannot be instantiated.");
    }
}
