package com.vmturbo.components.test.utilities.alert.report;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import tec.units.ri.unit.MetricPrefix;

/**
 * A formatter for use in formatting numbers to be used in results tabulation.
 *
 * Smaller numbers are formatted using a provided {@link DecimalFormat}.
 * Large numbers are formatted by suffixing with "k", "M", "G", "T", etc. as appropriate.
 */
public class NumberFormatter {
    /**
     * Formatter for formatting small numbers in the table.
     */
    private final DecimalFormat smallNumberFormatter;

    /**
     * The default decimal formatter to use when formatting very small numbers.
     * Large numbers are formatted differently than small ones.
     */
    private static final DecimalFormat DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.#####");
    static {
        // Round numbers so that 1.5 -> 2, 1.4 -> 1 (typical rounding scheme used in school)
        DEFAULT_DECIMAL_FORMAT.setRoundingMode(RoundingMode.HALF_UP);
    }

    /**
     * Construct a new NumberFormatter that uses the default number format for small numbers.
     */
    public NumberFormatter() {
        this(DEFAULT_DECIMAL_FORMAT);
    }

    @VisibleForTesting
    NumberFormatter(@Nonnull final DecimalFormat smallNumberFormat) {
        this.smallNumberFormatter = smallNumberFormat;
        smallNumberFormatter.setRoundingMode(RoundingMode.CEILING);
    }

    /**
     * A map for use in determining thresholds at which to apply suffixes to large numbers.
     */
    private static final NavigableMap<Double, String> suffixes = new TreeMap<>();
    static {
        suffixes.put(1_000d, "k");
        suffixes.put(1_000_000d, "M");
        suffixes.put(1_000_000_000d, "G");
        suffixes.put(1_000_000_000_000d, "T");
        suffixes.put(1_000_000_000_000_000d, "P");
        suffixes.put(1_000_000_000_000_000_000d, "E");
    }

    /**
     * Adapted from the answer here:
     * http://stackoverflow.com/questions/4753251/how-to-go-about-formatting-1200-to-1-2k-in-java
     *
     * But instead of formatting longs, we are formatting doubles, and we also append decimal portions
     * up to 2 digits and append decimals when the converted value is 2 digits or less instead of only
     * on a single digit.
     *
     * @param value The double value to smallNumberFormatter
     * @return
     */
    public String format(double value) {
        if (value < 0) return "-" + format(-value);
        if (value < 1000) return smallNumberFormatter.format(value);

        Entry<Double, String> e = suffixes.floorEntry(value);
        double divideBy = e.getKey();
        String suffix = e.getValue();

        // Divide the value by the divisor appropriate for its size with 2 additional digits.
        long truncated = (long)(value / (divideBy / 100));

        // Determine if we should include a decimal for the formatted number.
        // Attach a decimal if the completed number is 2 digits or less and the decimal part is non-zero.
        boolean hasDecimal = truncated < 10000 && (truncated / 100d) != (truncated / 100);

        // Truncate the 2 additional digits from the additional division and optionally apply
        // up 2 additional decimal places.
        return hasDecimal ? (truncated / 100d) + suffix : (truncated / 100) + suffix;
    }

    /**
     * Note that base units are different for different types of data.
     *
     * For time: base units are in seconds.
     * For data: base units are in bytes.
     */
    private static final Map<String, Function<Double, Double>> BASE_TRANSFORMATIONS = new HashMap<>();
    static {
        /**
         * Time transformations
         */
        BASE_TRANSFORMATIONS.put("s", timeTransform(TimeUnit.SECONDS));
        BASE_TRANSFORMATIONS.put("sec", timeTransform(TimeUnit.SECONDS));
        BASE_TRANSFORMATIONS.put("second", timeTransform(TimeUnit.SECONDS));
        BASE_TRANSFORMATIONS.put("seconds", timeTransform(TimeUnit.SECONDS));

        BASE_TRANSFORMATIONS.put("m", timeTransform(TimeUnit.MINUTES));
        BASE_TRANSFORMATIONS.put("min", timeTransform(TimeUnit.MINUTES));
        BASE_TRANSFORMATIONS.put("minute", timeTransform(TimeUnit.MINUTES));
        BASE_TRANSFORMATIONS.put("minutes", timeTransform(TimeUnit.MINUTES));

        BASE_TRANSFORMATIONS.put("h", timeTransform(TimeUnit.HOURS));
        BASE_TRANSFORMATIONS.put("hour", timeTransform(TimeUnit.HOURS));
        BASE_TRANSFORMATIONS.put("hours", timeTransform(TimeUnit.HOURS));

        BASE_TRANSFORMATIONS.put("d", timeTransform(TimeUnit.DAYS));
        BASE_TRANSFORMATIONS.put("day", timeTransform(TimeUnit.DAYS));
        BASE_TRANSFORMATIONS.put("days", timeTransform(TimeUnit.DAYS));

        /**
         * SI-unit and storage-unit transformations
         */
        BASE_TRANSFORMATIONS.put("k", siTransform(MetricPrefix.KILO));
        BASE_TRANSFORMATIONS.put("kb", siTransform(MetricPrefix.KILO));

        BASE_TRANSFORMATIONS.put("M", siTransform(MetricPrefix.MEGA));
        BASE_TRANSFORMATIONS.put("mb", siTransform(MetricPrefix.MEGA));

        BASE_TRANSFORMATIONS.put("g", siTransform(MetricPrefix.GIGA));
        BASE_TRANSFORMATIONS.put("gb", siTransform(MetricPrefix.GIGA));

        BASE_TRANSFORMATIONS.put("t", siTransform(MetricPrefix.TERA));
        BASE_TRANSFORMATIONS.put("tb", siTransform(MetricPrefix.TERA));

        BASE_TRANSFORMATIONS.put("p", siTransform(MetricPrefix.PETA));
        BASE_TRANSFORMATIONS.put("pb", siTransform(MetricPrefix.PETA));

        BASE_TRANSFORMATIONS.put("e", siTransform(MetricPrefix.EXA));
        BASE_TRANSFORMATIONS.put("eb", siTransform(MetricPrefix.EXA));
    }

    private static Function<Double, Double> timeTransform(@Nonnull final TimeUnit conversionUnit) {
        return (t) -> t * conversionUnit.toSeconds(1);
    }

    private static Function<Double, Double> siTransform(@Nonnull final MetricPrefix siPrefix) {
        return (value) -> (double)siPrefix.getConverter().convert(value);
    }

    /**
     * Transform a value to a base unit by applying a transformation
     * determined by the units suffix. Transforms, for example, gigabytes or megabytes to bytes,
     * or hours and days to seconds.
     *
     * Suffixes are case-insensitive with the exception that lowercase m is minutes and capital m is Mega.
     *
     * Note that storage-related suffixes use SI notation and NOT IEC notation (that is they are
     * based on powers of 10 and not powers of 2).
     *
     * Examples:
     * transform(4.5, "kb") -> 4500
     * transform(1.2, "m") -> 72 (1.2 minutes in seconds)
     * transform(1.2, "M") -> 1_200_000 (1.2 mega)
     * transform(3, "d") -> 259_200 (4 days in seconds)
     * transform(6, "G") -> 6_000_000_000
     *
     * @param value The value to transform according to the units suffix.
     * @param unitsSuffix The suffix that specifies the transformation to apply.
     * @return The value transformed by
     */
    public static double transformToBaseUnitBySuffix(double value, @Nonnull final String unitsSuffix)
        throws NumberFormatException {
        Optional<Function<Double, Double>> transform = Optional.ofNullable(BASE_TRANSFORMATIONS.get(unitsSuffix));
        transform = transform.isPresent() ?
            transform :
            Optional.ofNullable(BASE_TRANSFORMATIONS.get(StringUtils.lowerCase(unitsSuffix)));

        return transform
            .map(transformation -> transformation.apply(value))
            .orElseThrow(() -> new NumberFormatException("Unknown units suffix: " + unitsSuffix));
    }
}
