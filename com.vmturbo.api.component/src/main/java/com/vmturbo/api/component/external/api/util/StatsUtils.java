package com.vmturbo.api.component.external.api.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class StatsUtils {

    private static final Logger logger = LogManager.getLogger();

    private final static String KBIT_SEC = "Kbit/sec";
    private final static String BIT_SEC = "bit/sec";

    // map of commodity number to units-multiplier pair
    private final static Map<Integer, Pair<String, Integer>> UNITS_CONVERTER = ImmutableMap.of(
        CommodityType.PORT_CHANEL_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.NET_THROUGHPUT_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.IO_THROUGHPUT_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.SWAPPING_VALUE, new Pair<>(BIT_SEC, 8));

    /**
     * Convert the default commodity units into converted units with multiplier that we need to
     * to the current value.
     *
     * @param commodityTypeValue the target commodity type value.
     * @param commodityTypeUnits the current commodity units.
     * @return pair of the converted units and multiplier.
     */
    public static Pair<String, Integer> getConvertedUnits(final int commodityTypeValue,
        @Nonnull final CommodityTypeUnits commodityTypeUnits) {
        if (!UNITS_CONVERTER.containsKey(commodityTypeValue)) {
            logger.warn("No converted units found for commodity type {}, will use the original " +
                "units", commodityTypeValue);
            return new Pair<>(commodityTypeUnits.getUnits(), 1);
        }
        return UNITS_CONVERTER.get(commodityTypeValue);
    }

    /**
     * Multiply all the values of a {@link StatValueApiDTO}.
     *
     * @param valueDTO stat value DTO.
     * @param multiplier the multiplier that apply to the stat value.
     */
    public static void convertDTOValues(@Nonnull final StatValueApiDTO valueDTO,
        final int multiplier) {
        valueDTO.setAvg(multiply(valueDTO.getAvg(), multiplier));
        valueDTO.setMin(multiply(valueDTO.getMin(), multiplier));
        valueDTO.setMax(multiply(valueDTO.getMax(), multiplier));
        valueDTO.setTotal(multiply(valueDTO.getTotal(), multiplier));
    }

    /**
     * Round a double value to float value in specific precision.
     *
     * @param value value to round.
     * @param precision number of decimals.
     * @return rounded value.
     */
    public static float round(final double value, final int precision) {
        return new BigDecimal(value).setScale(precision, RoundingMode.HALF_UP).floatValue();
    }

    @Nullable
    private static Float multiply(@Nullable final Float d, int multiplier) {
        return d != null && !Float.isNaN(d) && !Float.isInfinite(d) ? d * multiplier : null;
    }

    /**
     * Precision enum for API to calculate the value after rounding.
     */
    public enum PrecisionEnum {
        /**
         * For cost price we set the round precision 7. Since we also do some price calculation on
         * UI, a higher precision will be good for accurate value.
         */
        COST_PRICE(7),
        /**
         * For the other kind of stats value, the round precision will be 2.
         */
        STATS(2);

        private final int precision;

        PrecisionEnum(final int precision) {
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }
    }
}
