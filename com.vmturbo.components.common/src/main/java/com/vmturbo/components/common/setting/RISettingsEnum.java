package com.vmturbo.components.common.setting;

import java.util.Optional;

/**
 * Enum definitions for Reserved Instance(RI) Settings.
 */
public class RISettingsEnum {

    /**
     * Preferred Offering Class that can be used.
     */
    public enum PreferredOfferingClass { STANDARD, CONVERTIBLE }

    /**
     * Preferred Terms that can be used.
     */
    public enum PreferredTerm {
        //1 Year
        YEARS_1(1),
        //3 Years
        YEARS_3(3);

        //Integer value corresponding set years of PreferredTerm
        private final int years;

        /**
         * Instantiates PreferredTerm Enum of years values
         *
         * @param years year value of the PrefferedTerm
         */
        PreferredTerm(final int years) {
            this.years = years;
        }

        //Return years
        public int getYears() {
            return years;
        }

        /**
         * Returns Optional with enum const matching int value.
         *
         * @param years the int value of the enum const to be returned
         * @return Optional of PreferredTem Enum Const
         */
        public static Optional<PreferredTerm> getPrefferedTermEnum(final int years) {
            switch (years) {
                case 1:
                    return Optional.of(PreferredTerm.YEARS_1);
                case 3:
                    return Optional.of(PreferredTerm.YEARS_3);
                default:
                    return Optional.empty();
            }

        }
    }

    /**
     * Preferred Payment Options that can be used.
     */
    public enum PreferredPaymentOption { ALL_UPFRONT, PARTIAL_UPFRONT, NO_UPFRONT }

    /**
     * Demand type.
     */
    public enum DemandType {
        /**
         * Allocation demand type.
         */
        ALLOCATION,
        /**
         * Consumption demand type.
         */
        CONSUMPTION;
    }
}
