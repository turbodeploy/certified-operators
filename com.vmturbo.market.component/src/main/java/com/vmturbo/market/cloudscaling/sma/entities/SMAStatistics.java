package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.List;

/**
 * This class collects statistics on Stable Marriage Algorithm (SMA).
 */
public class SMAStatistics {

    /*
     * Statistics on inputs
     */
    /*
     * Number of Virtual Machines, an input to SMA.
     */
    private int numberOfVMs = 0;
    /*
     * Number of Reserved Instances, an input to SMA.
     */
    private int numberOfRIs = 0;
    /*
     * Number of Normalized RIs after RIs have been combined by context.
     */
    private int numberOfNormalizedRIs = 0;
    /*
     * Number of templates, an input to SMA.
     */
    private int numberOfTemplates = 0;
    /*
     * Number of families as input to SMA
     */
    private int numberOfFamilies = 0;
    /*
     * context
     */
    private SMAContext context;
    /*
     * What is the type of RIs
     */
    private TypeOfRIs typeOfRIs;

    /*
     * Statistics on coverage
     */
    /*
     * total VM coupons
     */
    private int totalVmCurrentCoupons = 0;
    private int totalVmNaturalCoupons = 0;
    private int totalVmDesiredStateCoupons = 0;
    /*
     * total RI coupons
     */
    private double totalRiCoupons = 0;
    /*
     * total RI coupons used
     */
    private double totalDiscountedCoupons = 0;

    /*
     * Costs
     */
    /*
     * The cost for the current template allocation
     */
    private float currentCost = 0;
    /*
     * The cost for the current template allocation with RI coverage
     */
    private float currentCostWithRICoverage = 0;
    /*
     * The cost for the natural template allocation
     */
    private float naturalCost = 0;
    /*
     * The cost for the SMA matching
     */
    private float smaMatchingCost = 0;
    /*
     * computations on input
     */
    private float savings = 0;

    /*
     * Statistics on running the algorithm
     */
    /*
     * How many times are RIs processed?
     */
    private int numberOfIterations = 0;
    /*
     * How many times a new engagement?
     */
    private int numberOfNewEngagements = 0;
    /*
     * How many preference calls?
     */
    private int numberOfPreferenceCalls = 0;
    /*
     * How many times is an engagement swapped?
     */
    private int numberOfSwaps = 0;
    /*
     * time to run SMA in milliseconds
     */
    private long time = 0;
    /*
     * Sort time to run SMA in milliseconds
     */
    private long sortTime = 0;
    /*
     * time to iterate over RIs in milliseconds
     */
    private long iterationTime = 0;
    /*
     * time to post process the SMA results in milliseconds
     */
    private long postProcessTime = 0;

    /**
     * constructor.
     */
    public SMAStatistics() {
    }

    /*
     * Compute the on-demand cost for all the virtual machines using current template.
     * Not using streams, because floats not well supported with streams.
     */
    private float computeCurrentCost(List<SMAVirtualMachine> vms) {
        float savings = 0;
        for (SMAVirtualMachine vm : vms) {
            savings += vm.getCurrentTemplate().getOnDemandTotalCost(vm.getBusinessAccountId(), vm.getOsType());
        }
        return savings;
    }

    /*
     * Compute the on-demand cost for all the virtual machines using current template and
     * RI coverage.
     */
    private float computeCurrentCostWithRICoverage(List<SMAVirtualMachine> vms) {
        float savings = 0;
        for (SMAVirtualMachine vm : vms) {
            float currentRICoverageFraction = vm.getCurrentRICoverage() / (float)vm.getCurrentTemplate().getCoupons();
            savings += vm.getCurrentTemplate().getOnDemandTotalCost(vm.getBusinessAccountId(), vm.getOsType()) *
                (1.0 - currentRICoverageFraction);
        }
        return savings;
    }

    /*
     * Compute the on-demand cost for all the virtual machines using natural template.
     */
    private float computeNaturalCost(List<SMAVirtualMachine> vms) {
        float savings = 0;
        for (SMAVirtualMachine vm : vms) {
            savings += vm.getNaturalTemplate().getOnDemandTotalCost(vm.getBusinessAccountId(), vm.getOsType());
        }
        return savings;
    }

    /*
     * Given the list of matches, compute the cost.
     */
    private float computeStableMarriageCost(List<SMAMatch> matches) {
        float total = 0;
        for (SMAMatch match : matches) {
            SMATemplate template = match.getTemplate();
            SMAVirtualMachine vm = match.getVirtualMachine();
            float onDemandTotalCost = template.getOnDemandTotalCost(vm.getBusinessAccountId(), vm.getOsType());
            float discountedTotalCost = template.getDiscountedTotalCost(vm.getBusinessAccountId(), vm.getOsType());
            float discountPercent = match.getDiscountedCoupons() / (float)template.getCoupons();
            /*
             * This computation assumes that
             * on-demand cost is the sum of the template's onDemandCosts' compute and license costs, and
             * RI cost is the sum of the template's discountedCost's compute and license costs.
             */
            float cost = (onDemandTotalCost * (1.0f - discountPercent)) +
                    (discountedTotalCost * discountPercent);
            total += cost;
        }
        return total;
    }

    /**
     * Compute the savings from the Stable Marriage Algorithm.
     * @param vms the list of all virtual machines
     * @param matches the matches generated by the SMA
     */
    public void computeSavings(List<SMAVirtualMachine> vms, List<SMAMatch> matches) {
        savings = 0f;
        smaMatchingCost = computeStableMarriageCost(matches);
        currentCostWithRICoverage = computeCurrentCostWithRICoverage(vms);
        currentCost = computeCurrentCost(vms);
        naturalCost = computeNaturalCost(vms);
        savings = currentCost - smaMatchingCost;
    }

    // Setters and getters.

    public void setNumberOfTemplates(int value) {
        numberOfTemplates = value;
    }

    public void setNumberOfFamilies(int value) {
        numberOfFamilies = value;
    }

    public void setNumberOfVirtualMachines(int value) {
        numberOfVMs = value;
    }

    public void setNumberOfReservedInstances(int value) {
        numberOfRIs = value;
    }

    public void setNumberOfNormalizedReservedInstances(int value) {
        numberOfNormalizedRIs = value;
    }

    public void setContext(SMAContext context) {
        this.context = context;
    }

    /**
     * increment the number of iterations.
     */
    public void incrementIterations() {
        numberOfIterations++;
    }

    /**
     * increment the number of new engagements.
     */
    public void incrementNewEngagements() {
        numberOfNewEngagements++;
    }

    /**
     * increment the number of preference calls.
     */
    public void incrementPreferenceCalls() {
        numberOfPreferenceCalls++;
    }

    /**
     * increment the number of swaps.
     */
    public void incrementSwaps() {
        numberOfSwaps++;
    }


    // total RI coupons.
    public void setTotalRiCoupons(double value) {
        totalRiCoupons = value;
    }

    //total VM coupons.
    public void setTotalVmCurrentCoupons(int value) {
        totalVmCurrentCoupons = value;
    }

    public void setTotalVmNaturalCoupons(int value) {
        totalVmNaturalCoupons = value;
    }

    public void setTotalVmDesiredStateCoupons(int value) {
        totalVmDesiredStateCoupons = value;
    }

    public void setTime(long value) {
        time = value;
    }

    public void setSortTime(long value) {
        sortTime = value;
    }

    public void setIterationTime(long value) {
        iterationTime = value;
    }

    public void setPostProcessTime(long value) {
        postProcessTime = value;
    }

    /*
     * total RI coupons used
     */
    public void setTotalDiscountedCoupons(double value) {
        totalDiscountedCoupons = value;
    }


    public TypeOfRIs getIsZonalRIs() {
        return typeOfRIs;
    }

    public void setIsZonalRIs(TypeOfRIs value) {
        typeOfRIs = value;
    }


    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SMA Statistics: time=")
                .append(time)
                .append("ms VMs=").append(numberOfVMs)
                .append(" RIs=").append(numberOfRIs)
                .append(" normalizedRIs=").append(numberOfNormalizedRIs)
                .append(" templates=").append(numberOfTemplates)
                .append(" familes=").append(numberOfFamilies)
                .append(" Coupons VM current=").append(totalVmCurrentCoupons)
                .append(" natural=").append(totalVmNaturalCoupons)
                .append(" desiredState=").append(totalVmDesiredStateCoupons)
                .append(" RI coupons=").append(totalRiCoupons)
                .append(" discountedCoupons=").append(totalDiscountedCoupons)
                .append(" iterations=").append(numberOfIterations)
                .append(" new engagements=").append(numberOfNewEngagements)
                .append(" preferences calls=").append(numberOfPreferenceCalls)
                .append(" swaps=").append(numberOfSwaps)
                .append("; Costs:")
                .append(" natural=").append(naturalCost)
                .append(" current=").append(currentCost)
                .append(" currentWithRICoverage=").append(currentCostWithRICoverage)
                .append(" SMA=").append(smaMatchingCost)
                .append("; Savings=").append(savings)
                .append(", typeOfRIs=").append(typeOfRIs)
                .append(", OS=").append(context.getOs())
                .append("\n");
        // header in CSV form
        buffer.append("time(ms), VMs, RIs, normalizedRIs, templates, families, VM_coupons, natural")
                .append(", desiredState, RI_coupons, discounted_coupons, iterations, engagements")
                .append(", preferences, swaps, costs_natural, currrent, currentWithRIs, SMA, typeOfRIs, OS")
                .append("\n***");
        // values in CSV form
        buffer.append(time)
                .append(", ").append(numberOfVMs)
                .append(", ").append(numberOfRIs)
                .append(", ").append(numberOfNormalizedRIs)
                .append(", ").append(numberOfTemplates)
                .append(", ").append(numberOfFamilies)
                .append(", ").append(totalVmCurrentCoupons)
                .append(", ").append(totalVmNaturalCoupons)
                .append(", ").append(totalVmDesiredStateCoupons)
                .append(", ").append(totalRiCoupons)
                .append(", ").append(totalDiscountedCoupons)
                .append(", ").append(numberOfIterations)
                .append(", ").append(numberOfNewEngagements)
                .append(", ").append(numberOfPreferenceCalls)
                .append(", ").append(numberOfSwaps)
                .append(", ").append(naturalCost)
                .append(", ").append(currentCost)
                .append(", ").append(currentCostWithRICoverage)
                .append(", ").append(smaMatchingCost)
                .append(", ").append(typeOfRIs)
                .append(", ").append(context.getOs().name());

        return buffer.toString();
    }

    /**
     * Checks if the input RI's are all regional, all zonal or mixed.
     */
    public enum TypeOfRIs {
        /**
         * all ris are zonal.
         */
        ZONAL,
        /**
         * all ris are regional.
         */
        REGIONAL,
        /**
         * there are both regional and zonal ris.
         */
        MIXED;
    }
}
