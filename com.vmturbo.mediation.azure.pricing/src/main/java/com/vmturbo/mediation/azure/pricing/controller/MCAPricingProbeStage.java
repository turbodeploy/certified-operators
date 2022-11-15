package com.vmturbo.mediation.azure.pricing.controller;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Enumeration of all stages of operation for MCA pricing discovery.
 */
public enum MCAPricingProbeStage implements ProbeStageEnum {
    /**
     * Download the price sheet for the account.
     */
    DOWNLOAD_PRICE_SHEET("Download price sheet"),

    /**
     * Select the entries of interest in z zip file.
     */
    SELECT_ZIP_ENTRIES("Select entries in zip file"),

    /**
     * Open the entries in a zip file.
     */
    OPEN_ZIP_ENTRIES("Open zip file entries"),

    /**
     * Create Readers from InputStreams.
     */
    BOM_AWARE_READERS("Byte Order Mark-aware file reading"),

    /**
     * Single stream of CSV Records from stream of Readers.
     */
    CHAINED_CSV_PARSERS("Initialize CSV Parsing"),

    /**
     * Deserialize CSV records into meter records.
     */
    DESERIALIZE_METERS("Load meters from CSV records"),

    /**
     * filter meters based on effective start/end dates.
     */
    EFFECTIVE_DATE_FILTER("Effective date filter stage"),

    /**
     * Resolve meters, identifying their meaning.
     */
    RESOLVE_METERS("Resolve meters"),

    /**
     * Apply fallback pricing.
     */
    PLAN_FALLBACK("Apply plan-to-plan fallback pricing"),

    /**
     * Regroup the resolved meters by type and create a pricing workspace.
     */
    REGROUP_METERS("Group resolved meters by type"),

    /**
     * Process IP Meter Pricing.
     */
    IP_PRICE_PROCESSOR("IP Price Processing"),

    /**
     * Process VM Pricing.
     */
    INSTANCE_TYPE_PROCESSOR("VM Instance Type Price Processing"),

    /**
     * Add onDemandLicenseOverrides.
     */
    LICENSE_OVERRIDES("Add on demand license overrides"),

    /**
     * Process Licence Pricing.
     */
    LICENSE_PRICE_PROCESSOR("License Price Processing"),

    /**
     * Process Storage Tiers with Fixed Sizes.
     */
    FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR("Fixed Size Storage Tier Price Processing"),

    /**
     * Process Storage Tiers with per-GB pricing.
     */
    LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR("Linear Size Storage Tier Price Processing"),

    /**
     * Process Ultra Disk Storage Tier.
     */
    ULTRA_DISK_STORAGE_TIER("Ultra Disk Storage Tier Processing"),

    /**
     * Assign pricing identifiers to plans and return a DiscoveredPricing result.
     */
    ASSIGN_IDENTIFIERS("Assign pricing identifiers");

    private final String description;

    MCAPricingProbeStage(String description) {
        this.description = description;
    }

    @Nonnull
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * The list of stages performed as a part of validation.
     */
    public static final List<MCAPricingProbeStage> VALIDATION_STAGES = ImmutableList.of(
            // TODO
    );

    /**
     * The list of stages performed as a part of discovery.
     */
    public static final List<MCAPricingProbeStage> DISCOVERY_STAGES = ImmutableList.of(
            DOWNLOAD_PRICE_SHEET,
            SELECT_ZIP_ENTRIES,
            OPEN_ZIP_ENTRIES,
            BOM_AWARE_READERS,
            CHAINED_CSV_PARSERS,
            DESERIALIZE_METERS,
            EFFECTIVE_DATE_FILTER,
            RESOLVE_METERS,
            PLAN_FALLBACK,
            REGROUP_METERS,
            IP_PRICE_PROCESSOR,
            INSTANCE_TYPE_PROCESSOR,
            LICENSE_OVERRIDES,
            LICENSE_PRICE_PROCESSOR,
            FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR,
            LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR,
            ULTRA_DISK_STORAGE_TIER,
            ASSIGN_IDENTIFIERS);
}
