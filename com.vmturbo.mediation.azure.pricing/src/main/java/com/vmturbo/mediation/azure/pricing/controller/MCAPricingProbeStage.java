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
     * Regroup the resolved meters by type and create a pricing workspace.
     */
    REGROUP_METERS("Group resolved meters by type"),

    /**
     * Process IP Meter Pricing.
     */
    IP_PRICE_PROCESSOR("IP Price Processing"),

    /**
     * Add onDemandLicenseOverrides.
     */
    LICENSE_OVERRIDES("Add on demand license overrides"),

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
            REGROUP_METERS,
            IP_PRICE_PROCESSOR,
            LICENSE_OVERRIDES,
            ASSIGN_IDENTIFIERS);
}
