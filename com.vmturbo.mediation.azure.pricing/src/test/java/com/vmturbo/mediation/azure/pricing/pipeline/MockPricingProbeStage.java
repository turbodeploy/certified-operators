package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Enumeration of all stages of operation for MCA pricing discovery.
 */
public enum MockPricingProbeStage implements ProbeStageEnum {
    /**
     * Download the price sheet for the account.
     */
    DOWNLOAD_PRICE_SHEET("Download price sheet"),

    /**
     * Select entries from a zip file.
     */
    SELECT_ZIP_ENTRIES("Select entries from Zip file"),

    /**
     * Open entries from a zip file.
     */
    OPEN_ZIP_ENTRIES("Open entries of Zip file"),

    /**
     * Create Readers from InputStreams.
     */
    BOM_AWARE_READERS("Byte Order Mark aware file reading"),

    /**
     * Single stream of CSV Records from stream of Readers.
     */
    CHAINED_CSV_PARSERS("Initialize CSV Parsing"),

    /**
     * Single stream of CSV Records from stream of Readers.
     */
    DESERIALIZE_CSV("Deserialzie meters from CSV"),

    /**
     * A stage that introduces some kind of brokenness to the next stage.
     */
    BROKEN_INPUT_STAGE("A stage that introduces some kind of brokenness to the next stage"),

    /**
     * Filter meters based on effective start/end dates.
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
     * Regroup meters by MeterType.
     */
    REGROUP_METERS("Group meters by type"),

    /**
     * IP Meter Processing Stage.
     */
    IP_PRICE_PROCESSOR("IP Price Processing"),

    /**
     * Process VM Pricing.
     */
    INSTANCE_TYPE_PROCESSOR("VM Instance Type Price Processing"),

    /**
     * IP Meter Processing Stage.
     */
    LICENSE_OVERRIDES("Add On Demand License Override"),

    /**
     * License Price Processing Stage.
     */
    LICENSE_PRICE_PROCESSING("License Price Processing"),

    /**
     * Fake Storage Tier Pricing Stage.
     */
    FAKE_STORAGE_TIER_PROCESSING("Fake Storage Tier Price Processing"),

    /**
     * Fixed Size Storage Tier Pricing Stage.
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

    MockPricingProbeStage(String description) {
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
    public static final List<MockPricingProbeStage> VALIDATION_STAGES = ImmutableList.of(
    );

    /**
     * The list of stages performed as a part of discovery.
     */
    public static final List<MockPricingProbeStage> DISCOVERY_STAGES = ImmutableList.of(
        DOWNLOAD_PRICE_SHEET,
        SELECT_ZIP_ENTRIES,
        BOM_AWARE_READERS,
        DESERIALIZE_CSV,
        EFFECTIVE_DATE_FILTER,
        RESOLVE_METERS,
        PLAN_FALLBACK,
        REGROUP_METERS,
        IP_PRICE_PROCESSOR,
        INSTANCE_TYPE_PROCESSOR,
        LICENSE_OVERRIDES,
        LICENSE_PRICE_PROCESSING,
        FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR,
        LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR,
        ULTRA_DISK_STORAGE_TIER,
        ASSIGN_IDENTIFIERS);
}
