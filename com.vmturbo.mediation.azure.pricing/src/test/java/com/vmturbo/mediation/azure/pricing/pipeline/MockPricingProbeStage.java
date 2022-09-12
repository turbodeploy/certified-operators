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
     * TODO remove this.
     */
    PLACEHOLDER_FINAL("Placeholder Final Stage");

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
            DOWNLOAD_PRICE_SHEET, SELECT_ZIP_ENTRIES, BOM_AWARE_READERS,
            PLACEHOLDER_FINAL);
}
