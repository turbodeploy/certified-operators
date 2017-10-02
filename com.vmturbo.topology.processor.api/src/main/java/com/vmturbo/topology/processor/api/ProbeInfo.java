package com.vmturbo.topology.processor.api;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * Represents probe information.
 */
public interface ProbeInfo {

    /**
     * Returns id of the probe.
     *
     * @return id of the probe
     */
    long getId();

    /**
     * Returns probe type.
     *
     * @return probe type
     */
    @Nonnull
    String getType();

    /**
     * Returns probe category.
     *
     * @return probe category
     */
    @Nonnull
    String getCategory();

    /**
     * Returns account definitions list for this probe.
     *
     * @return account definitions
     */
    @Nonnull
    List<AccountDefEntry> getAccountDefinitions();

    /**
     * List of fields, that can identify the target. All the field names, specified in the list must
     * be present in {@link #getAccountDefinitions()}. For example, if the probe is uniquely
     * identified by field named "nameOrAddress", this list should be filled with only one element -
     * "nameOrAddress", if probe is identified by "address" and "port", this list should be
     * {"address", "port"}
     *
     * @return list of identifying fields
     */
    List<String> getIdentifyingFields();
}
