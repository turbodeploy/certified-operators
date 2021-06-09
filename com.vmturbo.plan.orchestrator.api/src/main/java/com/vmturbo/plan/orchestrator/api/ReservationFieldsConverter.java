package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.ReservationGrouping;
import com.vmturbo.api.enums.ReservationMode;
import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * A converter between db status, mode, grouping ints and {@link ReservationDTO.ReservationStatus},
 * {@link ReservationDTO.ReservationMode}, {@link ReservationDTO.ReservationGrouping}.
 */
public final class ReservationFieldsConverter {

    /**
     * Status to Db.
     */
    private static final ImmutableBiMap<ReservationDTO.ReservationStatus, Integer> STATUS_TO_DB
        = ImmutableBiMap.<ReservationDTO.ReservationStatus, Integer>builder()
                            .put(ReservationDTO.ReservationStatus.FUTURE, 1)
                            .put(ReservationDTO.ReservationStatus.RESERVED, 2)
                            .put(ReservationDTO.ReservationStatus.INVALID, 3)
                            .put(ReservationDTO.ReservationStatus.UNFULFILLED, 4)
                            .put(ReservationDTO.ReservationStatus.INPROGRESS, 5)
                            .put(ReservationDTO.ReservationStatus.PLACEMENT_FAILED, 6)
                            .put(ReservationDTO.ReservationStatus.INITIAL, 7)
                            .build();

    /**
     * Mode to Db.
     */
    private static final ImmutableBiMap<TopologyDTO.ReservationMode, Integer> MODE_TO_DB
        = ImmutableBiMap.of(TopologyDTO.ReservationMode.NO_GROUPING, 1,
                            TopologyDTO.ReservationMode.AFFINITY, 2);

    /**
     * Grouping to Db.
     */
    private static final ImmutableBiMap<TopologyDTO.ReservationGrouping, Integer> GROUPING_TO_DB
        = ImmutableBiMap.of(TopologyDTO.ReservationGrouping.NONE, 1,
                            TopologyDTO.ReservationGrouping.CLUSTER, 2);

    /**
     * Mode to Api.
     */
    private static final ImmutableBiMap<TopologyDTO.ReservationMode, ReservationMode> MODE_TO_API
        = ImmutableBiMap.of(TopologyDTO.ReservationMode.NO_GROUPING, ReservationMode.NO_GROUPING,
                            TopologyDTO.ReservationMode.AFFINITY, ReservationMode.AFFINITY);

    /**
     * Grouping to Api.
     */
    private static final ImmutableBiMap<TopologyDTO.ReservationGrouping, ReservationGrouping> GROUPING_TO_API
        = ImmutableBiMap.of(TopologyDTO.ReservationGrouping.NONE, ReservationGrouping.NONE,
                            TopologyDTO.ReservationGrouping.CLUSTER, ReservationGrouping.CLUSTER);

    private ReservationFieldsConverter() {}

    /**
     * Add the status to database.
     *
     * @param status reservation status.
     * @return status value.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public static int statusToDb(@Nonnull final ReservationDTO.ReservationStatus status)
            throws NoSuchValueException {
        Integer value = STATUS_TO_DB.get(status);
        if (value == null) {
            throw new NoSuchValueException("Unexpected status to db: " + status);
        }
        return value;
    }

    /**
     * Get the status from the database.
     *
     * @param dbStatus value from db.
     * @return ReservationDTO.ReservationStatus.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static ReservationDTO.ReservationStatus statusFromDb(final int dbStatus)
            throws NoSuchValueException {
        ReservationDTO.ReservationStatus value = STATUS_TO_DB.inverse().get(dbStatus);
        if (value == null) {
            throw new NoSuchValueException("Unexpected status from db: " + dbStatus);
        }
        return value;
    }

    /**
     * Add the mode to database.
     *
     * @param mode reservation mode.
     * @return mode value.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public static int modeToDb(@Nonnull final TopologyDTO.ReservationMode mode)
            throws NoSuchValueException {
        Integer value = MODE_TO_DB.get(mode);
        if (value == null) {
            throw new NoSuchValueException("Unexpected mode to db: " + mode);
        }
        return value;
    }

    /**
     * Get the mode from the database.
     *
     * @param dbMode value from db.
     * @return ReservationDTO.ReservationMode.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static TopologyDTO.ReservationMode modeFromDb(final int dbMode)
            throws NoSuchValueException {
        TopologyDTO.ReservationMode value = MODE_TO_DB.inverse().get(dbMode);
        if (value == null) {
            throw new NoSuchValueException("Unexpected mode from db: " + dbMode);
        }
        return value;
    }

    /**
     * Return the mode to the api.
     *
     * @param mode reservation mode.
     * @return mode value.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static ReservationMode modeToApi(@Nonnull final TopologyDTO.ReservationMode mode)
            throws NoSuchValueException {
        ReservationMode value = MODE_TO_API.get(mode);
        if (value == null) {
            throw new NoSuchValueException("Unexpected mode to api: " + mode);
        }
        return value;
    }

    /**
     * Get the mode from the api.
     *
     * @param apiMode value from api.
     * @return ReservationDTO.ReservationMode.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static TopologyDTO.ReservationMode modeFromApi(@Nonnull final ReservationMode apiMode)
            throws NoSuchValueException {
        TopologyDTO.ReservationMode value = MODE_TO_API.inverse().get(apiMode);
        if (value == null) {
            throw new NoSuchValueException("Unexpected mode from api: " + apiMode);
        }
        return value;
    }

    /**
     * Add the grouping to database.
     *
     * @param grouping reservation grouping.
     * @return grouping value.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public static int groupingToDb(@Nonnull final TopologyDTO.ReservationGrouping grouping)
            throws NoSuchValueException {
        Integer value = GROUPING_TO_DB.get(grouping);
        if (value == null) {
            throw new NoSuchValueException("Unexpected grouping to db: " + grouping);
        }
        return value;
    }

    /**
     * Get the grouping from the database.
     *
     * @param dbGrouping value from db.
     * @return ReservationDTO.ReservationGrouping.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static TopologyDTO.ReservationGrouping groupingFromDb(final int dbGrouping)
            throws NoSuchValueException {
        TopologyDTO.ReservationGrouping value = GROUPING_TO_DB.inverse().get(dbGrouping);
        if (value == null) {
            throw new NoSuchValueException("Unexpected grouping from db: " + dbGrouping);
        }
        return value;
    }

    /**
     * Return the grouping to the api.
     *
     * @param grouping reservation grouping.
     * @return grouping value.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static ReservationGrouping groupingToApi(@Nonnull final TopologyDTO.ReservationGrouping grouping)
            throws NoSuchValueException {
        ReservationGrouping value = GROUPING_TO_API.get(grouping);
        if (value == null) {
            throw new NoSuchValueException("Unexpected grouping to api: " + grouping);
        }
        return value;
    }

    /**
     * Get the grouping from the api.
     *
     * @param apiGrouping value from api.
     * @return ReservationDTO.ReservationGrouping.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    @Nonnull
    public static TopologyDTO.ReservationGrouping groupingFromApi(@Nonnull ReservationGrouping apiGrouping)
            throws NoSuchValueException {
        TopologyDTO.ReservationGrouping value = GROUPING_TO_API.inverse().get(apiGrouping);
        if (value == null) {
            throw new NoSuchValueException("Unexpected grouping from api: " + apiGrouping);
        }
        return value;
    }
}
