package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.ReservationGrouping;
import com.vmturbo.api.enums.ReservationMode;
import com.vmturbo.common.protobuf.plan.ReservationDTO;

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
    private static final ImmutableBiMap<ReservationDTO.ReservationMode, Integer> MODE_TO_DB
        = ImmutableBiMap.of(ReservationDTO.ReservationMode.NO_GROUPING, 1,
                            ReservationDTO.ReservationMode.AFFINITY, 2);

    /**
     * Grouping to Db.
     */
    private static final ImmutableBiMap<ReservationDTO.ReservationGrouping, Integer> GROUPING_TO_DB
        = ImmutableBiMap.of(ReservationDTO.ReservationGrouping.NONE, 1,
                            ReservationDTO.ReservationGrouping.CLUSTER, 2);

    /**
     * Mode to Api.
     */
    private static final ImmutableBiMap<ReservationDTO.ReservationMode, ReservationMode> MODE_TO_API
        = ImmutableBiMap.of(ReservationDTO.ReservationMode.NO_GROUPING, ReservationMode.NO_GROUPING,
                            ReservationDTO.ReservationMode.AFFINITY, ReservationMode.AFFINITY);

    /**
     * Grouping to Api.
     */
    private static final ImmutableBiMap<ReservationDTO.ReservationGrouping, ReservationGrouping> GROUPING_TO_API
        = ImmutableBiMap.of(ReservationDTO.ReservationGrouping.NONE, ReservationGrouping.NONE,
                            ReservationDTO.ReservationGrouping.CLUSTER, ReservationGrouping.CLUSTER);

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
    public static int modeToDb(@Nonnull final ReservationDTO.ReservationMode mode)
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
    public static ReservationDTO.ReservationMode modeFromDb(final int dbMode)
            throws NoSuchValueException {
        ReservationDTO.ReservationMode value = MODE_TO_DB.inverse().get(dbMode);
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
    public static ReservationMode modeToApi(@Nonnull final ReservationDTO.ReservationMode mode)
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
    public static ReservationDTO.ReservationMode modeFromApi(@Nonnull final ReservationMode apiMode)
            throws NoSuchValueException {
        ReservationDTO.ReservationMode value = MODE_TO_API.inverse().get(apiMode);
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
    public static int groupingToDb(@Nonnull final ReservationDTO.ReservationGrouping grouping)
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
    public static ReservationDTO.ReservationGrouping groupingFromDb(final int dbGrouping)
            throws NoSuchValueException {
        ReservationDTO.ReservationGrouping value = GROUPING_TO_DB.inverse().get(dbGrouping);
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
    public static ReservationGrouping groupingToApi(@Nonnull final ReservationDTO.ReservationGrouping grouping)
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
    public static ReservationDTO.ReservationGrouping groupingFromApi(@Nonnull ReservationGrouping apiGrouping)
            throws NoSuchValueException {
        ReservationDTO.ReservationGrouping value = GROUPING_TO_API.inverse().get(apiGrouping);
        if (value == null) {
            throw new NoSuchValueException("Unexpected grouping from api: " + apiGrouping);
        }
        return value;
    }
}
