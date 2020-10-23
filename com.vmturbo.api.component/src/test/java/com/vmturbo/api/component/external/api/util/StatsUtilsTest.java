package com.vmturbo.api.component.external.api.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class StatsUtilsTest {

    private static final long ONE_DAY_IN_MILLIS = 1000*60*60*24;

    /**
     * Test if the scope is valid for the RI query.
     */
    @Test
    public void testIsValidScopeForRIBoughtQuery() {
        // Business Account
        ApiId apiId = mockApiId(ImmutableSet.of(ApiEntityType.BUSINESS_ACCOUNT), false);
        boolean isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Zone
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.AVAILABILITY_ZONE), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Region
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.REGION), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Provider
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.SERVICE_PROVIDER), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Plan
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE), true);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Not Valid
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertFalse(isValid);
    }

    @NotNull
    private ApiId mockApiId(@NotNull final Set<ApiEntityType> entityTypes, boolean isCloudPlan) {
        Optional<Set<ApiEntityType>> types = Optional.of(entityTypes);
        ApiId apiId = Mockito.mock(ApiId.class);
        Mockito.when(apiId.isCloudPlan()).thenReturn(isCloudPlan);
        Mockito.when(apiId.getScopeTypes()).thenReturn(types);

        return apiId;
    }

    /**
     * Test that if only the start date is provided and it is in the past, then the end date is set
     * to current time.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateInThePast() {
        long currentTime = new Date().getTime();
        long startTime = currentTime - ONE_DAY_IN_MILLIS;      // set start date to 1 day ago
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(startTime), null);

        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getEndDate());
        assertEquals(currentTime, Long.parseLong(inputDto.getPeriod().getEndDate()));
    }

    /**
     * Test that if only the start date is provided and it is set to now, then both are set
     * to null.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateNow() {
        long currentTime = new Date().getTime();
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(currentTime), null);

        // Assert that both dates are null
        assertNull(inputDto.getPeriod().getEndDate());
        assertNull(inputDto.getPeriod().getStartDate());
    }

    /**
     * Test that if only the start date is provided and it is in the future, the end date is set to
     * start date.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateInTheFuture() {
        long currentTime = new Date().getTime();
        // set start date to 1 day in the future
        long startTime = currentTime + ONE_DAY_IN_MILLIS;
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(startTime), null);

        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getEndDate());
        assertEquals(startTime, Long.parseLong(inputDto.getPeriod().getEndDate()));
    }

    /**
     * Test that if only the end date is provided and it is in the past, then an exception is thrown
     * since it is not a valid input.
     *
     * @throws IllegalArgumentException as expected.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateInThePast() {
        long currentTime = new Date().getTime();
        long endDate = currentTime - ONE_DAY_IN_MILLIS;      // set end date to 1 day ago
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(endDate));
    }

    /**
     * Test that if only the end date is provided and it is set to now, then both are set
     * to null.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateNow() {
        long currentTime = new Date().getTime();
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(currentTime));

        // Assert that both dates are null
        assertNull(inputDto.getPeriod().getEndDate());
        assertNull(inputDto.getPeriod().getStartDate());
    }

    /**
     * Test that if only the end date is provided and it is in the future, then the start date is
     * set to now.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateInTheFuture() {
        long currentTime = new Date().getTime();
        // set end date to 1 day in the future
        long endTime = currentTime + ONE_DAY_IN_MILLIS;
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(endTime));
        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getStartDate());
        assertEquals(currentTime, Long.parseLong(inputDto.getPeriod().getStartDate()));
    }

    /**
     * Creates a StatScopesApiInputDTO with the times provided, executes processRequest() verifying
     * how many times runHistoricalStatsRequest() and runProjectedStatsRequest() were invoked, and
     * returns the dto in order to provide a way to the caller function to check if the dates
     * were altered correctly.
     *
     * @param currentTime a unix timestamp (milliseconds) to be considered 'now' for the execution.
     * @param startTime a String to initialize the dto's start date with. Can be null.
     * @param endTime a String to initialize the dto's end date with. Can be null.
     * @return the StatScopesApiInputDTO (with the dates possibly altered by processRequest()).
     */
    private StatScopesApiInputDTO getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
            long currentTime, String startTime, String endTime) {
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStartDate(startTime);
        period.setEndDate(endTime);
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName("a");
        List<StatApiInputDTO> statisticsRequested = Collections.singletonList(statApiInputDTO);
        period.setStatistics(statisticsRequested);
        inputDto.setPeriod(period);
        StatsUtils.sanitizeStartDateOrEndDate(inputDto.getPeriod(), currentTime, 200);
        return inputDto;
    }
}
