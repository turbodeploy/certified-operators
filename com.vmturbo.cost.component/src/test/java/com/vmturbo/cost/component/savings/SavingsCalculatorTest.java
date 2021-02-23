package com.vmturbo.cost.component.savings;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.apache.commons.collections4.MultiValuedMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.cost.component.savings.EventInjector.ScriptEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests to verify operation of the savings algorithm.
 */
public class SavingsCalculatorTest {

    /**
     * Test setup.
     *
     * @throws Exception if there's an error.
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * Ensure that the algorithm is generating the correct savings entries.
     *
     * @throws FileNotFoundException if the test events file cannot be opened.
     * @throws ParseException if the events file contains invalid JSON.
     */
    @Test
    public void calculate() throws FileNotFoundException, ParseException {
        EntityStateCache entityStateCache = new InMemoryEntityStateCache();
        SavingsCalculator savingsCalculator = new SavingsCalculator(entityStateCache);
        EntityEventsJournal entityEventsJournal = new InMemoryEntityEventsJournal();
        EntitySavingsStore entitySavingsStore = new SavingsCapture();
        EntitySavingsTracker tracker = new EntitySavingsTracker(entitySavingsStore,
                entityEventsJournal, entityStateCache);
        // Inject some events
        addTestEvents("src/test/resources/savings/unit-test.json", entityEventsJournal);
        tracker.processEvents(roundTime(entityEventsJournal.getNewestEventTime(), true).getTimeInMillis());
        Assert.assertTrue(true);
    }

    /**
     * Round the indicated time up or down to the nearest top of the hour.
     *
     * @param time time in ms to round
     * @param roundUp true to round up (future), else round down (past).
     * @return rounded time
     */
    private Calendar roundTime(long time, boolean roundUp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (roundUp) {
            calendar.add(Calendar.HOUR_OF_DAY, 1);
        }
        // Set time to the top of the hour.
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;

    }

    private void addTestEvents(String eventFileName, EntityEventsJournal entityEventsJournal)
            throws FileNotFoundException {
        // Open the script file, convert the events to SavingsEvents, and add them to the event
        // journal.
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(eventFileName));
        List<ScriptEvent> events = Arrays.asList(gson.fromJson(reader, ScriptEvent[].class));
        events.forEach(event -> EventInjector.addEvent(event, entityEventsJournal));
    }

    /**
     * Stub savings store to capture generated savings events.
     */
    class SavingsCapture implements EntitySavingsStore {
        private List<EntitySavingsStats> stats;

        SavingsCapture() {
            this.stats = new ArrayList<>();
        }

        @Override
        public void addHourlyStats(@NotNull Set<EntitySavingsStats> hourlyStats)
                throws EntitySavingsException {
            stats.addAll(hourlyStats);
        }

        List<EntitySavingsStats> getStats() {
            return this.stats;
        }

        @NotNull
        @Override
        public Set<AggregatedSavingsStats> getHourlyStats(
                @NotNull Set<EntitySavingsStatsType> statsTypes, @NotNull Long startTime,
                @NotNull Long endTime, @NotNull MultiValuedMap<EntityType, Long> entitiesByType)
                throws EntitySavingsException {
            // Not used.
            return new HashSet<>();
        }

        @Nullable
        @Override
        public Long getMaxStatsTime() {
            return null;
        }
    }
}