package com.vmturbo.cost.component.savings;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EventInjector.ScriptEvent;

/**
 * Event injector tests.
 */
public class EventInjectorTest {
    private static final String SCRIPT_FILE = "/tmp/injected-events.json";

    /**
     * Test setup.
     */
    @Before
    public void setup() {
    }

    /**
     * Verify that a script file can be parsed.
     * @throws FileNotFoundException when the script file can't be found.
     */
    @Test
    public void parseEventFile() throws FileNotFoundException {
        Gson gson = new Gson();
        // Open the file
        JsonReader reader = new JsonReader(new FileReader(
                "src/test/resources/savings/demo-events.json"));
        List<ScriptEvent> events = Arrays.asList(gson.fromJson(reader, ScriptEvent[].class));
        Assert.assertEquals(6, events.size());
    }

    /**
     * Wrapper to create an empty file without needing to try/catch.  This uses an external
     * touch command to create the file to avoid issues related to failing to detect files
     * created by File.createNewFile().
     *
     * @param path complete path of file to create.
     * @return the File representing the file, which can be used to delete it later.
     */
    private File createFile(String path) {
        // Use external shell command.  Internally created files aren't reliably picked up by
        // the file watcher.
        try {
            Runtime.getRuntime().exec("touch " + path);
        } catch (IOException e) {
            return null;
        }
        return new File(path);
    }

    /**
     * Create the exists file.
     *
     * @return the File representing the file, which can be used to delete it later.
     */
    private File createExistsFile() {
        return createFile(SCRIPT_FILE + ".available");
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Helper class to describe expected events in the event journal.
     */
    private class ExpectedEvent {
        public long timestamp;
        public boolean actionEvent;
        public ActionEventType actionType;

        ExpectedEvent(long timestamp, ActionEventType actionType) {
            this.timestamp = timestamp;
            this.actionEvent = true;
            this.actionType = actionType;
        }

        ExpectedEvent(long timestamp) {
            this.timestamp = timestamp;
            this.actionEvent = false;
            this.actionType = null;
        }
    }

    List<ExpectedEvent> expectedEvents = ImmutableList.of(
        new ExpectedEvent(1612908000000L, ActionEventType.RECOMMENDATION_ADDED),
        new ExpectedEvent(1612908000000L, ActionEventType.EXECUTION_SUCCESS),
        new ExpectedEvent(1612940400000L, ActionEventType.RECOMMENDATION_ADDED),
        new ExpectedEvent(1612940400000L, ActionEventType.EXECUTION_SUCCESS),
        new ExpectedEvent(1612947600000L),
        new ExpectedEvent(1612972800000L),
        new ExpectedEvent(1612976400000L),
        new ExpectedEvent(1612978200000L));

    /**
     * Ensure the event tracker's event journal is populated.
     */
    @Test
    public void testJournalPopulation() {
        EntitySavingsTracker entitySavingsTracker = mock(EntitySavingsTracker.class);
        EntityEventsJournal entityEventsJournal = new InMemoryEntityEventsJournal();
        EventInjector injector = new EventInjector(entitySavingsTracker, entityEventsJournal);
        File testScript = new File("src/test/resources/savings/unit-test.json");
        File script = new File(SCRIPT_FILE);
        File available = new File(SCRIPT_FILE + ".available");
        // Remove stale test files
        script.delete();
        available.delete();
        try {
            // Move the script file into place
            Files.copy(testScript, script);
            injector.start();
            sleep(5000L);
            createExistsFile();
            sleep(5000L);
            List<SavingsEvent> events = entityEventsJournal.removeAllEvents();
            // Verify that the journal has 6 events
            Assert.assertEquals(expectedEvents.size(), events.size());
            // Verify that the events are ordered correctly
            Iterator<ExpectedEvent> it = expectedEvents.iterator();
            for (SavingsEvent event : events) {
                ExpectedEvent expected = it.next();
                Assert.assertEquals(event.getTimestamp(), expected.timestamp);
                if (expected.actionEvent) {
                    Assert.assertTrue(event.hasActionEvent());
                    Assert.assertEquals(event.getActionEvent().get().getEventType(), expected.actionType);
                } else {
                    Assert.assertFalse(event.hasActionEvent());
                }
            }
        } catch (IOException e) {
            Assert.assertTrue("Cannot copy test events to /tmp", false);
        } finally {
            // Clean up test files
            script.delete();
            available.delete();
        }
    }
}
