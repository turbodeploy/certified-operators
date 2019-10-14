package com.vmturbo.components.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * A utility for running an OS command in a separate process, and log the output from the child process.
 * The {@code stdout} and the {@code stderr} streams from the child process are combined and logged via log4j. Output
 * lines are tagged with the command being run.
 */
@Component
public class OsCommandProcessRunner {

    @Autowired
    OsProcessFactory osProcessFactory;

    private Logger log = LogManager.getLogger();

    // sleep time, in ms, in the stdOut monitoring loop, so as not to lock up the processor.
    private static final int MONITOR_SLEEP_TIME_MS = 50; // polling delay 50ms

    // return code in case there is an IO error
    private static final int ERROR_RC = 100;

    /**
     * Launch a Process to run the given O/S command and wait until the process completes. The stdOut and stdError
     * streams from the Process are combined, and the output stream is dumped via log4j.
     *  @param osCommandPath OS command path
     * @param args arguments to the given OS command
     * @return the response code from the command - usually '0' == success
     */
    public int runOsCommandProcess(String osCommandPath, String... args) {
        Process scriptProcess = osProcessFactory.startOsCommand(osCommandPath, args);
        int rc;
        try (BufferedReader stdOutStream =
                 new BufferedReader(new InputStreamReader(scriptProcess.getInputStream()))) {
            String tag = new File(osCommandPath).getName();
            new Thread(() -> {
                try {
                    while (scriptProcess.isAlive()) {
                        log.info(tag + ":---" + stdOutStream.readLine());
                        Thread.sleep(MONITOR_SLEEP_TIME_MS);
                    }
                } catch (IOException e) {
                    log.error("Error tracing output from " + osCommandPath, e);
                } catch (InterruptedException e) {
                    log.error("stream monitor loop sleep interrupted");
                }
            }).start();
            // wait for the process to complete and then flush the output stream to the log
            rc = scriptProcess.waitFor();
            while (stdOutStream.ready()) {
                log.info(tag + ":---" + stdOutStream.readLine());
            }
        } catch (InterruptedException e) {
            // log the error and continue, instead of throwing exception
            log.error("Error running " + osCommandPath + " - Interrupted:   " + e.toString());
            rc = ERROR_RC;
        } catch (IOException e) {
            throw new RuntimeException("Stream error for " + osCommandPath, e);
        }
        return rc;
    }
}