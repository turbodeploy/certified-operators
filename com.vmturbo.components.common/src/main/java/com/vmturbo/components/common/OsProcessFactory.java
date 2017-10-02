package com.vmturbo.components.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * Factory for creating Process for running diagnostics. Extracted to enable testing since ProcessBuilder is declared
 * as "final", and hence cannot be mocked.
 *
 * <p>The {@code stderr} output stream is combined with the {@code stdout} stream, so the caller need only monitor
 * a single output stream.
 **/
@Component
public class OsProcessFactory {

    /**
     * Create and start a Process to run the given command with the given arguments. Returns the newly created process.
     * The {@code stderr} output stream is combined with the {@code stdout} stream, so the caller need only monitor
     * a single output stream.
     *
     * @param osCommandString the command to run in a new Process
     * @param args            the arguments to pass to the command.
     * @return the newly started command Process
     */
    public Process startOsCommand(String osCommandString, String[] args) {
        Process osCommandProcess;
        try {
            List<String> argList = new ArrayList<>(args.length + 1);
            argList.add(osCommandString);
            argList.addAll(Arrays.asList(args));
            osCommandProcess = new ProcessBuilder(argList)
                    .redirectErrorStream(true)
                    .start();
        } catch (IOException e) {
            throw new RuntimeException("Error launching " + osCommandString, e);
        }
        return osCommandProcess;
    }
}
