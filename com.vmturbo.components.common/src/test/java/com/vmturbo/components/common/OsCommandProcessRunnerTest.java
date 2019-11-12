package com.vmturbo.components.common;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * OsCommandProcessRunner launches a
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:command-process-runner-test.xml")
public class OsCommandProcessRunnerTest {

    @Autowired
    OsCommandProcessRunner commandProcessRunner;

    @Autowired
    OsProcessFactory mockProcessFactory;

    Logger log = LogManager.getLogger();

    private static long TEST_DURATION_MS=2000; // 2 seconds

    @Test
    public void testRunOsCommandProcess() throws Exception {
        // Arrange
        String osCommandPath = "/tmp/os-command-path-test";
        String[] args = {"arg1", "arg2"};
        Process mockProcess = Mockito.mock(Process.class);
        String stdOutStrings = "testing\n1,2,3\ndone\n";
        String stdErrStrings = "e1\ne2\ne3";
        InputStream mockStdOutStream = new ByteArrayInputStream(stdOutStrings.getBytes());
        InputStream mockStdErrStream = new ByteArrayInputStream(stdErrStrings.getBytes());
        long startTime = new Date().getTime();
        when(mockProcess.isAlive()).thenAnswer(invocationOnMock -> {
            long delta = new Date().getTime() - startTime;
            log.debug("isAlive:" + delta);
            return delta < TEST_DURATION_MS;
        });
        when(mockProcess.getInputStream()).thenReturn(mockStdOutStream);
        when(mockProcess.getErrorStream()).thenReturn(mockStdErrStream);
        when(mockProcess.waitFor()).thenAnswer(invocationOnMock -> {
            log.debug("------waiting for process");
            Thread.sleep(TEST_DURATION_MS);
            return 0;
        });
        when(mockProcessFactory.startOsCommand(osCommandPath, args)).thenReturn(mockProcess);

        // Act
        commandProcessRunner.runOsCommandProcess(osCommandPath, args);

        // Assert
        assertThat(mockStdOutStream.available(), is(0));
    }
}