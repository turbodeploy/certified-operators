package com.vmturbo.auth.api.auditing;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.MessageFormat;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.sender.TcpSyslogMessageSender;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.auth.api.authentication.ICredentials;
import com.vmturbo.auth.api.authentication.credentials.ADCredentials;
import com.vmturbo.auth.api.authentication.credentials.BasicCredentials;
import com.vmturbo.auth.api.authentication.credentials.CredentialsBuilder;

/**
 * Tests {@link AuditLogUtilsTest}
 */
public class AuditLogUtilsTest {


    public static final String TEST = "test";

    /**
     * Verify if the retries <= maxConnectRetryCount (2), it will retry again.
     */
    @Test
    public void testRetrySendingLogicWithIOException() throws Exception {
        final long maxConnectRetryCount = 2L;
        TcpSyslogMessageSender messageSender = Mockito.mock(TcpSyslogMessageSender.class);
        doThrow(new IOException()).when(messageSender).sendMessage(TEST);
        AuditLogUtils.sendToRsyslog("test", messageSender, maxConnectRetryCount);
        verify(messageSender, times((int) (maxConnectRetryCount +1))).sendMessage(anyString());
    }

    /**
     * Verify the positive path.
     */
    @Test
    public void testRetrySendingLogic() throws Exception {
        final long maxConnectRetryCount = 2L;
        TcpSyslogMessageSender messageSender = Mockito.mock(TcpSyslogMessageSender.class);
        doNothing().when(messageSender).sendMessage(TEST);
        AuditLogUtils.sendToRsyslog("test", messageSender, maxConnectRetryCount);
        verify(messageSender, times(1)).sendMessage(anyString());
    }
}
