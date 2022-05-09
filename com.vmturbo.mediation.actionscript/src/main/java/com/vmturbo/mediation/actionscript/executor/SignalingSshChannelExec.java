package com.vmturbo.mediation.actionscript.executor;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.common.SshConstants;
import org.apache.sshd.common.util.buffer.Buffer;

/**
 * This class implements an SSH channel that can execute a command and send signals to it
 *
 * <p>This is an extension of the existing ChannelExec class, which provides for command execution.
 * We add support for sending signals.</p>
 */
public class SignalingSshChannelExec extends ChannelExec {

    public SignalingSshChannelExec(final @Nonnull String command) {
        super(command, null, null);
    }

    /**
     * Send a signal to the executing command.
     *
     * @param signal Signal name, minus the "SIG" prefix (e.g. "TERM" for SIGTERM)
     * @throws IOException if we have trouble sending the packet
     */
    public void signal(final @Nonnull String signal) throws IOException {
        // see section 6.9 of SSH Connection Protocol at https://tools.ietf.org/html/rfc4254#section-6.9
        Buffer buffer = getSession().createBuffer(SshConstants.SSH_MSG_CHANNEL_REQUEST, 100);
        buffer.putInt(getRecipient());
        buffer.putString("signal");
        buffer.putBoolean(false);
        buffer.putString(signal);
        writePacket(buffer);
    }
}
