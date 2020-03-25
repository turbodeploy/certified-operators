package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.ConnectionService;
import org.apache.sshd.server.SshServer;
import org.junit.Assert;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import com.vmturbo.mediation.actionscript.SshUtils.RemoteCommand;
import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.mediation.actionscript.executor.SignalingSshChannelExec;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;

public class ActionScriptTestBase extends Assert {

    private static final Logger logger = LogManager.getLogger(ActionScriptTestBase.class);

    private SshServer apacheSshd = null;
    private static SystemSshdLauncher systemSshd = null;

    protected int startApacheSshd(@Nonnull File fileSystemRoot) throws IOException, GeneralSecurityException {
        this.apacheSshd = new ApacheSshdBuilder()
            .setupSftp()
            .setupSftp()
            .chroot(fileSystemRoot.toPath())
            .build();
        return apacheSshd.getPort();
    }

    protected void stopApacheSshd() throws IOException {
        if (apacheSshd != null) {
            apacheSshd.stop();
        }
    }

    protected static int startSystemSshd(String authKey) throws IOException {
        systemSshd = new SystemSshdLauncher(authKey);
        systemSshd.launch();
        return systemSshd.getPort();
    }

    protected static void stopSystemSshd() {
        systemSshd.close();
    }
    /**
     * Copy the file structure rooted at "remoteFiles" in resources relative to this package, to
     * the TempFolder object.
     *
     * @throws IOException
     */
    protected static void copyResouresToFileTree(String resourceName, String clsOrPkgName, File root) throws IOException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String prefix = clsOrPkgName.replaceAll("[.]", "/") + "/"+resourceName;
        logger.info("Copying test files to {}", root);
        for (Resource resource : resolver.getResources("classpath*:" + prefix + "/**/*")) {
            if (resource.exists() && !resource.getFile().isDirectory()) {
                String path = resource.getFile().toString();
                // we have some tests that use script paths with funky characters in them, and those
                // funky characters don't work in resource names. So we %-encode them like in URLs
                // in the resource name, but here we create the files with funky chars intact
                String encodedPath = path.substring(path.indexOf(prefix) + prefix.length());
                String realPath = URLDecoder.decode(encodedPath, "UTF-8");
                File dest = new File(root, realPath);
                FileUtils.forceMkdir(dest.getParentFile());
                try (Reader in = new InputStreamReader(resource.getInputStream())) {
                    try (Writer out = new FileWriter(dest)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }

    protected static void setOwnerExecutable(File root, Pattern fileNamePattern) throws IOException {
        Files.walk(root.toPath())
            .map(p->p.toFile())
            .filter(f->f.isFile() && fileNamePattern.matcher(f.getName()).matches())
            .forEach(file -> {
                file.setExecutable(true, true);
            });
    }

    protected static void setAllExecutable(File root, Pattern fileNamePattern) throws IOException {
        Files.walk(root.toPath())
            .map(p->p.toFile())
            .filter(f->f.isFile() && fileNamePattern.matcher(f.getName()).matches())
            .forEach(file -> {
                file.setExecutable(true, false);
            });
    }

    /**
     * Check whether the remote sshd support channel signaling.
     *
     * <p>The test consists of the following sequence, once a channel executing "/bin/cat" has been opened:</p>
     * <ol>
     *     <li>Send a TERM signal via the channel. If this works, the remote process will exit.</li>
     *     <li>Close STDIN. If signaling didn't work, this will cause the remote process to exit.</li>
     * </ol>
     * <p>If the channel reports an exit signal of "TERM", then signaling works correctly.</p>
     *
     * @param accountValues
     * @return
     * @throws RemoteExecutionException
     * @throws KeyValidationException
     */
    protected static boolean isChannelSignalingSupported(ActionScriptProbeAccount accountValues) throws RemoteExecutionException, KeyValidationException {
        return SshUtils.runInSshSession(accountValues, null, new RemoteCommand<Boolean>() {
            @Override
            public Boolean execute(final ActionScriptProbeAccount accountValues, final ClientSession session,
                                   @Nullable final ActionExecutionDTO actionExecution) throws RemoteExecutionException {
                try (SignalingSshChannelExec channel = new SignalingSshChannelExec("/bin/cat");
                     PipedOutputStream stdinPipe = new PipedOutputStream();
                     PipedInputStream stdin = new PipedInputStream(stdinPipe)) {
                    session.getService(ConnectionService.class).registerChannel(channel);
                    channel.setIn(stdin);
                    channel.open();
                    Thread.sleep((1000));
                    channel.signal("TERM");
                    Thread.sleep((1000));
                    try {
                        // If signaling worked, we would expect the pipe to be closed here, so we'll catch
                        // and ignore that specific case below
                        stdinPipe.close();
                    } catch (IOException e) {
                        if (!e.getMessage().equals("Pipe closed")) {
                            throw e;
                        }
                    }
                    final Set<ClientChannelEvent> fired = channel.waitFor(Arrays.asList(ClientChannelEvent.CLOSED), TimeUnit.SECONDS.toMillis(30));
                    return Objects.equals(channel.getExitSignal(), "TERM");
                } catch (IOException | InterruptedException e) {
                    // assume not supported if anything goes wrong
                    return false;
                }
            }
        });
    }
}
