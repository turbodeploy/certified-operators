package com.vmturbo.mediation.actionscript;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.common.Factory;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.ClassLoadableResourceKeyPairProvider;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.KeySetPublickeyAuthenticator;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.springframework.util.SocketUtils;

import com.google.common.io.Resources;

public class ApacheSshdBuilder {
    private static final Logger logger = LogManager.getLogger(ApacheSshdBuilder.class);

    private final int testPort;
    private SshServer sshd;


    public ApacheSshdBuilder() throws IOException, GeneralSecurityException {
        this.sshd = SshServer.setUpDefaultServer();
        // allocate service port
        testPort = SocketUtils.findAvailableTcpPort();
        logger.info("TCP Test Port for SSH: {}", testPort);
        sshd.setPort(testPort);
        setupTestKeys();
    }

    private void setupTestKeys() throws IOException, GeneralSecurityException {
        // load the private key id_rsa from the src/test/resources/ssh folder
        sshd.setKeyPairProvider(new ClassLoadableResourceKeyPairProvider("ssh/id_rsa"));
        // fetch the private key info for this test from the resource
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"), Charset.defaultCharset());
        // extract a keypair from the key for this test
        final KeyPair keyPair = SshUtils.extractKeyPair(privateKeyString);
        // prepare the authenticator to recognize the public key of this pair
        sshd.setPublickeyAuthenticator(new KeySetPublickeyAuthenticator(Collections.singleton(keyPair.getPublic())));
    }

    public ApacheSshdBuilder setupSftp() {
        List<NamedFactory<Command>> factories = sshd.getSubsystemFactories();
        factories = factories != null ? factories : new ArrayList<>();
        factories.add(new SftpSubsystemFactory.Builder().build());
        sshd.setSubsystemFactories(factories);
        return this;
    }

    public ApacheSshdBuilder chroot(Path path) {
        sshd.setFileSystemFactory(new VirtualFileSystemFactory(path));
        return this;
    }

    public ApacheSshdBuilder setupShell() {
        sshd.setShellFactory(new TestShellFactory());
        return this;
    }

    public SshServer build() throws IOException {
        sshd.start();
        return sshd;
    }

    /**
     * A fake Shell factory, to be used instead of running real shell commands
     */
    private class TestShellFactory implements Factory<Command> {
        @Override
        public Command create() {
            return new Command() {

                @Override
                public void start(final Environment environment) throws IOException {

                }

                @Override
                public void destroy() throws Exception {

                }

                @Override
                public void setInputStream(final InputStream inputStream) {

                }

                @Override
                public void setOutputStream(final OutputStream outputStream) {

                }

                @Override
                public void setErrorStream(final OutputStream outputStream) {

                }

                @Override
                public void setExitCallback(final ExitCallback exitCallback) {

                }
            };
        }
    }
}
