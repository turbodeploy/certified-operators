package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.FileUtils;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.sshd.common.Factory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.ClassLoadableResourceKeyPairProvider;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.KeySetPublickeyAuthenticator;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.SocketUtils;

import com.google.common.io.Resources;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * Class to test the path validation for the Action Script folder.
 * This folder is where the user will place the executable files (scripts)
 * which the ActionScript probe will invoke in the Executor.
 */
public class ActionScriptDiscoveryTest extends Assert {

    private static Logger logger = LogManager.getLogger(ActionScriptDiscoveryTest.class);

    private SshServer sshd;
    // must be > 8000 for non-privileged process to create it
    private int testPort;
    private ActionScriptProbeAccount accountValues;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException, GeneralSecurityException {
        setupSshd();
        setupRemoteFiles();
    }

    private ActionScriptProbeAccount createAccountValues(String manifestPath) throws IOException {
        return new ActionScriptProbeAccount("localhost",
            System.getProperty("user.name"),
            Resources.toString(Resources.getResource("ssh/id_rsa"), Charset.defaultCharset()),
            manifestPath,
            Integer.toString(testPort));
    }


    @After
    public void takeDown() throws IOException {
        if (sshd != null) {
            sshd.stop();
        }
    }

    @Test
    public void testCanReadValidYAMLManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/manifest.yaml")).validateManifestFile();
        assertEquals(0, result.getErrorDTOCount());
    }

    @Test
    public void testCanReadValidJSONManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/manifest.json")).validateManifestFile();
        assertEquals(0, result.getErrorDTOCount());
    }

    @Test
    public void testCantReadNonexistentManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/bogus/manifest.yaml")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    @Test
    public void testCantReadMisnamedManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/bogus/manifest.txt")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    @Test
    public void testCantReadMalformedYAMLManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/bad-manifest.yaml")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    @Test
    public void testCantReadMalformedJSONManifest() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/bad-manifest.json")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    @Test
    public void testCanDiscoverActions() throws IOException {
        ActionScriptProbeAccount accountValues = createAccountValues("/scripts/manifest.yaml");
        final ActionScriptDiscovery discovery = new ActionScriptDiscovery(accountValues);
        discovery.validateManifestFile();
        final DiscoveryResponse results = discovery.discoverActionScripts();
        assertEquals(3, results.getWorkflowCount());
        checkScriptValues(results, 0, "valid-in-default-dir", "/scripts/val-default.sh", "valid script in default directory", EntityType.PHYSICAL_MACHINE, ActionType.MOVE, ActionScriptPhase.REPLACE, "foo", "bar");
        checkScriptValues(results, 1, "valid-in-other-dir", "/otherscripts/val-other.sh", null, null, null, null);
        checkScriptValues(results, 2, "valid-in-rel-dir", "/otherscripts/val-rel.sh", null, null, null, null);
        assertEquals(4, results.getErrorDTOCount());
        checkErrorValues(results, 0, accountValues.getNameOrAddress(), "missing-script", "/no/scripts/here/script.sh");
        checkErrorValues(results, 1, accountValues.getNameOrAddress(), null, "/scripts/no-name.sh");
        checkErrorValues(results, 2, accountValues.getNameOrAddress(), "no-path", null);
        checkErrorValues(results, 3, accountValues.getNameOrAddress(), "non-executable", "/scripts/non-exec.sh");
    }

    private void checkScriptValues(DiscoveryResponse results, int index, String name, String path, String description, EntityType entityType, ActionType actionType, ActionScriptPhase actionPhase, String... tags) {
        assertTrue("No valid script at position " + index + " in discovery response", index < results.getWorkflowCount());
        final Workflow workflow = results.getWorkflow(index);
        assertEquals(name, workflow.hasDisplayName() ? workflow.getDisplayName() : null);
        assertEquals(path, workflow.hasScriptPath() ? workflow.getScriptPath() : null);
        assertEquals(description, workflow.hasDescription() ? workflow.getDescription() : null);
        assertEquals(entityType, workflow.hasEntityType() ? workflow.getEntityType() : null);
        assertEquals(actionType, workflow.hasActionType() ? workflow.getActionType() : null);
        assertEquals(actionPhase, workflow.hasPhase() ? workflow.getPhase() : null);
    }

    private void checkErrorValues(DiscoveryResponse results, int index, String host, String name, String path) {
        assertTrue("No invalid script at position " + index + " in discovery response", index < results.getErrorDTOCount());
        String expected = ActionScriptDiscovery.generateWorkflowID(host, name, path);
        assertEquals(expected, results.getErrorDTO(index).getEntityUuid());
    }

    private void setupSshd() throws IOException, GeneralSecurityException {
        testPort = SocketUtils.findAvailableTcpPort();
        logger.info("TCP Test Port for SSH: {}", testPort);

        sshd = SshServer.setUpDefaultServer();
        sshd.setPort(testPort);

        // load the private key id_rsa from the src/test/resources/ssh folder
        final ClassLoadableResourceKeyPairProvider keyPairProvider
            = new ClassLoadableResourceKeyPairProvider("ssh/id_rsa");
        sshd.setKeyPairProvider(keyPairProvider);

        // fetch the private key info for this test from the resource
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());

        // extract a keypair from the key for this test
        final KeyPair keyPair = SshUtils.extractKeyPair(privateKeyString);

        // prepare the authenticator to recognize the public key of this pair
        sshd.setPublickeyAuthenticator(new KeySetPublickeyAuthenticator(
            Collections.singleton(keyPair.getPublic())));

        // create a fake Shell factory instead of running real shell commands
        sshd.setShellFactory(new TestShellFactory());

        // chroot sshd to our temp folder
        sshd.setFileSystemFactory(new VirtualFileSystemFactory(tempFolder.getRoot().toPath()));

        sshd.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
        sshd.start();
    }

    /**
     * Copy the file structure rooted at "remoteFiles" in resources relative to this package, to
     * the TempFolder object.
     *
     * @throws IOException
     */
    private void setupRemoteFiles() throws IOException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String prefix = ActionScriptDiscoveryTest.class.getPackage().getName().replaceAll("[.]", "/") + "/remoteFiles";
        File root = tempFolder.getRoot();
        for (Resource resource : resolver.getResources("classpath*:" + prefix + "/**/*")) {
            if (resource.exists() && !resource.getFile().isDirectory()) {
                String path = resource.getFile().toString();
                String relPath = path.substring(path.indexOf(prefix) + prefix.length());
                File dest = new File(root, relPath);
                FileUtils.makeParentDirs(dest);
                try (Reader in = new InputStreamReader(resource.getInputStream())) {
                    try (Writer out = new FileWriter(dest)) {
                        IOUtils.copy(in, out);
                    }
                }
                if (dest.getName().startsWith("val-")) {
                    final Set<PosixFilePermission> perms = Files.getPosixFilePermissions(dest.toPath());
                    perms.removeAll(Arrays.asList(PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE));
                    if (dest.getName().startsWith("val-owner-")) {
                        perms.add(PosixFilePermission.OWNER_EXECUTE);
                    } else {
                        perms.add(PosixFilePermission.OTHERS_EXECUTE);
                    }
                    Files.setPosixFilePermissions(dest.toPath(), perms);
                }
            }
        }
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
