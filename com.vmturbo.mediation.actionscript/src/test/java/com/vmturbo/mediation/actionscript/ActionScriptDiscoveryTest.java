package com.vmturbo.mediation.actionscript;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
public class ActionScriptDiscoveryTest extends ActionScriptTestBase {
    // certain production functionality (manifest validation) is not cross-platform
    private static boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");
    private int testPort;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException, GeneralSecurityException {
        this.testPort = startApacheSshd(tempFolder.getRoot());
        copyResouresToFileTree("remoteFiles", this.getClass().getName(), tempFolder.getRoot());
        setOwnerExecutable(tempFolder.getRoot(), Pattern.compile("execByOwner-.*"));
        setAllExecutable(tempFolder.getRoot(), Pattern.compile("execByAll-.*"));
    }

    @After
    public void teardown() throws IOException {
        stopApacheSshd();
    }

    private ActionScriptProbeAccount createAccountValues(String manifestPath) throws IOException {
        return new ActionScriptProbeAccount("localhost",
            System.getProperty("user.name"),
            Resources.toString(Resources.getResource("ssh/id_rsa"), Charset.defaultCharset()),
            manifestPath,
            Integer.toString(testPort), null);
    }

    @Test
    public void testCanReadValidYAMLManifest() throws IOException {
        Assume.assumeFalse(IS_WINDOWS);
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/manifest.yaml")).validateManifestFile();
        assertEquals(0, result.getErrorDTOCount());
    }

    @Test
    public void testCanReadValidJSONManifest() throws IOException {
        Assume.assumeFalse(IS_WINDOWS);
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
    public void testCantReadJsonManifestWithUnrecognizedProperties() throws IOException {
        Assume.assumeFalse(IS_WINDOWS);
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/unrec-props-manifest.json")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
        String msg = result.getErrorDTO(0).getDescription();
        assertTrue(msg.contains("Unrecognized field \"actionTypo\""));
    }

    @Test
    public void testCantReadYamlManifestWithUnrecognizedProperties() throws IOException {
        Assume.assumeFalse(IS_WINDOWS);
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/unrec-props-manifest.yaml")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
        String msg = result.getErrorDTO(0).getDescription();
        assertTrue(msg.contains("Unrecognized field \"actionTypo\""));
    }

    @Test
    public void testCantReadManifestWithRelativePath() throws IOException {
        final ValidationResponse result = new ActionScriptDiscovery(createAccountValues("scripts/unrec-props-manifest.yaml")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    @Test
    public void testCanDiscoverActions() throws IOException {
        Assume.assumeFalse(IS_WINDOWS);
        ActionScriptProbeAccount accountValues = createAccountValues("/scripts/manifest.yaml");
        final ActionScriptDiscovery discovery = new ActionScriptDiscovery(accountValues);
        discovery.validateManifestFile();
        final DiscoveryResponse results = discovery.discoverActionScripts();
        assertEquals(4, results.getWorkflowCount());
        checkScriptValues(results, 0, "valid-in-default-dir", "/scripts/execByAll-valid-in-default-dir.sh",
            "valid script in default directory", EntityType.PHYSICAL_MACHINE, ActionType.MOVE, ActionScriptPhase.REPLACE, 10000L);
        checkScriptValues(results, 1, "valid-in-other-dir", "/otherscripts/execByAll-valid-in-other-path.sh",
            null, null, null, null, null);
        checkScriptValues(results, 2, "valid-in-rel-dir", "/otherscripts/execByAll-valid-in-relative-path.sh",
            null, null, null, null, null);
        checkScriptValues(results, 3, "valid-owner-exec", "/scripts/execByOwner-valid-in-default-dir.sh",
            null, null, null, null, null);
        assertEquals(4, results.getErrorDTOCount());
        checkErrorValues(results, 0, accountValues.getNameOrAddress(), "missing-script", "/no/scripts/here/script.sh");
        checkErrorValues(results, 1, accountValues.getNameOrAddress(), null, "/scripts/no-name.sh");
        checkErrorValues(results, 2, accountValues.getNameOrAddress(), "no-path", null);
        checkErrorValues(results, 3, accountValues.getNameOrAddress(), "non-executable", "/scripts/non-exec.sh");
    }

    @Test
    public void testCantReadWrongNamedManifest() throws IOException {
        ValidationResponse result = new ActionScriptDiscovery(createAccountValues("/scripts/manifest")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
        result = new ActionScriptDiscovery(createAccountValues("/scripts/manifest.json.bogus")).validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    /**
     * If the manifest file contains a null value for "scripts", validation should fail.
     *
     * @throws Exception if there's a problem running the test
     */
    @Test
    public void testFailIfScriptsPropertyIsNull() throws Exception {
        ValidationResponse result = new ActionScriptDiscovery(
            createAccountValues("/scripts/manifest-null-scripts-property.yaml"))
            .validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    /**
     * If the manifest file defines no scripts, validation should fail.
     *
     * @throws Exception if there's a problem running the test
     */
    @Test
    public void testFailIfNoScriptsDefined() throws Exception {
        ValidationResponse result = new ActionScriptDiscovery(
            createAccountValues("/scripts/manifest-no-scripts-defined.yaml"))
            .validateManifestFile();
        assertEquals(1, result.getErrorDTOCount());
    }

    private void checkScriptValues(DiscoveryResponse results, int index, String name, String path, String description, EntityType entityType, ActionType actionType, ActionScriptPhase actionPhase, Long timeLimitSeconds) {
        assertTrue("No valid script at position " + index + " in discovery response", index < results.getWorkflowCount());
        final Workflow workflow = results.getWorkflow(index);
        assertEquals(name, workflow.hasDisplayName() ? workflow.getDisplayName() : null);
        assertEquals(path, workflow.hasScriptPath() ? workflow.getScriptPath() : null);
        assertEquals(description, workflow.hasDescription() ? workflow.getDescription() : null);
        assertEquals(entityType, workflow.hasEntityType() ? workflow.getEntityType() : null);
        assertEquals(actionType, workflow.hasActionType() ? workflow.getActionType() : null);
        assertEquals(actionPhase, workflow.hasPhase() ? workflow.getPhase() : null);
        assertEquals(timeLimitSeconds, workflow.hasTimeLimitSeconds() ? workflow.getTimeLimitSeconds() : null);
    }

    private void checkErrorValues(DiscoveryResponse results, int index, String host, String name, String path) {
        assertTrue("No invalid script at position " + index + " in discovery response", index < results.getErrorDTOCount());
        String expected = ActionScriptDiscovery.generateWorkflowID(host, name, path);
        assertEquals(expected, results.getErrorDTO(index).getEntityUuid());
    }

}
