package com.vmturbo.mediation.actionscript;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;

/**
 * Class to test the path validation for the Action Script folder.
 * This folder is where the user will place the executable files (scripts)
 * which the ActionScript probe will invoke in the Executor.
 */
public class ActionScriptPathValidatorTest {

    @Rule
    public TemporaryFolder tempParentFolder = new TemporaryFolder();

    /**
     * Create a temporary folder path and check that the path is found.
     *
     * @throws IOException if there is an error creating the folder.
     */
    @Test
    public void testValidateActionScriptPathExists() throws IOException {
        // arrange
        final File existingFolder = tempParentFolder.newFolder("a", "b", "c");
        final Path testExistingPath = existingFolder.toPath();
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // act
        final List<ErrorDTO> errors = validator.validateActionScriptPath(testExistingPath);
        // assert
        assertTrue(errors.isEmpty());
    }

    /**
     * test that a file path that does not exist reports the error
     */
    @Test
    public void testValidateActionScriptPathDoesntExist() {
        // arrange
        final Path nonExistingPath = Paths.get("this", "path", "doest", "exist");
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // act
        final List<ErrorDTO> errors = validator.validateActionScriptPath(nonExistingPath);
        // assert
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("is not found"));
    }

    /**
     * Test that a path to a file is rejected as an invalid actionScriptPath.
     *
     * @throws IOException if there is an error creating the test file
     */
    @Test
    public void testValidateActionScriptNotFolder() throws IOException {
        // arrange
        final File existingFolder = tempParentFolder.newFolder("a", "b");
        // create a/b/c as a file, not a folder
        final Path filePath = Paths.get(existingFolder.getAbsolutePath(), "c");
        final File cFile = filePath.toFile();
        // ignore value since we're calling for side-effect: create parent directories if necessary
        //noinspection ResultOfMethodCallIgnored
        cFile.getParentFile().mkdirs();
        cFile.createNewFile();
        // test the path to the file as the actionScriptPath
        final Path testExistingPath = cFile.toPath();
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // act
        final List<ErrorDTO> errors = validator.validateActionScriptPath(testExistingPath);
        // assert
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("is not a directory"));
    }

    /**
     * test that a file path that does not exist reports the error
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testValidateActionScriptPathNotExecutable() throws IOException {
        final File existingFolder = tempParentFolder.newFolder("a", "b", "c");
        final Path testExistingPath = existingFolder.toPath();
        testExistingPath.toFile().setExecutable(false);
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // act
        final List<ErrorDTO> errors = validator.validateActionScriptPath(testExistingPath);
        // assert
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("is not executable"));
    }

}