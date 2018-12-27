package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;

/**
 * Class to validate the ActionScript folder. We test that the path points to a directory and that
 * directory is executable (searchable).
 */
public class ActionScriptPathValidator {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Validate the ActionScript folder at 'actionScriptsRootPath'. Check that the path denotes
     * a directory that is executable (i.e. searchable). Return a list of validate errors, if any.
     * Otherwise return an empty list.
     * <p/>
     * Note that, currently, only a single error will be returned from validate, if any. In the future,
     * though, this may change, and so we feel the List of ErrorDTO's returned is best for the future.
     *
     * @param actionScriptsRootPath the Path to the directory containing the ActionScript files
     * @return a list of {@link ErrorDTO}s reflecting any validation errors, or an empty list if no
     * errors are found.
     */
    public List<ErrorDTO> validateActionScriptPath(@Nonnull final Path actionScriptsRootPath) {
        List<ErrorDTO> errorList = Lists.newArrayList();
        final File actionScriptsPathRoot = actionScriptsRootPath.toFile();
        if (actionScriptsPathRoot.exists()) {
            if (!actionScriptsPathRoot.isDirectory()) {
                logError(errorList, "ActionScript Directory (" + actionScriptsRootPath +
                    ") is not a directory");
            } else {
                if (!actionScriptsPathRoot.canExecute()) {
                    logError(errorList, "ActionScript Directory (" + actionScriptsRootPath +
                        ") is not executable");
                }
            }
        } else {
            logError(errorList, "ActionScript Directory (" + actionScriptsRootPath +
                ") is not found");
        }
        return errorList;
    }

    /**
     * Log an error string to the console and add a new {@link ErrorDTO} to the given errorList.
     *
     * @param errorList the list of errors to add to
     * @param errorString the string describing the error
     */
    private void logError(final List<ErrorDTO> errorList, final String errorString) {
        logger.error(errorString);
        errorList.add(ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.CRITICAL)
            .setDescription(errorString)
            .build());
    }
}