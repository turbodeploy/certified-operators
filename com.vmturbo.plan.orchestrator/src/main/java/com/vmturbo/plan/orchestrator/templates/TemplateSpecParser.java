package com.vmturbo.plan.orchestrator.templates;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;

/**
 * Parse a Template spec Json file, it contains pre-defined template specs which contains information
 * about how to display template fields on UI.
 */
public class TemplateSpecParser implements DiagsRestorable {

    private final Logger log = LogManager.getLogger();

    private final String templateSpecFileName;

    private final Map<String, TemplateSpec> templateSpecMap;

    @VisibleForTesting
    static final Type TYPE = new TypeToken<Map<String, TemplateSpec>>() {}.getType();
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    public TemplateSpecParser(String templateSpecFileName) {
        this.templateSpecFileName = templateSpecFileName;
        this.templateSpecMap = buildTemplateSpec();
    }

    public Map<String, TemplateSpec> getTemplateSpecMap() {
        return this.templateSpecMap;
    }

    /**
     * Parse Template Spec Json file to TemplateSpec Map
     *
     * @return A TemplateSpec Map which key is entity Type and value is TemplateSpec object
     */
    private Map<String, TemplateSpec> buildTemplateSpec() {
        try (InputStream is = Thread.currentThread()
            .getContextClassLoader().getResourceAsStream(templateSpecFileName)) {
            log.info("Loading default template spec json file " + templateSpecFileName);
            String dataJSON = IOUtils.toString(is, Charset.defaultCharset());
            return GSON.fromJson(dataJSON, TYPE);
        } catch (IOException e) {
            throw new RuntimeException("Unable to build template spec from file " + templateSpecFileName);
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method retrieves the map of all template specs and serializes it as a JSON string.
     *
     * @return a list of serialized template specs
     * @throws DiagnosticsException if the db already contains template specs, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              templates.
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        log.info("Collecting diagnostics for map of {} template specs",
                getTemplateSpecMap().size());
        appender.appendString(GSON.toJson(getTemplateSpecMap(), TYPE));
    }

    /**
     * {@inheritDoc}
     *
     * This method clears the map of all existing template specs, then deserializes and adds a map
     * of serialized template specs from diagnostics. There should only be one map in the list
     * if the correct diags file is being read.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @throws DiagnosticsException if template specs are already present, or in response
     *                              to any errors that may occur deserializing or restoring the
     *                              template spec map.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final int preexisting = getTemplateSpecMap().size();
        if (preexisting > 0) {
            final String message = "Clearing " + preexisting + " preexisting template specs from " +
                "template spec map: " + getTemplateSpecMap().keySet();
            errors.add(message);
            log.warn(message);
            templateSpecMap.clear();
        }

        final String serialized = collectedDiags.get(0);
        if (collectedDiags.size() > 1) {
            errors.add("Only one template spec map should exist in diags. Restoring " +
                serialized + " and ignoring subsequent objects.");
        }

        try {
            templateSpecMap.putAll(GSON.fromJson(serialized, TYPE));
        } catch (JsonParseException e) {
            errors.add("Failed to restore serialized template spec map " + serialized +
                " from diagnostics because of parsing exception " + e.getMessage());
        }

        log.info("Restored {} template specs to the template spec parser from diags",
            templateSpecMap.size());

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }

    }

    @Nonnull
    @Override
    public String getFileName() {
        return "TemplateSpecs";
    }
}