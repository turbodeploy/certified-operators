package com.vmturbo.plan.orchestrator.templates;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Parse a Template spec Json file, it contains pre-defined template specs which contains information
 * about how to display template fields on UI.
 */
public class TemplateSpecParser {

    private final Logger log = LogManager.getLogger();

    private final String templateSpecFileName;

    private final Map<String, TemplateSpec> templateSpecMap;

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
            Type useCaseType = new TypeToken<Map<String, TemplateSpec>>() {}.getType();
            @SuppressWarnings("unchecked")
            Map<String, TemplateSpec> useCases = ComponentGsonFactory.createGson().fromJson(dataJSON, useCaseType);
            return useCases;
        } catch (IOException e) {
            throw new RuntimeException("Unable to build template spec from file " + templateSpecFileName);
        }
    }
}