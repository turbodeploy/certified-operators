package com.vmturbo.reports.component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.logging.Level;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.birt.core.data.DataTypeUtil;
import org.eclipse.birt.core.exception.BirtException;
import org.eclipse.birt.core.framework.Platform;
import org.eclipse.birt.core.script.ParameterAttribute;
import org.eclipse.birt.report.engine.api.EngineConfig;
import org.eclipse.birt.report.engine.api.IGetParameterDefinitionTask;
import org.eclipse.birt.report.engine.api.IParameterDefnBase;
import org.eclipse.birt.report.engine.api.IRenderOption;
import org.eclipse.birt.report.engine.api.IRenderTask;
import org.eclipse.birt.report.engine.api.IReportDocument;
import org.eclipse.birt.report.engine.api.IReportEngine;
import org.eclipse.birt.report.engine.api.IReportEngineFactory;
import org.eclipse.birt.report.engine.api.IReportRunnable;
import org.eclipse.birt.report.engine.api.IRunTask;
import org.eclipse.birt.report.engine.api.IScalarParameterDefn;
import org.eclipse.birt.report.engine.api.RenderOption;
import org.eclipse.birt.report.engine.api.impl.ParameterSelectionChoice;
import org.eclipse.birt.report.model.api.elements.DesignChoiceConstants;

/**
 * Report runner. This class is a simplification and rework of {@link
 * org.eclipse.birt.report.engine.api.ReportRunner}.
 */
public class ComponentReportRunner implements AutoCloseable {

    /**
     * BIRT report engine.
     */
    private final IReportEngine engine;
    private final Logger logger = LogManager.getLogger();

    /**
     * The type of html. Used to decorate the HTML output, used in Render and RunAndRender mode.
     */
    protected String htmlType = "HTML";

    /**
     * Creates report runner, usable in a separate component.
     *
     * @throws BirtException if exception is raised while initializing Birt.
     */
    public ComponentReportRunner() throws BirtException {
        final EngineConfig config = createEngineConfig();
        Platform.startup(config);
        final IReportEngineFactory factory = (IReportEngineFactory)Platform.createFactoryObject(
                IReportEngineFactory.EXTENSION_REPORT_ENGINE_FACTORY);
        engine = factory.createReportEngine(config);

        // JRE default level is INFO, which may reveal too much internal
        // logging
        // information.
        engine.changeLogLevel(Level.WARNING);
    }

    /**
     * Runs a report and returns a stream representing the result (report data itself).
     *
     * @param request report request to run
     * @return resulting report bytes stream
     * @throws ReportingException if some internal error occurred.
     */
    public InputStream createReport(@Nonnull ReportRequest request) throws ReportingException {
        try {
            final File reportFile = runReport(request);
            final File resultFile;
            try {
                resultFile = renderReport(request, reportFile);
            } finally {
                if (!reportFile.delete()) {
                    logger.warn("Failed to remote report file {}", reportFile.getAbsolutePath());
                }
            }

            return new ColsableFileInputStream(resultFile);
        } catch (BirtException | IOException e) {
            throw new ReportingException("Could not run/render a report " + request.getRptDesign(),
                    e);
        }
    }

    /**
     * Render the report.
     *
     * @param request report request to use.
     * @param reportFile report file to render an output from. This file is an input for
     *         this method to generate report from.
     * @return file, containing an executed report
     * @throws BirtException if Birt thrown an internal exception
     * @throws IOException if error occurred while operating with files
     */
    private File renderReport(@Nonnull ReportRequest request, @Nonnull File reportFile)
            throws BirtException, IOException {
        // use the archive to open the report document
        final IReportDocument document = engine.openReportDocument(reportFile.getAbsolutePath());
        // create the render task
        final IRenderTask task = engine.createRenderTask(document);
        // set report render options
        final IRenderOption options = new RenderOption();
        // set the output format
        options.setOutputFormat(request.getFormat().getLiteral());
        // set the render options
        task.setRenderOption(options);
        // setup the locale
        //task.setLocale(getLocale(locale));
        task.setLocale(Locale.US);

        final File outputFile =
                File.createTempFile("report-", '.' + request.getFormat().getLiteral());
        // setup the output file
        options.setOutputFileName(outputFile.getAbsolutePath());
        task.setPageRange("All");
        task.render();
        task.close();
        return outputFile;
    }

    /**
     * Running the report to create the report document.
     *
     * @param request report rendering request
     * @return File containing a report, filled with data
     * @throws BirtException if Birt thrown an internal exception
     * @throws IOException if error occurred while operating with files
     */
    @Nonnull
    private File runReport(@Nonnull ReportRequest request) throws BirtException, IOException {
        // parse the source to get the report runnable
        try (final InputStream is = getClass().getResourceAsStream(request.getRptDesign())) {
            final IReportRunnable runnable = engine.openReportDesign(is);
            // create the report task
            final IRunTask task = engine.createRunTask(runnable);

            // set the paramter values
            final Map<String, ParameterAttribute> inputValues =
                    evaluateParameterValues(runnable, request.getParameters());
            for (Entry<String, ParameterAttribute> entry : inputValues.entrySet()) {
                final String paraName = entry.getKey();
                final ParameterAttribute pa = entry.getValue();
                final Object valueObject = pa.getValue();
                if (valueObject instanceof Object[]) {
                    final Object[] valueArray = (Object[])valueObject;
                    final String[] displayTextArray = (String[])pa.getDisplayText();
                    task.setParameter(paraName, valueArray, displayTextArray);
                } else {
                    task.setParameter(paraName, pa.getValue(), (String)pa.getDisplayText());
                }
            }

            // set the application context
            task.setAppContext(new HashMap());

            // run the task to create the report document
            final File targetFile = File.createTempFile("report-", ".rptdocument");
            task.run(targetFile.getAbsolutePath());

            // close the task.
            task.close();
            return targetFile;
        }
    }

    /**
     * Evaluate parameter values. This is a rework of (@link VmtReporRunner#evaluateParameterValues}.
     *
     * @param runnable report runnable to use.
     * @param params parameters map (input)
     * @return map of parameters, parsed from the input.
     */
    @Nonnull
    private Map<String, ParameterAttribute> evaluateParameterValues(IReportRunnable runnable,
            @Nonnull Map<String, String> params) {

        final Map<String, ParameterAttribute> inputValues = new HashMap<>();
        final IGetParameterDefinitionTask task = engine.createGetParameterDefinitionTask(runnable);
        @SuppressWarnings("unchecked")
        final Collection<IParameterDefnBase> paramDefns =
                (Collection<IParameterDefnBase>)task.getParameterDefns(false);
        for (IParameterDefnBase pBase : paramDefns) {
            if (pBase instanceof IScalarParameterDefn) {
                final IScalarParameterDefn paramDefn = (IScalarParameterDefn)pBase;

                final String paramName = paramDefn.getName();
                final String inputValue = params.get(paramName);
                final int paramDataType = paramDefn.getDataType();
                final String paramType = paramDefn.getScalarParameterType();

                // if allow multiple values
                boolean isAllowMutipleValues = false;
                try {
                    final Object paramValue;
                    if (DesignChoiceConstants.SCALAR_PARAM_TYPE_MULTI_VALUE.equals(paramType)) {
                        paramValue = stringToObjectArray(paramDataType, inputValue);
                        isAllowMutipleValues = true;
                    } else {
                        paramValue = stringToObject(paramDataType, inputValue);
                    }
                    if (paramValue != null) {
                        final List<ParameterSelectionChoice> selectList =
                                paramDefn.getSelectionList();
                        final ParameterAttribute pa;
                        if (isAllowMutipleValues) {
                            final List<Object> values = (List<Object>)paramValue;
                            final List<String> displayTextList = new ArrayList<>();
                            if (selectList != null && selectList.size() > 0) {
                                for (Object o : values) {
                                    for (ParameterSelectionChoice select : selectList) {
                                        if (o.equals(select.getValue())) {
                                            displayTextList.add(select.getLabel());
                                        }
                                    }
                                }
                            }
                            final String[] displayTexts = new String[displayTextList.size()];
                            pa = new ParameterAttribute(values.toArray(),
                                    displayTextList.toArray(displayTexts));
                        } else {
                            String displayText = null;
                            if (selectList != null && selectList.size() > 0) {
                                for (ParameterSelectionChoice select : selectList) {
                                    if (paramValue.equals(select.getValue())) {
                                        displayText = select.getLabel();
                                        break;
                                    }
                                }
                            }
                            pa = new ParameterAttribute(paramValue, displayText);
                        }
                        inputValues.put(paramName, pa);
                    }
                } catch (BirtException ex) {
                    logger.error("Value of parameter " + paramName + " is invalid", ex);
                }
            }
        }
        return inputValues;
    }

    /**
     * Returns object, represented by input string value.
     *
     * @param type type of the parameter
     * @param value string representation of a parameter
     * @return parameter value object
     * @throws BirtException if error occurred while parsing parameter value from string
     */
    @Nullable
    private static Object stringToObject(int type, String value) throws BirtException {
        if (value == null) {
            return null;
        }
        switch (type) {
            case IScalarParameterDefn.TYPE_BOOLEAN:
                return DataTypeUtil.toBoolean(value);
            case IScalarParameterDefn.TYPE_DATE:
                return DataTypeUtil.toSqlDate(value);
            case IScalarParameterDefn.TYPE_TIME:
                return DataTypeUtil.toSqlTime(value);
            case IScalarParameterDefn.TYPE_DATE_TIME:
                return DataTypeUtil.toDate(value);
            case IScalarParameterDefn.TYPE_DECIMAL:
                return DataTypeUtil.toBigDecimal(value);
            case IScalarParameterDefn.TYPE_FLOAT:
                return DataTypeUtil.toDouble(value);
            case IScalarParameterDefn.TYPE_STRING:
                return DataTypeUtil.toString(value);
            case IScalarParameterDefn.TYPE_INTEGER:
                return DataTypeUtil.toInteger(value);
        }
        return null;
    }

    /**
     * Returns objects array represented by the input string.
     *
     * @param paramDataType the data type
     * @param inputValue parameter value in String
     * @return parameter value in Object[]
     * @throws BirtException if parsing thrown exception
     */
    @Nullable
    private static List<Object> stringToObjectArray(int paramDataType, @Nullable String inputValue)
            throws BirtException {
        if (inputValue == null) {
            return null;
        }
        final String[] inputValues = inputValue.split(",");
        final List<Object> result = new ArrayList<>(inputValues.length);
        for (String value : inputValues) {
            result.add(stringToObject(paramDataType, value));
        }
        return result;
    }

    /**
     * New a EngineConfig and config it with user's setting.
     *
     * @return Birt engine configuration
     */
    private static EngineConfig createEngineConfig() {
        return new EngineConfig();
    }

    @Override
    public void close() {
        engine.destroy();
    }

    /**
     * Special input stream, that will remove the file associated, after the stream is closed.
     */
    private class ColsableFileInputStream extends FileInputStream {

        private final File source;

        private ColsableFileInputStream(@Nonnull File source) throws FileNotFoundException {
            super(source);
            this.source = Objects.requireNonNull(source);
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (!source.delete()) {
                logger.warn("Failed to remove file " + source.getAbsolutePath() +
                        " after it has been closed");
            }
        }
    }
}
