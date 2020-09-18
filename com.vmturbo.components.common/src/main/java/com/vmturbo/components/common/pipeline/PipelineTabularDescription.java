package com.vmturbo.components.common.pipeline;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.components.common.pipeline.Stage.SupplyToContext;

/**
 * Utility for generating a tabular description of a Pipeline and its stages.
 */
public class PipelineTabularDescription {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Stages column headings.
     */
    public static final List<String> STAGES_TABLE_COLUMN_HEADINGS =
        Arrays.asList("STAGE", "INPUT", "OUTPUT", "PROVIDES", "REQUIRES", "DROPS");

    /**
     * The minimum column widths in the report table for each column.
     * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
     */
    public static final int[] STAGE_MIN_COLUMN_WIDTHS = { 21, 12, 12, 12, 12, 12 };

    /**
     * The maximum column widths in the report table for each column.
     * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
     */
    public static final int[] STAGE_MAX_COLUMN_WIDTHS = { 31, 30, 30, 25, 25, 25 };

    /**
     * The column width calculator used to format the table. For details, see
     * http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
     */
    private static final CWC_LongestLine STAGES_COLUMN_WIDTH_CALCULATOR = new CWC_LongestLine();

    static {
        for (int i = 0; i < STAGE_MIN_COLUMN_WIDTHS.length; i++) {
            STAGES_COLUMN_WIDTH_CALCULATOR.add(STAGE_MIN_COLUMN_WIDTHS[i], STAGE_MAX_COLUMN_WIDTHS[i]);
        }
    }

    private PipelineTabularDescription() {
        // Private constructor for utility class.
    }

    /**
     * Generate a simple tabular description summarizing the pipeline stages with their
     * inputs, outputs, and context member provides, requires, and drops. This is useful
     * to understand the dataflow within the pipeline at a glance.
     *
     * @param pipeline The pipeline to be described.
     * @param pipelineTitle The title of the pipeline (ie "Live TopologyPipeline") etc.
     * @param <I> The input to the pipeline. This is the input to the first stage.
     * @param <O> The output of the pipeline. This is the output of the last stage.
     * @param <C> The {@link PipelineContext} for the pipeline.
     * @param <S> The {@link PipelineSummary} for the pipeline.
     * @return An ASCII tabular description of the dataflow of the pipeline suitable for printing to the logs.
     */
    static <I, O, C extends PipelineContext, S extends PipelineSummary>
    String tabularDescription(@Nonnull final Pipeline<I, O, C, S> pipeline,
                              @Nonnull final String pipelineTitle) {
        try {
            final AsciiTable table = new AsciiTable();
            table.getContext().setGrid(A7_Grids.minusBarPlusEquals());
            table.getRenderer().setCWC(STAGES_COLUMN_WIDTH_CALCULATOR);

            table.addRule();
            table.addRow(signatureRow(STAGES_TABLE_COLUMN_HEADINGS, pipelineTitle + " definition (*indicates a default will be used)"));
            table.addRule();
            table.addRow(STAGES_TABLE_COLUMN_HEADINGS);

            final Set<PipelineContextMemberDefinition<?>> seenDefinitions = new HashSet<>();
            final List<SupplyToContext<?>> initialContextMemberSuppliers = pipeline.getInitialContextMemberSuppliers();
            if (initialContextMemberSuppliers.size() > 0) {
                // Add an entry for the context members provided at pipeline initialization.
                table.addRule();
                final List<PipelineContextMemberDefinition<?>> defs = initialContextMemberSuppliers.stream()
                    .map(supplier -> supplier.getMemberDefinition())
                    .collect(Collectors.toList());
                tableRow(table, Pipeline.INITIAL_STAGE_NAME, "", "", defs.iterator(),
                    Collections.emptyIterator(), Collections.emptyIterator(), seenDefinitions);
            }

            // Add entries for each stage.
            pipeline.getStages().forEach(stage -> {
                table.addRule();
                addStageRow(table, stage, seenDefinitions);
            });
            table.addRule();

            // Text alignment for the table must be set AFTER rows are added to it.
            table.setTextAlignment(TextAlignment.LEFT);
            return table.render();
        } catch (RuntimeException e) {
            logger.error("Exception: ", e);
            return "";
        }
    }

    private static void addStageRow(@Nonnull final AsciiTable table, @Nonnull final Stage<?, ?, ?> stage,
                                    @Nonnull final Set<PipelineContextMemberDefinition<?>> seenDefinitions) {
        try {
            final ParameterizedType type = getStageParameterizedType(stage.getClass());
            String inputName = "";
            String outputName = "";

            final Iterator<PipelineContextMemberDefinition<?>> provides = stage.getProvidedContextMembers().iterator();
            final Iterator<PipelineContextMemberDefinition<?>> requires = stage.getContextMemberRequirements().iterator();
            final Iterator<PipelineContextMemberDefinition<?>> toDrop = stage.getContextMembersToDrop().iterator();

            if (type != null) {
                final Type[] actualTypeArguments = type.getActualTypeArguments();

                if (actualTypeArguments != null && actualTypeArguments.length >= 1) {
                    if (actualTypeArguments.length == 1) {
                        // Indicates this is a passthrough stage
                        inputName = getParameterizedTypeName(actualTypeArguments[0]);
                        outputName = inputName;
                    } else {
                        // Indicates this is not a Passthrough stage.
                        inputName = getParameterizedTypeName(actualTypeArguments[0]);
                        outputName = getOutputParameterizedTypeName(actualTypeArguments[1], inputName);
                    }
                }
            }

            tableRow(table, stage.getName(), inputName, outputName, provides,
                requires, toDrop, seenDefinitions);
        } catch (RuntimeException e) {
            logger.error("Error: ", e);
        }
    }

    private static void tableRow(@Nonnull final AsciiTable table, @Nonnull final String stageName,
                                 @Nonnull final String inputName, @Nonnull final String outputName,
                                 @Nonnull final Iterator<PipelineContextMemberDefinition<?>> provides,
                                 @Nonnull final Iterator<PipelineContextMemberDefinition<?>> requires,
                                 @Nonnull final Iterator<PipelineContextMemberDefinition<?>> toDrop,
                                 @Nonnull final Set<PipelineContextMemberDefinition<?>> seenDefinitions) {
        table.addRow(stageName, inputName, outputName,
            nextProvides(provides, seenDefinitions),
            nextRequired(requires, seenDefinitions),
            toDrop.hasNext() ? toDrop.next().getName() : "");
        while (provides.hasNext() || requires.hasNext() || toDrop.hasNext()) {
            table.addRow("", "", "",
                nextProvides(provides, seenDefinitions),
                nextRequired(requires, seenDefinitions),
                toDrop.hasNext() ? toDrop.next().getName() : "");
        }
    }

    private static String nextRequired(@Nonnull final Iterator<PipelineContextMemberDefinition<?>> requires,
                                       @Nonnull final Set<PipelineContextMemberDefinition<?>> seenDefinitions) {
        if (requires.hasNext()) {
            final PipelineContextMemberDefinition<?> nextRequired = requires.next();
            if (!seenDefinitions.contains(nextRequired)) {
                seenDefinitions.add(nextRequired);
                return "*" + nextRequired.getName();
            } else {
                return nextRequired.getName();
            }
        } else {
            return "";
        }
    }

    private static String nextProvides(@Nonnull final Iterator<PipelineContextMemberDefinition<?>> provides,
                                       @Nonnull final Set<PipelineContextMemberDefinition<?>> seenDefinitions) {
        if (provides.hasNext()) {
            final PipelineContextMemberDefinition<?> nextProvides = provides.next();
            seenDefinitions.add(nextProvides);
            return nextProvides.getName();
        } else {
            return "";
        }
    }

    private static String getParameterizedTypeName(@Nonnull final Type type) {
        if (type instanceof Class) {
            return ((Class<?>)type).getSimpleName();
        } else {
            ParameterizedType pt = (ParameterizedType)type;
            final String rawClass = ((Class<?>)pt.getRawType()).getSimpleName();
            return rawClass + Stream.of(((ParameterizedType)type).getActualTypeArguments())
                .map(PipelineTabularDescription::getParameterizedTypeName)
                .collect(Collectors.joining(", ", "<", ">"));
        }
    }

    private static String getOutputParameterizedTypeName(@Nonnull final Type potentialOutputType,
                                                         @Nonnull final String inputTypeName) {
        if (potentialOutputType instanceof Class) {
            Class<?> klass = (Class<?>)potentialOutputType;
            if (PipelineContext.class.isAssignableFrom(klass)) {
                return inputTypeName;
            } else {
                return klass.getSimpleName();
            }
        } else {
            return getParameterizedTypeName(potentialOutputType);
        }
    }

    private static ParameterizedType getStageParameterizedType(@Nonnull Class stageClass) {
        while (stageClass != null && !(stageClass.getGenericSuperclass() instanceof ParameterizedType)) {
            stageClass = stageClass.getSuperclass();
        }

        if (stageClass != null && stageClass.getGenericSuperclass() instanceof ParameterizedType) {
            return (ParameterizedType)stageClass.getGenericSuperclass();
        }
        return null;
    }

    /**
     * Compose a table row with all nulls except the last which is the signature.
     * Equivalent to Arrays.asList(null, null, ...., signature) where the
     * number of nulls is STAGES_TABLE_COLUMN_HEADINGS.size() - 1.
     * <p/>
     * The reason for this is we want the signature to span the entire row, and
     * nulling out a column merges it with the following column in the table.
     * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_02_ColSpan.html.
     *
     * @param columnHeadings Titles for the column headings.
     * @param signature The signature to be inserted into the row.
     * @return A collection appropriate for the table signature row.
     */
    private static Collection<String> signatureRow(@Nonnull final List<String> columnHeadings,
                                                   @Nonnull final String signature) {
        return Stream.concat(
            columnHeadings.stream()
                .limit(columnHeadings.size() - 1)
                .map(heading -> null),
            Stream.of(signature)
        ).collect(Collectors.toList());
    }
}
