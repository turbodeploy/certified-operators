package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;

/**
 * A helper class to tabulate a list of {@link QualifiedJournalEntry}s in an ASCII table.
 * The logic of tabulation is closely tied to the entry, but we split it off for readability.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public abstract class JournalEntryTabulator<E> {

    protected final Class<? extends QualifiedJournalEntry> entryClass;
    private final Collection<ColumnInfo> columnInfos;

    /**
     * Constructor.
     *
     * @param entryClass class of an item that contribute to the cost of an entity
     * @param columnInfos information about the columns for the table
     */
    public JournalEntryTabulator(final Class<? extends QualifiedJournalEntry> entryClass,
            final Collection<ColumnInfo> columnInfos) {
        Preconditions.checkArgument(!columnInfos.isEmpty(), "there should be at least one column");
        this.entryClass = entryClass;
        this.columnInfos = columnInfos;
    }

    /**
     * Tabulate the entries of this tabulator's entry class into an ASCII table.
     *
     * @param infoExtractor The extractor to use to get information out of the entity.
     * @param entries The journal entries to tabulate. A single {@link JournalEntryTabulator}
     * subclass will only tabulate a subset of the entries (matching the class).
     * @return The string of an ASCII table for the entry class of this tabulator.
     */
    @Nonnull
    public String tabulateEntries(@Nonnull final EntityInfoExtractor<E> infoExtractor,
            @Nonnull final Map<CostCategory, SortedSet<QualifiedJournalEntry<E>>> entries) {
        final CWC_LongestLine columnWidthCalculator = new CWC_LongestLine();
        columnInfos.forEach(
                colInfo -> columnWidthCalculator.add(colInfo.getMinWidth(), colInfo.getMaxWidth()));

        final AsciiTable table = new AsciiTable();
        table.getContext().setGrid(A7_Grids.minusBarPlusEquals());
        table.getRenderer().setCWC(columnWidthCalculator);
        table.addRule();
        table.addRow(Collections2.transform(columnInfos, ColumnInfo::getHeading));
        boolean empty = true;
        for (final Entry<CostCategory, SortedSet<QualifiedJournalEntry<E>>> categoryEntry : entries.entrySet()) {
            final List<QualifiedJournalEntry<E>> typeSpecificEntriesForCategory =
                    categoryEntry.getValue()
                            .stream()
                            .filter(entryClass::isInstance)
                            .collect(Collectors.toList());
            if (!typeSpecificEntriesForCategory.isEmpty()) {
                table.addRule();
                table.addRow(Stream.concat(
                        columnInfos.stream().limit(columnInfos.size() - 1L).map(heading -> null),
                        Stream.of(categoryEntry.getKey().name())).collect(Collectors.toList()));
                typeSpecificEntriesForCategory.forEach(entry -> {
                    table.addRule();
                    addToTable(entry, table, infoExtractor);
                });
                empty = false;
            }
        }

        if (empty) {
            return "None";
        }

        table.addRule();

        // Text alignment for the table must be set AFTER rows are added to it.
        table.setTextAlignment(TextAlignment.LEFT);
        return table.render();
    }

    /**
     * Add an entry of this tabulator's type to the ascii table being constructed.
     *
     * @param entry the entry to add. This entry will be of the proper subclass (although
     * I haven't been able to get the generic parameters to work properly
     * to guarantee it).
     * @param asciiTable the ascii table to add the row to
     * @param infoExtractor the entity extractor to use
     */
    protected abstract void addToTable(@Nonnull QualifiedJournalEntry<E> entry,
            @Nonnull AsciiTable asciiTable, @Nonnull EntityInfoExtractor<E> infoExtractor);
}
