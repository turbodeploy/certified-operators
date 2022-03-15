package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Arrays;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;

import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.CloudCommitmentDiscountJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link JournalEntryTabulator} for {@link CloudCommitmentDiscountJournalEntry}.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class CloudCommitmentDiscountTabulator<E> extends JournalEntryTabulator<E> {

    /**
     * Constructor.
     */
    public CloudCommitmentDiscountTabulator() {
        super(CloudCommitmentDiscountJournalEntry.class, Arrays.asList(
                        new ColumnInfo("Commitment ID", 14, 18),
                        new ColumnInfo("Type", 14, 18),
                        new ColumnInfo("Commodity", 14, 18),
                        new ColumnInfo("Coverage percent", 14, 18),
                        new ColumnInfo("Capacity", 14, 18),
                        new ColumnInfo("Used", 14, 18)
        ));
    }

    @Override
    protected void addToTable(@Nonnull QualifiedJournalEntry<E> entry,
                                        @Nonnull AsciiTable asciiTable,
                                        @Nonnull EntityInfoExtractor<E> infoExtractor) {
        if (entry instanceof CloudCommitmentDiscountJournalEntry) {
            final CloudCommitmentDiscountJournalEntry<E> commitmentEntry = (CloudCommitmentDiscountJournalEntry<E>)entry;
            long commitmentId = commitmentEntry.getCommitmentData().commitmentId();
            String commitmentTypeString = commitmentEntry.getCoverageVector().getVectorType().getCoverageType().toString();
            String commodityTypeString = commitmentEntry.commodityType().map(Enum::toString).orElse("");
            TraxNumber coveragePercent = commitmentEntry.getCoverageRatio().times(100.0).compute();
            String coveragePercentString = String.format("%.0f%%", coveragePercent.getValue());
            double capacity = commitmentEntry.getCoverageVector().getCapacity();
            double used = commitmentEntry.getCoverageVector().getUsed();
            asciiTable.addRow(
                commitmentId,
                commitmentTypeString,
                commodityTypeString,
                coveragePercentString,
                capacity,
                used
            );
        }
    }
}
