package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Arrays;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.ReservedLicenseJournalEntry;

/**
 * A {@link JournalEntryTabulator} for {@link ReservedLicenseJournalEntry}.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class ReservedLicenseTabulator<E> extends JournalEntryTabulator<E> {

    /**
     * Constructor.
     */
    public ReservedLicenseTabulator() {
        super(ReservedLicenseJournalEntry.class,
                Arrays.asList(
                        new ColumnInfo("RI Tier ID", 14, 18),
                        new ColumnInfo("RI ID", 14, 18),
                        new ColumnInfo("RI Spec ID", 14, 18),
                        new ColumnInfo("RI bought percentage", 20, 20)));
    }

    @Override
    protected void addToTable(@Nonnull final QualifiedJournalEntry<E> entry,
            @Nonnull final AsciiTable asciiTable,
            @Nonnull final EntityInfoExtractor<E> infoExtractor) {
        if (entry instanceof ReservedLicenseJournalEntry) {
            final ReservedLicenseJournalEntry<E> reservedLicenseEntry =
                    (ReservedLicenseJournalEntry<E>)entry;
            final ReservedInstanceData riData = reservedLicenseEntry.getRiData();
            final ReservedInstanceSpec reservedInstanceSpec = riData.getReservedInstanceSpec();
            asciiTable.addRow(reservedInstanceSpec.getReservedInstanceSpecInfo().getTierId(),
                    riData.getReservedInstanceBought().getId(), reservedInstanceSpec.getId(),
                    reservedLicenseEntry.getRiBoughtPercentage().getValue());
        }
    }
}
