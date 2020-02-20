package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Arrays;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.RIDiscountJournalEntry;

/**
 * A {@link JournalEntryTabulator} for {@link RIDiscountJournalEntry}.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class RIDiscountTabulator<E> extends JournalEntryTabulator<E> {

    /**
     * Constructor.
     */
    public RIDiscountTabulator() {
        super(RIDiscountJournalEntry.class, Arrays.asList(
                new ColumnInfo("RI Tier ID", 14, 18),
                new ColumnInfo("RI ID", 14, 18),
                new ColumnInfo("RI Spec ID", 14, 18),
                new ColumnInfo("Is buy RI", 10, 10),
                new ColumnInfo("RI bought percentage", 20, 20)));
    }

    @Override
    protected void addToTable(@Nonnull final QualifiedJournalEntry<E> entry,
            @Nonnull final AsciiTable asciiTable,
            @Nonnull final EntityInfoExtractor<E> infoExtractor) {
        if (entry instanceof RIDiscountJournalEntry) {
            final RIDiscountJournalEntry<E> riDiscountEntry = (RIDiscountJournalEntry<E>)entry;
            final ReservedInstanceData riData = riDiscountEntry.getRiData();
            final ReservedInstanceSpec reservedInstanceSpec = riData.getReservedInstanceSpec();
            asciiTable.addRow(reservedInstanceSpec.getReservedInstanceSpecInfo().getTierId(),
                    riData.getReservedInstanceBought().getId(), reservedInstanceSpec.getId(),
                    riDiscountEntry.isBuyRI(), riDiscountEntry.getRiBoughtPercentage().getValue());
        }
    }
}
