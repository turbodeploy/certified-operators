package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Arrays;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.RIJournalEntry;

/**
 * A {@link JournalEntryTabulator} for {@link RIJournalEntry}.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class RIEntryTabulator<E> extends JournalEntryTabulator<E> {

    /**
     * Constructor.
     */
    public RIEntryTabulator() {
        super(RIJournalEntry.class, Arrays.asList(
                new ColumnInfo("RI Tier ID", 14, 18),
                new ColumnInfo("RI ID", 14, 18),
                new ColumnInfo("RI Spec ID", 14, 18),
                new ColumnInfo("Coupons", 10, 21),
                new ColumnInfo("Cost", 10, 10)));
    }

    @Override
    protected void addToTable(@Nonnull final QualifiedJournalEntry<E> entry,
            @Nonnull final AsciiTable asciiTable,
            @Nonnull final EntityInfoExtractor<E> infoExtractor) {
        if (entry instanceof RIJournalEntry) {
            final RIJournalEntry<E> riEntry = (RIJournalEntry<E>)entry;
            final ReservedInstanceData riData = riEntry.getRiData();
            final ReservedInstanceSpec reservedInstanceSpec = riData.getReservedInstanceSpec();
            asciiTable.addRow(reservedInstanceSpec.getReservedInstanceSpecInfo().getTierId(),
                    riData.getReservedInstanceBought().getId(), reservedInstanceSpec.getId(),
                    riEntry.getCouponsCovered(), riEntry.getHourlyCost().getValue());
        }
    }
}
