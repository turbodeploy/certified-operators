package com.vmturbo.cost.calculation.journal.tabulator;

import java.util.Arrays;

import javax.annotation.Nonnull;

import de.vandermeer.asciitable.AsciiTable;

import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.entry.OnDemandJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A {@link JournalEntryTabulator} for {@link OnDemandJournalEntry}.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class OnDemandEntryTabulator<E> extends JournalEntryTabulator<E> {

    /**
     * Constructor.
     */
    public OnDemandEntryTabulator() {
        super(OnDemandJournalEntry.class, Arrays.asList(
                new ColumnInfo("Payee Name", 21, 31),
                new ColumnInfo("Payee ID", 14, 18),
                new ColumnInfo("Payee Type", 10, 20),
                new ColumnInfo("Price Unit", 5, 12),
                new ColumnInfo("End Range", 5, 10),
                new ColumnInfo("Price", 10, 10),
                new ColumnInfo("Amt Bought", 10, 10)));
    }

    @Override
    protected void addToTable(@Nonnull final QualifiedJournalEntry<E> entry,
            @Nonnull final AsciiTable asciiTable,
            @Nonnull final EntityInfoExtractor<E> infoExtractor) {
        if (entry instanceof OnDemandJournalEntry) {
            final OnDemandJournalEntry<E> onDemandEntry = (OnDemandJournalEntry<E>)entry;
            final int entityTypeInt = infoExtractor.getEntityType(onDemandEntry.getPayee());
            final EntityType entityType = EntityType.forNumber(entityTypeInt);

            asciiTable.addRow(infoExtractor.getName(onDemandEntry.getPayee()),
                    infoExtractor.getId(onDemandEntry.getPayee()),
                    entityType == null ? entityTypeInt : entityType,
                    onDemandEntry.getPrice().getUnit(),
                    onDemandEntry.getPrice().getEndRangeInUnits() == 0 ? "Inf" :
                            onDemandEntry.getPrice().getEndRangeInUnits(),
                    onDemandEntry.getPrice().getPriceAmount().getAmount(),
                    onDemandEntry.getUnitsBought().getValue());
        }
    }
}
