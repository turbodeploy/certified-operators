/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.history.stats.PropertySubType;

/**
 * Checks that {@link HistUtilizationRecordVisitor} is working as expected.
 */
public class HistUtilizationRecordVisitorTest {

    /**
     * Checked that result {@link StatRecord} will contain {@link HistUtilizationValue}s in the
     * order of property slot numbers.
     */
    @Test
    public void checkStatRecordsCreatedInOrderBasedOnSlotNumber() {
        final RecordVisitor<HistUtilizationRecord> timeslotsRecordVisitor =
                        new HistUtilizationRecordVisitor(HistoryUtilizationType.Timeslot);
        final List<HistUtilizationRecord> timeSlotRecords =
                        Arrays.asList(createTimeslotRecord(10, 258),
                                        createTimeslotRecord(20, 10),
                                        createTimeslotRecord(30, 500),
                                        createTimeslotRecord(40, 450));
        timeSlotRecords.forEach(timeslotsRecordVisitor::visit);
        final StatRecord.Builder builder = StatRecord.newBuilder();
        timeslotsRecordVisitor.build(builder);
        final StatRecord record = builder.build();
        final List<HistUtilizationValue> histUtilizationValues =
                        record.getHistUtilizationValueList();
        final Collection<String> types =
                        histUtilizationValues.stream().map(HistUtilizationValue::getType)
                                        .collect(Collectors.toSet());
        Assert.assertThat(types.size(), CoreMatchers.is(1));
        Assert.assertThat(types.iterator().next(),
                        CoreMatchers.is(HistoryUtilizationType.Timeslot.getApiParameterName()));
        final List<Float> usages =
                        histUtilizationValues.stream().map(HistUtilizationValue::getUsage)
                                        .map(StatValue::getAvg).collect(Collectors.toList());
        Assert.assertThat(usages, CoreMatchers.is(Arrays.asList(20F, 10F, 40F, 30F)));
        final Set<Float> capacities =
                        histUtilizationValues.stream().map(HistUtilizationValue::getCapacity)
                                        .map(StatValue::getAvg).collect(Collectors.toSet());
        Assert.assertThat(capacities.size(), CoreMatchers.is(1));
        Assert.assertThat(capacities.iterator().next(), CoreMatchers.is(100F));
    }

    private static HistUtilizationRecord createTimeslotRecord(int utilization, int slot) {
        return new HistUtilizationRecord(777L, null, UICommodityType.POOL_CPU.typeNumber(),
                        PropertySubType.Utilization.ordinal(), null,
                        HistoryUtilizationType.Timeslot.ordinal(), slot,
                        BigDecimal.valueOf(utilization), 100D);
    }

}
