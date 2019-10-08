package com.vmturbo.common.protobuf;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * Miscellaneous list utilities for use with protobuf objects.
 */
public class ListUtil {

    private ListUtil() {}

    /**
     * Merge two sorted lists and remove duplicates.
     *
     * @param sortedList1 sorted list1
     * @param sortedList2 sorted list2
     * @param <T> the type of the element in the list
     * @return merged sorted list without duplicates
     */
    public static <T extends Comparable<? super T>> List<T> mergeTwoSortedListsAndRemoveDuplicates(
            @Nonnull final List<T> sortedList1,
            @Nonnull final List<T> sortedList2) {
        int size1 = sortedList1.size();
        int size2 = sortedList2.size();
        final List<T> sortedList = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while (i < size1 || j < size2) {
            T x = (i < size1) ? sortedList1.get(i) : sortedList2.get(j);
            T y = (j < size2) ? sortedList2.get(j) : sortedList1.get(i);
            if (x.compareTo(y) < 0) {
                sortedList.add(x);
                i++;
            } else if (x.compareTo(y) > 0) {
                sortedList.add(y);
                j++;
            } else {
                sortedList.add(x);
                i++;
                j++;
            }
        }

        return sortedList;
    }
}
