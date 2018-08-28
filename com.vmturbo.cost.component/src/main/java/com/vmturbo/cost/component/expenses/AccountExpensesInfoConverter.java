package com.vmturbo.cost.component.expenses;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;

/**
 * Convert a AccountExpensesInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class AccountExpensesInfoConverter implements Converter<byte[], AccountExpensesInfo> {

    @Override
    public AccountExpensesInfo from(byte[] instanceBlob) {
        try {
            return AccountExpensesInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instance blob to Account Expenses: ", e);
        }
    }

    @Override
    public byte[] to(AccountExpensesInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<AccountExpensesInfo> toType() {
        return AccountExpensesInfo.class;
    }
}
