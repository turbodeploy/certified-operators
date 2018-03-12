package com.vmturbo.history.schema;

import org.jooq.impl.EnumConverter;

import com.vmturbo.api.enums.ReportOutputFormat;

public class ReportOutputFormatConverter extends EnumConverter<Byte, ReportOutputFormat>{
	private static final long serialVersionUID = 1L;

	public ReportOutputFormatConverter() {
		super(Byte.class, ReportOutputFormat.class);
	}
}
