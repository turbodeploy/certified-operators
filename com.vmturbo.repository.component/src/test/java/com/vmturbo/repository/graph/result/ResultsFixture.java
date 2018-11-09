package com.vmturbo.repository.graph.result;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import com.google.common.base.Joiner;

import javaslang.collection.Stream;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

public class ResultsFixture {

    private static final Random RANDOM = new Random(System.nanoTime());

    public static final String BA_TYPE = "BusinessAccount";
    public static final String PM_TYPE = "PhysicalMachine";
    public static final String VM_TYPE = "VirtualMachine";
    public static final String APP_TYPE = "Application";
    public static final String CONTAINER_TYPE = "Container";
    public static final String DC_TYPE = "DataCenter";
    public static final String ST_TYPE = "Storage";
    public static final String DA_TYPE = "DiskArray";
    public static final String VDC_TYPE = "VirtualDataCenter";

    public static Supplier<ServiceEntityRepoDTO> serviceEntitySupplier(final String type) {
        return () -> {
            final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
            final long oid = RANDOM.nextInt(999999999);
            final String uuid = Long.toString(oid);

            serviceEntityRepoDTO.setEntityType(type);
            serviceEntityRepoDTO.setUuid(uuid);
            serviceEntityRepoDTO.setOid(Long.toString(oid));
            serviceEntityRepoDTO.setDisplayName(Joiner.on('-').join(type, oid));

            return serviceEntityRepoDTO;
        };
    }

    public static List<ServiceEntityRepoDTO> fill(final int n, final Supplier<ServiceEntityRepoDTO> supplier) {
        return Stream.continually(supplier).take(n).toJavaList();
    }

    public static List<ServiceEntityRepoDTO> fill(final int n, final String entityType) {
        return Stream.continually(serviceEntitySupplier(entityType)).take(n).toJavaList();
    }

    public static ServiceEntityRepoDTO fillOne(final String entityType) {
        return Stream.continually(serviceEntitySupplier(entityType)).take(1).get();
    }
}
