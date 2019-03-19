package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;

/**
 * Class to hold contents of disk type info like the number of different disk types from
 * TopologyEntityDTO .
 */
@JsonInclude(Include.NON_EMPTY)
public class DiskTypeInfoRepoDTO {

    private Long numSsd;

    private Long num7200Disks;

    private Long num10kDisks;

    private Long num15kDisks;

    private Long numVSeriesDisks;

    public DiskTypeInfoRepoDTO() { }

    public DiskTypeInfoRepoDTO(@Nonnull final DiskTypeInfo diskTypeInfo) {

        if (diskTypeInfo.hasNumSsd()) {
            setNumSsd(diskTypeInfo.getNumSsd());
        }
        if (diskTypeInfo.hasNum7200Disks()) {
            setNum7200Disks(diskTypeInfo.getNum7200Disks());
        }
        if (diskTypeInfo.hasNum10KDisks()) {
            setNum10kDisks(diskTypeInfo.getNum10KDisks());
        }
        if (diskTypeInfo.hasNum15KDisks()) {
            setNum15kDisks(diskTypeInfo.getNum15KDisks());
        }
        if (diskTypeInfo.hasNumVSeriesDisks()) {
            setNumVSeriesDisks(diskTypeInfo.getNumVSeriesDisks());
        }
    }

    @Nonnull
    public DiskTypeInfo createDiskTypeInfo() {

        final DiskTypeInfo.Builder dtInfoBuilder = DiskTypeInfo.newBuilder();
        if (getNumSsd() != null) {
            dtInfoBuilder.setNumSsd(getNumSsd());
        }
        if (getNum7200Disks() != null) {
            dtInfoBuilder.setNum7200Disks(getNum7200Disks());
        }
        if (getNum10kDisks() != null) {
            dtInfoBuilder.setNum10KDisks(getNum10kDisks());
        }
        if (getNum15kDisks() != null) {
            dtInfoBuilder.setNum15KDisks(getNum15kDisks());
        }
        if (getNumVSeriesDisks() != null) {
            dtInfoBuilder.setNumVSeriesDisks(getNumVSeriesDisks());
        }
        return dtInfoBuilder.build();
    }

    public Long getNumSsd() {
        return numSsd;
    }

    public void setNumSsd(final Long numSsd) {
        this.numSsd = numSsd;
    }

    public Long getNum7200Disks() {
        return num7200Disks;
    }

    public void setNum7200Disks(final Long num7200Disks) {
        this.num7200Disks = num7200Disks;
    }

    public Long getNum10kDisks() {
        return num10kDisks;
    }

    public void setNum10kDisks(final Long num10kDisks) {
        this.num10kDisks = num10kDisks;
    }

    public Long getNum15kDisks() {
        return num15kDisks;
    }

    public void setNum15kDisks(final Long num15kDisks) {
        this.num15kDisks = num15kDisks;
    }

    public Long getNumVSeriesDisks() {
        return numVSeriesDisks;
    }

    public void setNumVSeriesDisks(final Long numVSeriesDisks) {
        this.numVSeriesDisks = numVSeriesDisks;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DiskTypeInfoRepoDTO that = (DiskTypeInfoRepoDTO) o;
        return Objects.equals(numSsd, that.numSsd) &&
                Objects.equals(num7200Disks, that.num7200Disks) &&
                Objects.equals(num10kDisks, that.num10kDisks) &&
                Objects.equals(num15kDisks, that.num15kDisks) &&
                Objects.equals(numVSeriesDisks, that.numVSeriesDisks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSsd, num7200Disks, num10kDisks, num15kDisks, numVSeriesDisks);
    }

    @Override
    public String toString() {
        return "DiskTypeInfoRepoDTO{" +
                "numSsd=" + numSsd +
                ", num7200Disks=" + num7200Disks +
                ", num10kDisks=" + num10kDisks +
                ", num15kDisks=" + num15kDisks +
                ", numVSeriesDisks=" + numVSeriesDisks +
                '}';
    }
}
