package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

/**
 * Class that encapsulates the DatabaseInfo data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class DatabaseInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String edition;

    private String engine;

    private String licenseModel;

    private String deploymentType;

    private String version;


    public String getEdition() {
        return edition;
    }

    public void setEdition(final String edition) {
        this.edition = edition;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(final String engine) {
        this.engine = engine;
    }

    public String getLicenseModel() {
        return licenseModel;
    }

    public void setLicenseModel(final String licenseModel) {
        this.licenseModel = licenseModel;
    }

    public String getDeploymentType() {
        return deploymentType;
    }

    public void setDeploymentType(final String deploymentType) {
        this.deploymentType = deploymentType;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasDatabase()) {
            return;
        }
        final DatabaseInfo databaseInfo = typeSpecificInfo.getDatabase();
        if (databaseInfo.hasEdition()) {
            setEdition(databaseInfo.getEdition().name());
        }
        if (databaseInfo.hasEngine()) {
            setEngine(databaseInfo.getEngine().name());
        }
        if (databaseInfo.hasLicenseModel()) {
            setLicenseModel(databaseInfo.getLicenseModel().name());
        }
        if (databaseInfo.hasDeploymentType()) {
            setDeploymentType(databaseInfo.getDeploymentType().name());
        }
        if (databaseInfo.hasVersion()) {
            setVersion(databaseInfo.getVersion());
        }
        serviceEntityRepoDTO.setDatabaseInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final DatabaseInfo.Builder databaseInfoBuilder = DatabaseInfo.newBuilder();
        if (getEdition() != null) {
            databaseInfoBuilder.setEdition(DatabaseEdition.valueOf(getEdition()));
        }
        if (getEngine() != null) {
            databaseInfoBuilder.setEngine(DatabaseEngine.valueOf(getEngine()));
        }
        if (getLicenseModel() != null) {
            databaseInfoBuilder.setLicenseModel(LicenseModel.valueOf(getLicenseModel()));
        }
        if (getDeploymentType() != null) {
            databaseInfoBuilder.setDeploymentType(DeploymentType.valueOf(getDeploymentType()));
        }
        if (getVersion() != null) {
            databaseInfoBuilder.setVersion(getVersion());
        }
        return TypeSpecificInfo.newBuilder()
            .setDatabase(databaseInfoBuilder)
            .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseInfoRepoDTO)) return false;
        final DatabaseInfoRepoDTO that = (DatabaseInfoRepoDTO) o;
        return Objects.equal(edition, that.edition) &&
            Objects.equal(engine, that.engine) &&
            Objects.equal(licenseModel, that.licenseModel) &&
            Objects.equal(deploymentType, that.deploymentType) &&
            Objects.equal(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(edition, engine, licenseModel, deploymentType, version);
    }

    @Override
    public String toString() {
        return "DatabaseInfoRepoDTO{" +
                "edition='" + edition + '\'' +
                ", engine='" + engine + '\'' +
                ", licenseModel='" + licenseModel + '\'' +
                ", deploymentType='" + deploymentType + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
