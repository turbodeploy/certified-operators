FROM registry.access.redhat.com/ubi8/python-36

# Required OpenShift Labels
LABEL name="prometheus-config-manager" \
      vendor="Turbonomic" \
      version="7" \
      release="1" \
      summary="prometheus-config-manager" \
      description="This component is responsible for reading telemetry opt-in setting from settings and updating the prometheus configuration to upload (or not) customer data to data cloud"

# Required Licenses
COPY licenses /licenses

# Copy the requirements that contain libraries to be installed
COPY requirements.txt /

# Required libraries for grpc call
RUN pip3 install --upgrade --no-cache -r /requirements.txt

# Copy python script to be called at the startup
COPY prometheus-config-manager.py /

ENTRYPOINT ["python3.6", "/prometheus-config-manager.py"]
