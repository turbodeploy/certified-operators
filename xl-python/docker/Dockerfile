FROM registry.access.redhat.com/ubi8

RUN dnf install -y python3
RUN python3 -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY generate.py ./

ENTRYPOINT [ "python3", "generate.py" ]
CMD  [ "--help" ]
