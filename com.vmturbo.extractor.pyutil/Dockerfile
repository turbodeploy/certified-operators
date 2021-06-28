FROM python:3.6.13-slim
RUN python3 -m pip install pipenv
COPY dbutil dbutil/
COPY Pipfile .
RUN pipenv install
ENTRYPOINT ["/usr/local/bin/pipenv","run"]
