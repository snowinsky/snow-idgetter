FROM ubuntu:latest
LABEL authors="snow"

ENTRYPOINT ["top", "-b"]