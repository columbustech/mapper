FROM golang:1.13.8
WORKDIR $GOPATH/src/github.com/columbustech/mapper
COPY api .
#RUN go build
#CMD ./profiler
CMD ["sh", "-c", "tail -f /dev/null"]
