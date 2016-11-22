FROM golang
RUN go get github.com/tahmmee/righthand
RUN go build github.com/tahmmee/righthand
ENTRYPOINT ["./righthand"]
