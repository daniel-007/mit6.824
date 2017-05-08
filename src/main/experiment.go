package main

import (
	"errors"
	"net/rpc"
	"net"
	"log"
	"net/http"
	"fmt"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func main() {
	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal(e)
	}
	go http.Serve(l, nil)

	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal(err)
	}

	args := &Args{10, 8}
	var reply Arith
	client.Call("Arith.Multiply", args, &reply)

	fmt.Println(reply)

	var replay Quotient

	doneChan := client.Go("Arith.Divide", args, &replay, nil)
	<- doneChan.Done
	fmt.Println(replay)
}
