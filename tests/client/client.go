package main

import (
	"context"
	"fmt"
	"log"
	"sonekKV/client"
	"sync"
	"time"
)

func main()  {
	cli, err := client.New(client.Config{
		Endpoint: "127.0.0.1:2379",
		DialTimeout: 20 * time.Second,
	})
	if err != nil {
		log.Println(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i:=0;i<10;i++{
			_, err := cli.Put(ctx, "foo", fmt.Sprintf("test--%d", i))
			if err != nil {
				log.Println(err)
				return
			}
		}
		log.Println("put success")
	}()
	resp := cli.Watch(ctx, "foo")
	for ch := range resp {
		log.Printf("respose %v", ch.Event)
	}

	wg.Wait()
}