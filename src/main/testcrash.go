package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"plugin"

	"6.824/mr"
)

// type KeyValue struct {
// 	Key   string
// 	Value string
// }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin(os.Args[1])
	fmt.Println("filename: text.txt")
	fmt.Println("string: abc")
	ret := mapf("text.txt", "abc")
	fmt.Printf("%s \n", ret)
	ofile, _ := os.Create("enc_t.txt")
	enc := json.NewEncoder(ofile)
	for _, kv := range ret {
		enc.Encode(kv)
	}
	ofile.Close()
	ofile, _ = os.Open("enc_t.txt")
	dec := json.NewDecoder(ofile)

	var kv mr.KeyValue
	kva := []mr.KeyValue{}
	for {
		if err := dec.Decode(&kv); err != nil {
			fmt.Printf("%v", err)
			break
		}
		fmt.Printf("%v: %v \n", kv.Key, kv.Value)
		kva = append(kva, kv)
	}
	ret1 := reducef("abc", []string{"abc", "bce", "aaa"})
	fmt.Printf("%s \n", ret1)

}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Println(err)
		log.Printf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Printf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Printf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
