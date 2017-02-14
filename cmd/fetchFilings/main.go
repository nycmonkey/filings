package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/nycmonkey/filings"
)

func main() {
	idxFile := flag.String("i", "master.idx", "path to decompressed SEC master index file")
	formType := flag.String("f", "10-K", "form type to download")
	includeAmended := flag.Bool("a", false, "exclude amended forms")
	outputDir := flag.String("o", "data", "output directory")
	flag.Parse()

	index, err := os.Open(*idxFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer index.Close()

	idx, err := filings.NewIndex(*outputDir)
	if err != nil {
		log.Fatalln(err)
	}

	scanner := bufio.NewScanner(index)
	var inBody bool
	for scanner.Scan() {
		if inBody {
			fields := strings.Split(scanner.Text(), `|`)
			if len(fields) != 5 {
				panic("expected 5 fields, got " + strconv.Itoa(len(fields)) + "\n")
			}
			if fields[2] == *formType || (*includeAmended && fields[2] == *formType+"/A") {
				fmt.Println("fetching form", fields[2], "about", fields[1], "from", fields[4])
				err = idx.Put(fields[0], fields[4], fields[2], fields[3])
				if err != nil {
					panic(err)
				}
			}
			continue
		}
		if strings.HasPrefix(scanner.Text(), "-----") {
			inBody = true
		}
	}
	if err = scanner.Err(); err != nil {
		panic(err)
	}
}
