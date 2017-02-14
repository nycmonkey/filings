package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	cikMasterListURL = `https://www.sec.gov/edgar/NYU/cik.coleft.c`
)

var (
	pattern       = regexp.MustCompile(`^(?P<name>.+?)(?:\s*/(?P<tag>[A-Z]+))?(?:\s*):(?P<cik>\d{10}):$`)
	nameDelimiter = `#@#@`
)

type secFiler struct {
	CIK   string
	Names []string
	Tag   string
}

type combinedRecord struct {
	CIK   string
	Names string
	Tag   string
}

func (filer secFiler) Flatten() (c combinedRecord) {
	sort.Strings(filer.Names)
	c.CIK = filer.CIK
	c.Names = strings.Join(filer.Names, nameDelimiter)
	c.Tag = filer.Tag
	return
}

type byCIK []combinedRecord

func (bcn byCIK) Len() int {
	return len(bcn)
}

func (bcn byCIK) Swap(i, j int) {
	bcn[i], bcn[j] = bcn[j], bcn[i]
}

func (bcn byCIK) Less(i, j int) bool {
	return bcn[i].CIK < bcn[j].CIK
}

func main() {
	outPath := flag.String("o", "cik_list.csv", "output path for master CIK list")
	flag.Parse()
	o, err := os.Create(*outPath)
	if err != nil {
		panic(err)
	}
	defer o.Close()
	var records byCIK
	{
		resp, err := http.Get(cikMasterListURL)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		data := make(map[string]*secFiler) // maps CIK codes to the SEC filer, which can have multiple names
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			matches := pattern.FindSubmatch(scanner.Bytes())
			if len(matches) != 4 {
				os.Stderr.Write([]byte(scanner.Text() + " yielded " + strconv.Itoa(len(matches)) + " pattern matches\n"))
				continue
			}
			cik := string(matches[3])
			name := string(matches[1])
			tag := string(matches[2])
			_, ok := data[cik]
			if !ok {
				data[cik] = new(secFiler)
			}
			data[cik].CIK = cik
			data[cik].Names = append(data[cik].Names, name)
			if len(tag) > 0 {
				data[cik].Tag = tag
			}
		}
		if err = scanner.Err(); err != nil {
			panic(err)
		}
		for _, filer := range data {
			records = append(records, filer.Flatten())
		}
		sort.Sort(records)
	}
	w := csv.NewWriter(o)
	w.Write([]string{"CIK", "Names", "Tag"})
	for _, r := range records {
		w.Write([]string{r.CIK, r.Names, r.Tag})
	}
	w.Flush()
}
