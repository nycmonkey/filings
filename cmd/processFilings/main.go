package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"

	"golang.org/x/text/transform"

	"encoding/csv"
	"encoding/json"

	"bytes"

	"unicode"

	"github.com/nycmonkey/filings"
)

type Filing struct {
	CIK    string `json:"cik"`
	Source string `json:"source"`
	Hash   string `json:"hash"`
	Text   string `json:"text"`
}

var (
	letters = regexp.MustCompile(`[A-z]`)
	isNoise = func(r rune) bool {
		switch r {
		case '*', '_', '☐':
			return true
		case '(', ')', '.', ',', '\'', '"', '\n', '/':
			return false
		}
		return !unicode.In(r, unicode.L, unicode.Nd, unicode.Nl, unicode.Z, unicode.P)
	}
	isMn = func(r rune) bool {
		return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
	}
)

func makeHandler(formType string, w io.Writer, maxIter int, idx *filings.FilingIndex) func(*filings.Filing, error) bool {
	var i int
	return func(f *filings.Filing, err error) bool {
		var r io.Reader
		r, err = idx.GetPlainText(f.Hash)
		if err != nil {
			log.Println(f.Hash+":", err)
			return false
		}
		t := transform.Chain(transform.RemoveFunc(isNoise))
		r = transform.NewReader(r, t)
		var cleaned [][]byte
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			if letters.Match(scanner.Bytes()) {
				cleaned = append(cleaned, bytes.Join(bytes.Fields(scanner.Bytes()), []byte(" ")))
			}
		}
		if err = scanner.Err(); err != nil {
			log.Println(err)
			return false
		}
		js, err := json.Marshal(Filing{CIK: f.CIK, Hash: f.Hash, Text: string(bytes.Join(cleaned, []byte("\n")))})
		if err != nil {
			fmt.Println(err)
			return true
		}
		_, err = w.Write(js)
		if err != nil {
			fmt.Println(err)
			return true
		}
		_, err = fmt.Fprintln(w, "")
		if err != nil {
			fmt.Println(err)
			return true
		}
		i++
		if i >= maxIter {
			return true
		}
		return false
	}
}

func makeFormDHandler(idx *filings.FilingIndex, output io.Writer) func(*filings.Filing, error) bool {
	w := csv.NewWriter(output)
	err := filings.FormDHeadingsToCSV(w)
	if err != nil {
		log.Fatalln(err)
	}
	return func(meta *filings.Filing, err error) (stop bool) {
		if err != nil {
			log.Println(err)
			return true
		}
		var d *filings.FormDSubmission
		d, err = idx.ParseFormD(meta.Hash)
		if err != nil {
			log.Println(err)
			return false
		}
		err = d.ToCSV(w)
		if err != nil {
			return true
		}
		w.Flush()
		return false
	}
}

func main() {
	formType := flag.String("f", "10-K", "form type to download")
	dataDir := flag.String("d", "data", "data directory")
	// tikaURL := flag.String("t", "http://localhost:9998", "tika endpoint")
	output := flag.String("o", "10-K.json", "output file")
	max := flag.Int("m", 10000000, "maximum number of filings to process")
	flag.Parse()

	idx, err := filings.NewIndex(*dataDir)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("opened index")

	defer idx.Close()
	out, err := os.Create(*output)
	if err != nil {
		log.Fatalln(err)
	}
	switch *formType {
	case "D":
		idx.MostRecentOfType(*formType, makeFormDHandler(idx, out))
	default:
		idx.MostRecentOfType(*formType, makeHandler(*formType, out, *max, idx))
	}

}
