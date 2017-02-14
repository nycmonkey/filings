package filings

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"

	"strings"

	"github.com/Gelembjuk/articletext"
	_ "github.com/mattn/go-sqlite3"
)

var (
	baseURL               = `https://www.sec.gov/Archives/`
	errObjectIDTooShort   = errors.New(`objectID must be more than two characters`)
	createFilingsTableSQL = `
    CREATE TABLE IF NOT EXISTS 'filings' (
        'id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'cik' CHAR(10) NOT NULL,
        'filed_on' DATE NOT NULL,
        'content_sha256' CHAR(64) NOT NULL,
        'form' VARCHAR(20) NOT NULL,
        'source_path' VARCHAR(64) UNIQUE NOT NULL       
    );`
	createIndexesSQL = []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS 'source_path_idx' ON 'filings' ('source_path')`,
		`CREATE INDEX IF NOT EXISTS 'hash_idx' ON 'filings' ('content_sha256')`,
		`CREATE INDEX IF NOT EXISTS 'form_idx' ON 'filings' ('form')`,
		`CREATE INDEX IF NOT EXISTS 'filed_on_idx' ON 'filings' ('filed_on')`,
	}
	checkExistsSQL  = `SELECT 'content_sha256' FROM 'filings' WHERE 'source_path' = ?`
	latestFormQuery = `SELECT f.cik, f.form, f.filed_on, f.content_sha256, f.source_path
    FROM (
        SELECT cik, MAX(filed_on) as MaxFiledOn
        FROM filings
        WHERE form LIKE ?
        GROUP BY cik
    ) as x
    INNER JOIN filings f on f.cik = x.cik and f.filed_on = x.MaxFiledOn and f.form LIKE ?`
	badChars     = regexp.MustCompile(`[\pM\pC]`)
	htmlPattern  = regexp.MustCompile(`(?is:(<html>.+?</html>))`)
	textPattern  = regexp.MustCompile(`(?is:(<text>.+?</text>))`)
	formDPattern = regexp.MustCompile(`(?is:(<edgarSubmission>.+</edgarSubmission>))`)
	errNotHTML   = errors.New("invalid HTML")
	tagPattern   = regexp.MustCompile(`<[/A-z].*?>`)
)

type FilingIndex struct {
	db      *sql.DB
	store   ObjectStore
	insrt   *sql.Stmt
	baseURL string
}

func (i FilingIndex) bucketFor(cik, form, filed_on string) string {
	return "edgar"
}

func tryJustText(data []byte) (text string, err error) {
	m := textPattern.FindSubmatch(data)
	if m == nil {
		return string(data), nil
	}
	return string(tagPattern.ReplaceAllLiteral(m[1], []byte(""))), nil
}

func extractText(data []byte) (text string, err error) {
	// to do: fix embedded document with SGML tags
	// have seen plain text contain <TEXT></TEXT>  that jumps right into the HTML body (no html, head or body tags)
	m := htmlPattern.FindSubmatch(data)
	if m == nil {
		return tryJustText(data)
	}
	return articletext.GetArticleText(bytes.NewReader(m[1]))
}

func (i FilingIndex) Put(cik string, remotePath string, form string, filed_on string) (err error) {
	var hash string
	bucket := i.bucketFor(cik, form, filed_on)
	err = i.db.QueryRow(checkExistsSQL, remotePath).Scan(&hash)
	if err != nil && err != sql.ErrNoRows {
		return
	}
	if err == nil {
		if i.store.Exists(bucket, hash) {
			return
		}
		var data io.ReadCloser
		var hash2 string
		data, hash2, err = i.fetchFiling(remotePath)
		if err != nil {
			return
		}
		if hash != hash2 {
			return errors.New("Expected hash of " + remotePath + " to be " + hash + ", but got " + hash2)
		}
		return i.store.Put(bucket, hash, data)
	}
	var data io.ReadCloser
	data, hash, err = i.fetchFiling(remotePath)
	if err != nil {
		return
	}
	if err = i.store.Put(bucket, hash, data); err != nil {
		return
	}
	_, err = i.insrt.Exec(cik, filed_on, hash, form, remotePath)
	return
}

func (i FilingIndex) GetSource(hash string) (io.ReadCloser, error) {
	return i.store.Get(`edgar`, hash)
}

func (i FilingIndex) ParseFormD(hash string) (d *FormDSubmission, err error) {
	var r io.ReadCloser
	r, err = i.store.Get(`edgar`, hash)
	if err != nil {
		return
	}
	var data []byte
	data, err = ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return
	}
	m := formDPattern.FindSubmatch(data)
	if len(m) < 2 {
		err = errors.New("unexpected format")
	}
	d = new(FormDSubmission)
	err = xml.NewDecoder(bytes.NewReader(m[1])).Decode(d)
	return
}

func (i FilingIndex) GetPlainText(hash string) (rc io.Reader, err error) {
	var data io.ReadCloser
	data, err = i.store.Get(`edgar`, hash)
	if err != nil {
		return
	}
	defer data.Close()
	raw, err := ioutil.ReadAll(data)
	if err != nil {
		return
	}
	text, err := extractText(raw)
	if err != nil {
		return
	}
	rc = strings.NewReader(text)
	return
	/*
		s := bufio.NewScanner(data)
		s.Buffer(make([]byte, 0, 256*1024), 4096*1024)
		var inTextNode bool
		doc := new(bytes.Buffer)
		for s.Scan() {
			if inTextNode {
				if bytes.Contains(s.Bytes(), []byte(`</TEXT>`)) {
					break
				}
				_, err = doc.Write(badChars.ReplaceAll(s.Bytes(), []byte{}))
				if err != nil {
					return
				}
				_, err = doc.WriteRune('\n')
				if err != nil {
					return
				}
				continue
			}
			if bytes.Contains(s.Bytes(), []byte(`<TEXT>`)) {
				inTextNode = true
			}
		}
		if err = s.Err(); err != nil {
			return
		}
		var contentType string
		contentType, err = i.gika.DetectType(bytes.NewReader(doc.Bytes()), "10-k")
		if err != nil {
			return
		}
		var text []byte
		text, err = i.gika.Parse(bytes.NewReader(doc.Bytes()), contentType)
		if err != nil {
			return
		}
		rc = ioutil.NopCloser(bytes.NewReader(text))
		return
	*/
}

type Filing struct {
	FiledOn  string
	CIK      string
	FormType string
	Hash     string
	Source   string
}

type filingHandler func(f *Filing, err error) (stop bool)

func (i FilingIndex) MostRecentOfType(form string, h filingHandler) {
	var cik, filedOn, formType, hash, source string
	rows, err := i.db.Query(latestFormQuery, form+"%", form+"%")
	if err != nil {
		h(nil, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&cik, &formType, &filedOn, &hash, &source)
		if err != nil {
			h(nil, err)
			return
		}
		stop := h(&Filing{FiledOn: filedOn, CIK: cik, FormType: formType, Hash: hash, Source: source}, nil)
		if stop {
			break
		}
	}
}

func (i FilingIndex) fetchFiling(remotePath string) (data io.ReadCloser, hash string, err error) {
	var resp *http.Response
	resp, err = http.Get(i.baseURL + remotePath)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = errors.New("unexpected response code while fetching data: " + resp.Status)
		return
	}
	var buf bytes.Buffer
	h := sha256.New()
	w := io.MultiWriter(&buf, h)
	r := io.TeeReader(resp.Body, w)
	_, err = ioutil.ReadAll(r)
	if err != nil {
		return
	}
	hash = hex.EncodeToString(h.Sum(nil))
	data = ioutil.NopCloser(&buf)
	return
}

func (i FilingIndex) Close() error {
	i.insrt.Close()
	return i.db.Close()
}

func NewIndex(dir string) (idx *FilingIndex, err error) {
	db, err := sql.Open("sqlite3", filepath.Join(dir, `edgar_filings.db`))
	if err != nil {
		err = errors.New("opening database: " + err.Error())
		return
	}

	_, err = db.Exec(createFilingsTableSQL)
	if err != nil {
		err = errors.New("creating DB table: " + err.Error())
		db.Close()
		return
	}

	for _, stmt := range createIndexesSQL {
		_, err = db.Exec(stmt)
		if err != nil {
			err = errors.New("creating database indices: " + err.Error())
			db.Close()
			return
		}
	}
	insert, err := db.Prepare("INSERT INTO filings(cik, filed_on, content_sha256, form, source_path) values(?,?,?,?,?)")
	if err != nil {
		err = errors.New("preparing DB insert statement: " + err.Error())
		db.Close()
		return
	}
	return &FilingIndex{
		db:      db,
		store:   NewFileStore(dir),
		insrt:   insert,
		baseURL: baseURL,
	}, nil
}
