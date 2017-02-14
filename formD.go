package filings

import (
	"encoding/csv"
	"encoding/xml"
	"strings"
)

// FormDSubmission represents some fields of an SEC Form D filing
type FormDSubmission struct {
	XMLName       xml.Name `xml:"edgarSubmission"`
	Issuer        Issuer   `xml:"primaryIssuer"`
	RelatedPeople []Person `xml:"relatedPersonsList>relatedPersonInfo"`
	Offering      Offering `xml:"offeringData"`
}

// Offering captures certain details of the offeringData section of a Form D submission
type Offering struct {
	XMLName        xml.Name `xml:"offeringData`
	Industry       string   `xml:"industryGroup>industryGroupType"`
	SignatoryName  string   `xml:"signatureBlock>signature>nameOfSigner"`
	SignatoryTitle string   `xml:"signatureBlock>signature>signatureTitle"`
}

// Person captures informatino about a key person associated with a Form D filing
type Person struct {
	XMLName       xml.Name `xml:"relatedPersonInfo"`
	Name          Name     `xml:"relatedPersonName"`
	Address       Address  `xml:"relatedPersonAddress"`
	Relationships []string `xml:"relatedPersonRelationshipList>relationship"`
}

type Name struct {
	First string `xml:"firstName"`
	Last  string `xml:"lastName"`
}

type Issuer struct {
	XMLName             xml.Name `xml:"primaryIssuer"`
	CIK                 string   `xml:"cik"`
	Name                string   `xml:"entityName"`
	Address             Address  `xml:"issuerAddress"`
	Phone               string   `xml:"issuerPhoneNumber"`
	PreviousNames       string   `xml:"issuerPreviousNameList>previousName"`
	EntityType          string   `xml:"entityType"`
	YearOfIncorporation string   `xml:"yearOfInc>Value"`
}

type Address struct {
	Street1   string `xml:"street1"`
	City      string `xml:"city"`
	StateAbbr string `xml:"stateOrCountry"`
	State     string `xml:"stateOrCountryDescription"`
	ZIP       string `xml:"zipCode"`
}

func (d FormDSubmission) ToCSV(w *csv.Writer) (err error) {
	for _, p := range d.RelatedPeople {
		err = w.Write([]string{d.Issuer.CIK, d.Issuer.Name, d.Issuer.EntityType, d.Offering.Industry, p.Name.First, p.Name.Last, strings.Join(p.Relationships, ", "), p.Address.Street1, p.Address.City, p.Address.State, p.Address.ZIP})
		if err != nil {
			return
		}
	}
	return
}

func FormDHeadingsToCSV(w *csv.Writer) error {
	return w.Write([]string{
		"CIK",
		"Company Name",
		"Legal Form",
		"Industry",
		"Key Person First Name",
		"Key Person Last Name",
		"Key Person Role(s)",
		"Key Person Street 1",
		"Key Person City",
		"Key Person State",
		"Key Person Zip",
	})
}
