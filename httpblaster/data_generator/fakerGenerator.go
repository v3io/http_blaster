package data_generator

import (
	"crypto/rand"
	"encoding/base64"
	"github.com/brianvoe/gofakeit"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"math"
	"strconv"
	"time"
)

// Create structs with random injected data
type Fake struct {
	Key              string
	Name             string
	Email            string
	Phone            string
	BS               string
	BeerName         string
	Color            string
	Company          string
	CreditCardNumber string
	HackerPhrase     string
	JobTitle         string
	Password         string
	CurrencyShort    string
	Year             string
	Month            string
	Day              string
	Hour             string
}

func randomBase64String(l int) string {
	buff := make([]byte, int(math.Round(float64(l)/float64(1.33333333333))))
	rand.Read(buff)
	str := base64.RawURLEncoding.EncodeToString(buff)
	return str[:l] // strip 1 extra character we get from odd length results
}

func (self *Fake) Init() {
	gofakeit.Seed(0)
}

func (self *Fake) GenerateRandomData() string {
	t := time.Now()
	self.Key = randomBase64String(16)

	self.Name = gofakeit.Name()                                         // Markus Moen
	self.Email = gofakeit.Email()                                       // alaynawuckert@kozey.biz
	self.Phone = gofakeit.Phone()                                       // (570)245-7485
	self.BS = gofakeit.BS()                                             // front-end
	self.BeerName = gofakeit.BeerName()                                 // Duvel
	self.Color = gofakeit.Color()                                       // MediumOrchid
	self.Company = gofakeit.Company()                                   // Moen, Pagac and Wuckert
	self.CreditCardNumber = strconv.Itoa(gofakeit.CreditCardNumber())   // 4287271570245748
	self.HackerPhrase = gofakeit.HackerPhrase()                         // Connecting the array won't do anything, we need to generate the haptic COM driver!
	self.JobTitle = gofakeit.JobTitle()                                 // Director
	self.Password = gofakeit.Password(true, true, true, true, true, 32) // WV10MzLxq2DX79w1omH97_0ga59j8!kj
	self.CurrencyShort = gofakeit.CurrencyShort()
	self.Year = strconv.Itoa(t.Year())
	self.Month = strconv.Itoa(int(t.Month()))
	self.Day = strconv.Itoa(t.Day())
	self.Hour = strconv.Itoa(t.Hour())

	//return self.ToJsonString()// USD
	return ""
}

func (self *Fake) ConvertToIgzEmdItemJson() string {
	emdItem := igz_data.IgzEmdItem{}
	emdItem.ToJsonString()
	emdItem.InitItem()
	emdItem.InsertKey("key", igz_data.T_STRING, self.Key)
	emdItem.InsertItemAttr("Name", igz_data.T_STRING, self.Name)
	emdItem.InsertItemAttr("Email", igz_data.T_STRING, self.Email)
	emdItem.InsertItemAttr("Phone", igz_data.T_STRING, self.Phone)
	emdItem.InsertItemAttr("BS", igz_data.T_STRING, self.BS)
	emdItem.InsertItemAttr("BeerName", igz_data.T_STRING, self.BeerName)
	emdItem.InsertItemAttr("Color", igz_data.T_STRING, self.Color)
	emdItem.InsertItemAttr("Company", igz_data.T_STRING, self.Company)
	emdItem.InsertItemAttr("CreditCardNumber", igz_data.T_NUMBER, self.CreditCardNumber)
	emdItem.InsertItemAttr("HackerPhrase", igz_data.T_STRING, self.HackerPhrase)
	emdItem.InsertItemAttr("JobTitle", igz_data.T_STRING, self.JobTitle)
	emdItem.InsertItemAttr("Password", igz_data.T_STRING, self.Password)
	emdItem.InsertItemAttr("Year", igz_data.T_NUMBER, self.Year)
	emdItem.InsertItemAttr("Month", igz_data.T_NUMBER, self.Month)
	emdItem.InsertItemAttr("Day", igz_data.T_NUMBER, self.Day)
	emdItem.InsertItemAttr("Hour", igz_data.T_NUMBER, self.Hour)
	log.Info(emdItem.ToJsonString())
	return emdItem.ToJsonString()
}
