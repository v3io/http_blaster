package data_generator

import (
	"crypto/rand"
	"encoding/base64"
	"github.com/brianvoe/gofakeit"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"math"
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
	CreditCardNumber int
	HackerPhrase     string
	JobTitle         string
	Password         string
	CurrencyShort    string
}

func randomBase64String(l int) string {
	buff := make([]byte, int(math.Round(float64(l)/float64(1.33333333333))))
	rand.Read(buff)
	str := base64.RawURLEncoding.EncodeToString(buff)
	return str[:l] // strip 1 extra character we get from odd length results
}

func (self *Fake) GenerateRandomData() string {
	self.Key = randomBase64String(16)
	gofakeit.Seed(0)
	self.Name = gofakeit.Name()                                         // Markus Moen
	self.Email = gofakeit.Email()                                       // alaynawuckert@kozey.biz
	self.Phone = gofakeit.Phone()                                       // (570)245-7485
	self.BS = gofakeit.BS()                                             // front-end
	self.BeerName = gofakeit.BeerName()                                 // Duvel
	self.Color = gofakeit.Color()                                       // MediumOrchid
	self.Company = gofakeit.Company()                                   // Moen, Pagac and Wuckert
	self.CreditCardNumber = gofakeit.CreditCardNumber()                 // 4287271570245748
	self.HackerPhrase = gofakeit.HackerPhrase()                         // Connecting the array won't do anything, we need to generate the haptic COM driver!
	self.JobTitle = gofakeit.JobTitle()                                 // Director
	self.Password = gofakeit.Password(true, true, true, true, true, 32) // WV10MzLxq2DX79w1omH97_0ga59j8!kj
	self.CurrencyShort = gofakeit.CurrencyShort()
	//return self.ToJsonString()// USD
	return ""
}

func (self *Fake) ConvertToIgzEmdItemJson() string {
	emdItem := igz_data.IgzEmdItem{}
	emdItem.ToJsonString()
	emdItem.InitItem()
	emdItem.InsertKey("name", igz_data.T_STRING, self.Key)
	emdItem.InsertItemAttr("Name", igz_data.T_STRING, self.Name)
	log.Info(emdItem.ToJsonString())
	return emdItem.ToJsonString()
}
