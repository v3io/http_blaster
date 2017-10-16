package tui

import (
	"strconv"
	"runtime"
	ui "github.com/gizak/termui"
	"github.com/v3io/http_blaster/httpblaster/config"
	"fmt"
	"time"
	"sync"
)

type Term_ui struct {
	cfg *config.TomlConfig
	widget_title ui.GridBufferer
	widget_sys_info  ui.GridBufferer
	widget_server_info  ui.GridBufferer
	widget_put_iops_chart  *ui.LineChart
	widget_get_iops_chart  *ui.LineChart
	widget_logs  *ui.List
	widget_progress  ui.GridBufferer
	widget_request_bar_chart  *ui.BarChart
	widget_latency  *ui.BarChart

	iops_get_fifo *Float64Fifo
	iops_put_fifo *Float64Fifo
	logs_fifo *StringsFifo
	statuses map[int]uint64
	M sync.RWMutex
}

type StringsFifo struct {
	string
	Length int
	Items []string
}

func (self *StringsFifo)Init(length int){
	self.Length = length
	self.Items = make([]string, 10)
}

func (self *StringsFifo)Insert(msg string){
	if len(self.Items) < self.Length{
		self.Items = append(self.Items, msg)
	}else{
		self.Items = self.Items[1:]
		self.Items = append(self.Items, msg)
	}
}
func(self *StringsFifo)Get()[]string{
	return self.Items
}

type Float64Fifo struct {
	int
	Length int
	index int
	Items []float64
}

func (self *Float64Fifo)Init(length int){
	self.Length = length
	self.index = 0
	self.Items = make([]float64, length)
}

func (self *Float64Fifo)Insert(i float64){
	if self.index < self.Length{
		self.Items[self.index] = i
		self.index ++
	}else{
		self.Items = self.Items[1:]
		self.Items = append(self.Items, i)
	}
}
func(self *Float64Fifo)Get()[]float64{
	return self.Items
}

func (self *Term_ui)ui_set_title(x,y,w,h int)  (ui.GridBufferer){
	ui_titile_par := ui.NewPar("Running " +self.cfg.Title + " : PRESS q TO QUIT")
	ui_titile_par.Height = 3
	ui_titile_par.X = x
	ui_titile_par.Y = y
	ui_titile_par.Width = 50
	ui_titile_par.TextFgColor = ui.ColorWhite
	ui_titile_par.BorderLabel = "Title"
	ui_titile_par.BorderFg = ui.ColorCyan
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		// press q to quit
		ui.StopLoop()
	})
	ui.Render(ui_titile_par)
	return ui_titile_par
}

func (self *Term_ui)ui_set_servers_info(x,y,w,h int) (ui.GridBufferer){
	table1 := ui.NewTable()
	var rows [][]string
	rows = append(rows,[]string{"Server", "Port", "TLS Mode"})
	table1.Height += 1
	if len(self.cfg.Global.Servers)==0 {
		rows = append(rows, []string{self.cfg.Global.Server,
			self.cfg.Global.Port,
			strconv.FormatBool(self.cfg.Global.TLSMode)})
		table1.Height += 2
	}else{
		for _,s := range self.cfg.Global.Servers{
			rows = append(rows, []string{s,
				self.cfg.Global.Port,
				strconv.FormatBool(self.cfg.Global.TLSMode)})
			table1.Height += 2
		}
	}

	table1.Rows = rows
	table1.FgColor = ui.ColorWhite
	table1.BgColor = ui.ColorDefault
	table1.Y = y
	table1.X = x
	table1.Width = w
	table1.BorderLabel = "Servers"
	return table1
}

func (self *Term_ui)ui_set_system_info(x,y,w,h int) (ui.GridBufferer){
	table1 := ui.NewTable()
	var rows [][]string
	var mem_stat runtime.MemStats
	runtime.ReadMemStats(&mem_stat)
	rows = append(rows,[]string{"OS", "CPU's", "Memory"})
	rows = append(rows,[]string{runtime.GOOS, strconv.Itoa(runtime.NumCPU()), strconv.FormatInt(int64(mem_stat.Sys),10)})
	table1.Rows = rows
	table1.FgColor = ui.ColorWhite
	table1.BgColor = ui.ColorDefault
	table1.Y = y
	table1.X = x
	table1.Width = w
	table1.Height = h
	table1.BorderLabel = "Syetem Info"
	return table1
}

func (self *Term_ui)ui_set_log_list(x,y,w,h int) (*ui.List){
	list := ui.NewList()
	list.ItemFgColor = ui.ColorYellow
	list.BorderLabel = "Log"
	list.Height = h
	list.Width = w
	list.Y = y
	list.X = x
	list.Items = self.logs_fifo.Get()
	return list
}

func (self *Term_ui)ui_set_progress(x,y,w,h int)  (ui.GridBufferer){
	g := ui.NewGauge()
	g.Percent = 50
	g.Width = w
	g.Height = h
	g.Y = y
	g.X = x
	g.BorderLabel = "Progress"
	g.BarColor = ui.ColorRed
	g.BorderFg = ui.ColorWhite
	g.BorderLabelFg = ui.ColorCyan
	return g
}

func (self *Term_ui)ui_set_requests_bar_chart(x,y,w,h int)  (*ui.BarChart) {
	bc := ui.NewBarChart()
	bc.BarGap=3
	bc.BarWidth=8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "Status codes"
	bc.Data = data
	bc.Width = 50
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}


func (self *Term_ui)ui_set_latency_bar_chart(x,y,w,h int)  (*ui.BarChart) {
	bc := ui.NewBarChart()
	bc.BarGap=3
	bc.BarWidth=8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "Latency ms"
	bc.Data = data
	bc.Width = 50
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}



func (self *Term_ui)ui_get_iops(x,y,w,h int)  (*ui.LineChart) {
	lc2 := ui.NewLineChart()
	lc2.BorderLabel = "Get iops chart"
	lc2.Mode = "braille"

	lc2.Width = 77
	lc2.Height = 16
	lc2.X = x
	lc2.Y = 12
	lc2.AxesColor = ui.ColorWhite
	lc2.LineColor = ui.ColorCyan | ui.AttrBold
	lc2.Data = self.iops_get_fifo.Get()
	return lc2
}


func (self *Term_ui)ui_put_iops(x,y,w,h int) (*ui.LineChart){
	lc2 := ui.NewLineChart()
	lc2.BorderLabel = "Put iops chart"
	lc2.Mode = "braille"
	lc2.Data = self.iops_put_fifo.Get()
	lc2.Width = 77
	lc2.Height = 16
	lc2.X = x
	lc2.Y = 12
	lc2.AxesColor = ui.ColorWhite
	lc2.LineColor = ui.ColorCyan | ui.AttrBold
	return lc2
}

func (self *Term_ui) Update_requests(duration time.Duration, put_count , get_count uint64)  {

	seconds := uint64(duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	put_iops := put_count / seconds
	get_iops := get_count / seconds
	if put_iops > 0 {
		self.iops_put_fifo.Insert(float64(put_iops)/1000)
	}
	if get_iops > 0 {
		self.iops_get_fifo.Insert(float64(get_iops)/1000)
	}
	self.widget_put_iops_chart.Data = self.iops_put_fifo.Get()
	self.widget_get_iops_chart.Data = self.iops_get_fifo.Get()
	ui.Render(self.widget_put_iops_chart, self.widget_get_iops_chart)
	self.logs_fifo.Insert(fmt.Sprintf("Put iops %v", put_iops))
	self.logs_fifo.Insert(fmt.Sprintf("Get iops %v", get_iops))
	self.widget_logs.Items = self.logs_fifo.Get()
	//ui.Render(ui.Body)//ui.Render(self.widget_logs)
}

func (self *Term_ui)Update_status_codes(labels []string, values []int){
	self.widget_request_bar_chart.Data = values
	self.widget_request_bar_chart.DataLabels = labels
	//ui.Render(ui.Body)//ui.Render(self.widget_request_bar_chart)
}


func (self *Term_ui)Update_latency_chart(labels []string, values []int){
	self.widget_latency.Data = values
	self.widget_latency.DataLabels = labels
	//ui.Render(ui.Body)//ui.Render(self.widget_request_bar_chart)
}

func (self *Term_ui)Init_term_ui(cfg *config.TomlConfig){
	self.cfg = cfg
	self.iops_get_fifo = &Float64Fifo{}
	self.iops_get_fifo.Init(150)
	self.iops_put_fifo = &Float64Fifo{}
	self.iops_put_fifo.Init(150)
	self.logs_fifo = &StringsFifo{}
	self.logs_fifo.Init(30)
	self.statuses = make(map[int]uint64)
	err := ui.Init()
	if err != nil {
		panic(err)
	}

	self.widget_title = self.ui_set_title(0,0,128,3)
	self.widget_sys_info = self.ui_set_system_info(0,0,0,5)
	self.widget_server_info = self.ui_set_servers_info(0,0,0,5)
	self.widget_put_iops_chart = self.ui_put_iops(78,0,0,0)
	self.widget_get_iops_chart = self.ui_get_iops(0,0,0,0)
	self.widget_logs = self.ui_set_log_list(0, 0, 155,30)
	self.widget_request_bar_chart = self.ui_set_requests_bar_chart(0,0,0,16)
	self.widget_latency = self.ui_set_latency_bar_chart(0,0,0,30)
	//self.widget_progress = self.ui_set_progress(0, 0, 155, 3)

	ui.Body.AddRows(
		ui.NewRow(
			ui.NewCol(12,0, self.widget_title),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_sys_info),
			ui.NewCol(6,0, self.widget_server_info),
		),
		ui.NewRow(
			ui.NewCol(4,0, self.widget_put_iops_chart),
			ui.NewCol(4,0, self.widget_get_iops_chart),
			ui.NewCol(4,0, self.widget_request_bar_chart),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_logs),
			ui.NewCol(6,0, self.widget_latency),
		),
		//ui.NewRow(
		//	ui.NewCol(12,0, self.widget_progress),
		//),
	)

	ui.Body.Align()
	ui.Render(ui.Body)
	go ui.Loop()
}

func (self *Term_ui)Render()  {
	ui.Render(ui.Body)
}

func (self *Term_ui)Terminate_ui(){
	ui.Close()
}