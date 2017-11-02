package tui

import (
	"strconv"
	"runtime"
	ui "github.com/gizak/termui"
	"github.com/v3io/http_blaster/httpblaster/config"
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
	widget_put_latency  *ui.BarChart
	widget_get_latency  *ui.BarChart

	iops_get_fifo *Float64Fifo
	iops_put_fifo *Float64Fifo
	logs_fifo *StringsFifo
	statuses map[int]uint64
	M sync.RWMutex
	ch_done chan struct{}
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
	ui_titile_par.Height = h
	ui_titile_par.X = x
	ui_titile_par.Y = y
	ui_titile_par.Width = w
	ui_titile_par.TextFgColor = ui.ColorWhite
	ui_titile_par.BorderLabel = "Title"
	ui_titile_par.BorderFg = ui.ColorCyan
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		// press q to quit
		ui.StopLoop()
		ui.Close()
		close(self.ch_done)
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

func (self *Term_ui)ui_set_requests_bar_chart(x,y,w,h int)  (*ui.BarChart) {
	bc := ui.NewBarChart()
	bc.BarGap=3
	bc.BarWidth=8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "Status codes"
	bc.Data = data
	bc.Width = w
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}


func (self *Term_ui)ui_set_put_latency_bar_chart(x,y,w,h int)  (*ui.BarChart) {
	bc := ui.NewBarChart()
	bc.BarGap=3
	bc.BarWidth=8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "put Latency ms"
	bc.Data = data
	bc.Width = w
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}


func (self *Term_ui)ui_set_get_latency_bar_chart(x,y,w,h int)  (*ui.BarChart) {
	bc := ui.NewBarChart()
	bc.BarGap=3
	bc.BarWidth=8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "get Latency ms"
	bc.Data = data
	bc.Width = w
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

	lc2.Width = w
	lc2.Height = h
	lc2.X = x
	lc2.Y = y
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
	lc2.Width = w
	lc2.Height = h
	lc2.X = x
	lc2.Y = y
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
	self.widget_logs.Items = self.logs_fifo.Get()
}

func (self *Term_ui)Update_status_codes(labels []string, values []int){
	self.widget_request_bar_chart.Data = values
	self.widget_request_bar_chart.DataLabels = labels
}

func (self *Term_ui)Update_put_latency_chart(labels []string, values []int){
	self.widget_put_latency.Data = values
	self.widget_put_latency.DataLabels = labels
}

func (self *Term_ui)Update_get_latency_chart(labels []string, values []int){
	self.widget_get_latency.Data = values
	self.widget_get_latency.DataLabels = labels
}

func Percentage(value, total int) int {
	return value*total/100
}

func (self *Term_ui)Init_term_ui(cfg *config.TomlConfig) chan struct{}{
	self.cfg = cfg
	self.ch_done = make(chan struct{})
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
	term_hight := ui.TermHeight()

	self.widget_title = self.ui_set_title(0,0,50,Percentage(7, term_hight))
	self.widget_server_info = self.ui_set_servers_info(0,0,0,0)
	self.widget_sys_info = self.ui_set_system_info(0,0,0,self.widget_server_info.GetHeight())
	self.widget_put_iops_chart = self.ui_put_iops(0,0,0,Percentage(30, term_hight))
	self.widget_get_iops_chart = self.ui_get_iops(0,0,0,Percentage(30, term_hight))
	self.widget_put_latency = self.ui_set_put_latency_bar_chart(0,0,0,Percentage(30, term_hight))
	self.widget_get_latency = self.ui_set_get_latency_bar_chart(0,0,0,Percentage(30, term_hight))
	self.widget_request_bar_chart = self.ui_set_requests_bar_chart(0,0,0,Percentage(20, term_hight))
	self.widget_logs = self.ui_set_log_list(0, 0, 0,Percentage(20, term_hight))

	ui.Body.AddRows(
		ui.NewRow(
			ui.NewCol(12,0, self.widget_title),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_sys_info),
			ui.NewCol(6,0, self.widget_server_info),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_put_iops_chart),
			ui.NewCol(6,0, self.widget_put_latency),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_get_iops_chart),
			ui.NewCol(6,0, self.widget_get_latency),
		),
		ui.NewRow(
			ui.NewCol(6,0, self.widget_logs),
			ui.NewCol(6,0, self.widget_request_bar_chart),
		),
	)

	ui.Body.Align()
	ui.Render(ui.Body)
	go ui.Loop()
	return self.ch_done
}

func (self *Term_ui)Render()  {
	ui.Render(ui.Body)
}

func (self *Term_ui)Terminate_ui(){
	ui.StopLoop()
	ui.Close()
}

func (self *Term_ui)Write(p []byte) (n int, err error){
	if p == nil{
		return 0, nil
	}
	self.logs_fifo.Insert(string(p))
	if self.widget_logs != nil {
		self.widget_logs.Items = self.logs_fifo.Get()
	}
	return len(p), nil
}