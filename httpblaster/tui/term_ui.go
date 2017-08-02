package tui

import (
	"strconv"
	"runtime"
	ui "github.com/gizak/termui"
	"github.com/v3io/http_blaster/httpblaster/config"
	"fmt"
	"time"
)

type Term_ui struct {
	cfg *config.TomlConfig
}

func (self *Term_ui)ui_set_title(x,y,w,h int)  int{
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
	return ui_titile_par.Height + y
}

func (self *Term_ui)ui_set_servers_info(x,y,w,h int) int{
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
	table1.Width = 70
	table1.BorderLabel = "Servers"
	ui.Render(table1)
	return table1.Height + y
}

func (self *Term_ui)ui_set_system_info(x,y,w,h int) int{
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
	table1.Width = 57
	table1.Height = 5
	table1.BorderLabel = "Syetem Info"
	ui.Render(table1)
	return table1.Height + y
}

func (self *Term_ui)ui_set_log_list(x,y,w,h int) int{
	list := ui.NewList()
	list.Items = []string{"1", "2"}
	list.ItemFgColor = ui.ColorYellow
	list.BorderLabel = "Log"
	list.Height = h
	list.Width = w
	list.Y = y
	list.X = x
	for i:=0; i<100; i++{
		list.Items = append(list.Items,fmt.Sprintf("[%d] - hello", i))
		time.Sleep(time.Millisecond*100)
		ui.Render(list)
	}
	ui.Render(list)
	return y+h
}

func (self *Term_ui)ui_set_progress(x,y,w,h int)  int{
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
	ui.Render(g)
	return y+h
}

func (self *Term_ui)Init_term_ui(cfg *config.TomlConfig){
	self.cfg = cfg
	err := ui.Init()
	if err != nil {
		panic(err)
	}
	h := self.ui_set_title(0,0,128,3)
	self.ui_set_system_info(71,h,0,0)
	h = self.ui_set_servers_info(0,h,70,0)
	h = self.ui_set_log_list(0, h, 128,30)
	h = self.ui_set_progress(0, h, 128, 3)
	go ui.Loop()
}

func (self *Term_ui)Terminate_ui(){
	ui.Close()
}