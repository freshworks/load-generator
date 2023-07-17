package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	chartjs "github.com/brentp/go-chartjs"
	"github.com/brentp/go-chartjs/types"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"
)

func httpHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("Handling request: %v %v %v", r.URL, r.Method, r.RemoteAddr)

	switch r.URL.Path {
	case "/":
		fmt.Fprint(w, indexContent)
	case "/print":
		lg.printMetrics(w)
	case "/report":
		rep := getReport()
		j, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			logrus.Error(err)
			fmt.Fprint(w, err)
			return
		}
		fmt.Fprint(w, string(j))
	case "/graphs":
		err := graphs(w, getReport())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Server internal error: %v", err)
			return
		}
	case "/reset":
		lg.reset()
		fmt.Fprint(w, "OK")
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func getReport() *stats.Report {
	if lg.report != nil {
		return lg.report
	}

	return lg.stats.Export()
}

func graphs(w io.Writer, report *stats.Report) error {
	// TODO: Add histogram

	// TODO: Have larger palette
	colors := []*types.RGBA{
		&types.RGBA{102, 194, 165, 220},
		&types.RGBA{250, 141, 98, 220},
		&types.RGBA{141, 159, 202, 220},
		&types.RGBA{230, 138, 195, 220},
	}

	charts := map[string]*chartjs.Chart{}
	for _, r := range report.Results {
		chart, ok := charts[r.Target]
		if !ok {
			chart = &chartjs.Chart{Label: r.Target}
			chart.Options.Responsive = chartjs.False
			chart.Options.Title = &chartjs.Title{Display: chartjs.True, Text: r.Target}
			var err error
			_, err = chart.AddXAxis(chartjs.Axis{Type: chartjs.Linear, Position: chartjs.Bottom,
				ScaleLabel: &chartjs.ScaleLabel{LabelString: "Percentiles", Display: chartjs.True}})
			if err != nil {
				return err
			}
			_, err = chart.AddYAxis(chartjs.Axis{Type: chartjs.Linear, Position: chartjs.Left,
				ScaleLabel: &chartjs.ScaleLabel{LabelString: "Latency (ms)", Display: chartjs.True}})
			if err != nil {
				return err
			}
			charts[r.Target] = chart
		}

		var xys xy
		for _, d := range r.Histogram.Percentiles {
			xys.x = append(xys.x, d.Percentile)
			xys.y = append(xys.y, d.Value)
		}

		subtarget := r.SubTarget
		if q, ok := report.DigestToQuery[subtarget]; ok {
			subtarget = q
		}

		d := chartjs.Dataset{Data: xys, BorderColor: colors[1], Label: subtarget, Fill: chartjs.False,
			PointRadius: 10, PointBorderWidth: 4, BackgroundColor: colors[0]}
		chart.AddDataset(d)
	}

	cs := make([]chartjs.Chart, 0, len(charts))
	for _, c := range charts {
		cs = append(cs, *c)
	}
	sort.SliceStable(cs[:], func(i, j int) bool {
		return cs[i].Label < cs[j].Label
	})

	return chartjs.SaveCharts(w, nil, cs...)
}

var indexContent = `
<head>
  <title>Load Generator</title>
  <style>
    .home-table {
      font-family: sans-serif;
      font-size: medium;
      border-collapse: collapse;
    }

    .home-row:nth-child(even) {
      background-color: #dddddd;
    }

    .home-data {
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
    }

    .home-form {
      margin-bottom: 0;
    }
  </style>
</head>
<body>
  <table class='home-table'>
    <thead>
      <th class='home-data'>Command</th>
      <th class='home-data'>Description</th>
     </thead>
     <tbody>
<tr class='home-row'><td class='home-data'><a href='print'>print</a></td><td class='home-data'>print metrics</td></tr>
<tr class='home-row'><td class='home-data'><a href='report'>report</a></td><td class='home-data'>report of metrics in json</td></tr>
<tr class='home-row'><td class='home-data'><a href='graphs'>graphs</a></td><td class='home-data'>metrics graphs</td></tr>
<tr class='home-row'><td class='home-data'><form action='reset' method='post' class='home-form'><button>reset</button></form></td><td class='home-data'>reset metrics</td></tr>
    </tbody>
  </table>
</body>
`

// satisfy the required interface with this struct and methods.
type xy struct {
	x []float64
	y []float64
	r []float64
}

func (v xy) Xs() []float64 {
	return v.x
}
func (v xy) Ys() []float64 {
	return v.y
}
func (v xy) Rs() []float64 {
	return v.r
}
