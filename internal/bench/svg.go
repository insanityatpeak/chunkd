package bench

import (
	"fmt"
	"math"
	"strings"
)

// Series is one named set of values: one per category for Bars, one per
// x for Lines.
type Series struct {
	Name string
	Y    []float64
}

var palette = []string{"#1f6f9f", "#d9822b", "#3a9e6b", "#a14b8a", "#7a7a7a"}

const (
	width, height              = 720, 360
	left, right, top, bottom   = 64, 30, 40, 64
	plotW, plotH               = width - left - right, height - top - bottom
	fontFamily                 = "system-ui, sans-serif"
	axisColor, gridColor, text = "#555", "#ddd", "#222"
)

func header(title string) *strings.Builder {
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" font-family="%s" font-size="12">`+"\n", width, height, width, height, fontFamily)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#fff"/>`+"\n", width, height)
	fmt.Fprintf(&b, `<text x="%d" y="22" font-size="14" font-weight="600" fill="%s">%s</text>`+"\n", left, text, esc(title))
	return &b
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

func legend(b *strings.Builder, series []Series) {
	x := left
	for i, s := range series {
		fmt.Fprintf(b, `<rect x="%d" y="%d" width="10" height="10" fill="%s"/><text x="%d" y="%d" fill="%s">%s</text>`+"\n", x, height-18, palette[i%len(palette)], x+14, height-9, text, esc(s.Name))
		x += 14 + 7*len(s.Name) + 18
	}
}

// niceMax rounds up to 1, 2 or 5 times a power of ten.
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	p := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 5, 10} {
		if v <= m*p {
			return m * p
		}
	}
	return 10 * p
}

func fmtNum(v float64) string {
	switch {
	case v >= 1000 && v == math.Trunc(v):
		return fmt.Sprintf("%.0f", v)
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return fmt.Sprintf("%.0f", v)
	case v >= 1:
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.2g", v)
}

// Bars draws grouped bars: one group per category, one bar per series.
func Bars(title, ylabel string, cats []string, series []Series) string {
	b := header(title)
	maxV := 0.0
	for _, s := range series {
		for _, v := range s.Y {
			maxV = math.Max(maxV, v)
		}
	}
	top10 := niceMax(maxV)
	for i := 0; i <= 5; i++ {
		v := top10 * float64(i) / 5
		y := float64(top+plotH) - float64(plotH)*float64(i)/5
		fmt.Fprintf(b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s"/><text x="%d" y="%.1f" text-anchor="end" fill="%s">%s</text>`+"\n", left, y, left+plotW, y, gridColor, left-6, y+4, axisColor, fmtNum(v))
	}
	fmt.Fprintf(b, `<text x="14" y="%d" fill="%s" transform="rotate(-90 14 %d)" text-anchor="middle">%s</text>`+"\n", top+plotH/2, axisColor, top+plotH/2, esc(ylabel))
	group := float64(plotW) / float64(len(cats))
	bar := group * 0.8 / float64(len(series))
	for ci, c := range cats {
		x0 := float64(left) + group*float64(ci) + group*0.1
		for si, s := range series {
			h := float64(plotH) * s.Y[ci] / top10
			x := x0 + bar*float64(si)
			fmt.Fprintf(b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"><title>%s, %s: %s</title></rect>`+"\n", x, float64(top+plotH)-h, bar-1, h, palette[si%len(palette)], esc(s.Name), esc(c), fmtNum(s.Y[ci]))
			fmt.Fprintf(b, `<text x="%.1f" y="%.1f" text-anchor="middle" font-size="10" fill="%s">%s</text>`+"\n", x+bar/2, float64(top+plotH)-h-3, text, fmtNum(s.Y[ci]))
		}
		fmt.Fprintf(b, `<text x="%.1f" y="%d" text-anchor="middle" fill="%s">%s</text>`+"\n", x0+group*0.4, top+plotH+16, text, esc(c))
	}
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s"/>`+"\n", left, top+plotH, left+plotW, top+plotH, axisColor)
	legend(b, series)
	b.WriteString("</svg>\n")
	return b.String()
}

// Lines draws one polyline per series over xs. logX and logY use base-10 axes.
func Lines(title, xlabel, ylabel string, xs []float64, series []Series, logX, logY bool) string {
	b := header(title)
	maxV, minV := 0.0, math.Inf(1)
	for _, s := range series {
		for _, v := range s.Y {
			maxV = math.Max(maxV, v)
			if v > 0 {
				minV = math.Min(minV, v)
			}
		}
	}
	yTop, yLo := niceMax(maxV), 0.0
	if logY {
		yTop, yLo = math.Pow(10, math.Ceil(math.Log10(maxV))), math.Pow(10, math.Floor(math.Log10(minV)))
	}
	ypos := func(v float64) float64 {
		f := v / yTop
		if logY {
			f = (math.Log10(math.Max(v, yLo)) - math.Log10(yLo)) / (math.Log10(yTop) - math.Log10(yLo))
		}
		return float64(top+plotH) - float64(plotH)*f
	}
	xLo, xHi := xs[0], xs[len(xs)-1]
	xpos := func(v float64) float64 {
		f := (v - xLo) / (xHi - xLo)
		if logX {
			f = (math.Log10(v) - math.Log10(xLo)) / (math.Log10(xHi) - math.Log10(xLo))
		}
		return float64(left) + float64(plotW)*f
	}
	if logY {
		for v := yLo; v <= yTop*1.0001; v *= 10 {
			y := ypos(v)
			fmt.Fprintf(b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s"/><text x="%d" y="%.1f" text-anchor="end" fill="%s">%s</text>`+"\n", left, y, left+plotW, y, gridColor, left-6, y+4, axisColor, fmtNum(v))
		}
	} else {
		for i := 0; i <= 5; i++ {
			v := yTop * float64(i) / 5
			y := ypos(v)
			fmt.Fprintf(b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s"/><text x="%d" y="%.1f" text-anchor="end" fill="%s">%s</text>`+"\n", left, y, left+plotW, y, gridColor, left-6, y+4, axisColor, fmtNum(v))
		}
	}
	for _, x := range xs {
		fmt.Fprintf(b, `<text x="%.1f" y="%d" text-anchor="middle" fill="%s">%s</text>`+"\n", xpos(x), top+plotH+16, text, fmtNum(x))
	}
	fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="middle" fill="%s">%s</text>`+"\n", left+plotW/2, top+plotH+32, axisColor, esc(xlabel))
	fmt.Fprintf(b, `<text x="14" y="%d" fill="%s" transform="rotate(-90 14 %d)" text-anchor="middle">%s</text>`+"\n", top+plotH/2, axisColor, top+plotH/2, esc(ylabel))
	for si, s := range series {
		var pts []string
		for i, v := range s.Y {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", xpos(xs[i]), ypos(v)))
		}
		col := palette[si%len(palette)]
		fmt.Fprintf(b, `<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>`+"\n", col, strings.Join(pts, " "))
		for i, v := range s.Y {
			fmt.Fprintf(b, `<circle cx="%.1f" cy="%.1f" r="3" fill="%s"><title>%s, x=%s: %s</title></circle>`+"\n", xpos(xs[i]), ypos(v), col, esc(s.Name), fmtNum(xs[i]), fmtNum(v))
		}
	}
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s"/>`+"\n", left, top+plotH, left+plotW, top+plotH, axisColor)
	legend(b, series)
	b.WriteString("</svg>\n")
	return b.String()
}
