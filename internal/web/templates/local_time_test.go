package templates_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/web/templates"
)

func TestLocalTimeAttributes(t *testing.T) {
	t.Parallel()

	value := time.Date(2026, time.September, 7, 13, 49, 57, 493945000, time.FixedZone("CDT", -5*60*60))
	for _, tt := range []struct {
		name     string
		value    time.Time
		options  templates.LocalTimeOptions
		contains []string
		absent   []string
	}{
		{
			name:    "relative freshness",
			value:   value,
			options: templates.LocalTimeOptions{Relative: true},
			contains: []string{
				`datetime="2026-09-07T18:49:57.493945Z"`,
				`title="2026-09-07T18:49:57.493945Z"`,
				`data-local-time-style="time"`,
				`data-local-time-relative="true"`,
			},
		},
		{
			name:    "absolute date and time",
			value:   value,
			options: templates.LocalTimeOptions{Style: templates.LocalDateTimeZone},
			contains: []string{
				`datetime="2026-09-07T18:49:57.493945Z"`,
				`title="2026-09-07T18:49:57.493945Z"`,
				`data-local-time-style="date-time-zone"`,
			},
			absent: []string{"data-local-time-relative"},
		},
		{
			name:     "missing timestamp",
			options:  templates.LocalTimeOptions{Relative: true, Fallback: "--:--:--"},
			contains: []string{"--:--:--"},
			absent:   []string{"<time", "datetime", "data-local-time-relative"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			if err := templates.LocalTime(tt.value, tt.options).Render(context.Background(), &output); err != nil {
				t.Fatal(err)
			}
			for _, want := range tt.contains {
				if !strings.Contains(output.String(), want) {
					t.Errorf("rendered time missing %q: %s", want, output.String())
				}
			}
			for _, unwanted := range tt.absent {
				if strings.Contains(output.String(), unwanted) {
					t.Errorf("rendered time contains %q: %s", unwanted, output.String())
				}
			}
		})
	}
}
