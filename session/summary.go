package session

import "strings"

// SummaryTitleRunes bounds Summary.Title; clients render it as-is.
const SummaryTitleRunes = 60

// Summary is the list view of a Session. Stores may persist it beside the log
// so listing never replays events; it is derived state, never an execution fact.
type Summary struct {
	ID              string `json:"id"`
	ProjectID       int64  `json:"project_id"`
	Status          string `json:"status"`
	Sequence        uint64 `json:"sequence"`
	CancelRequested bool   `json:"cancel_requested,omitempty"`
	TurnCount       int    `json:"turn_count"`
	Title           string `json:"title"`
	// TitleImage marks a first user message that carried only images, so
	// clients can label it without the message itself.
	TitleImage bool `json:"title_image,omitempty"`
}

// Summarize derives the Summary of a projection. The title is the first user
// message with whitespace collapsed, cut to SummaryTitleRunes.
func Summarize(p Projection) Summary {
	s := Summary{ID: p.ID, ProjectID: p.ProjectID, Status: p.Status, Sequence: p.Sequence, CancelRequested: p.CancelRequested, TurnCount: len(p.Turns)}
	for _, m := range p.History {
		if m.Role != "user" {
			continue
		}
		title := []rune(strings.Join(strings.Fields(m.Content), " "))
		if len(title) > SummaryTitleRunes {
			title = title[:SummaryTitleRunes]
		}
		s.Title = string(title)
		s.TitleImage = s.Title == "" && len(m.Images) > 0
		break
	}
	return s
}
