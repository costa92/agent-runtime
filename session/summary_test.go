package session

import (
	"strings"
	"testing"
)

func TestSummarizeDerivesListFieldsFromProjection(t *testing.T) {
	p := Projection{
		ID: "s1", Sequence: 9, ProjectID: 3, Status: "cancelled", CancelRequested: true,
		Turns: []Turn{{ID: "t1"}, {ID: "t2"}},
		History: []Message{
			{Role: "system", Content: "hidden"},
			{Role: "user", Content: "  first\n\tquestion  "},
			{Role: "user", Content: "second"},
		},
	}
	got := Summarize(p)
	want := Summary{ID: "s1", ProjectID: 3, Status: "cancelled", Sequence: 9, CancelRequested: true, TurnCount: 2, Title: "first question"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSummarizeTitleTruncatesRunesAndFlagsImageOnlyMessages(t *testing.T) {
	long := strings.Repeat("长", 80)
	if got := Summarize(Projection{History: []Message{{Role: "user", Content: long}}}).Title; got != strings.Repeat("长", SummaryTitleRunes) {
		t.Fatalf("title not truncated by rune: %q", got)
	}
	image := Summarize(Projection{History: []Message{{Role: "user", Images: []ImageRef{{URL: "u"}}}}})
	if image.Title != "" || !image.TitleImage {
		t.Fatalf("image-only message: %+v", image)
	}
	if empty := Summarize(Projection{}); empty.Title != "" || empty.TitleImage {
		t.Fatalf("no user message: %+v", empty)
	}
}
