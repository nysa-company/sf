package cursorprovider

import (
	"strings"
	"testing"
)

func TestCatalogDisplayIsExactUniqueAndBounded(t *testing.T) {
	model := "gpt-5.6-luna-low"
	for _, suffix := range []string{"", " (default)", " (current)", " (current, default)"} {
		label, err := CatalogDisplay([]byte("Available models\n\n"+model+" - Qualified label"+suffix+"\n"), model)
		if err != nil || label != "Qualified label" {
			t.Fatal("documented shape rejected", err)
		}
	}
	for _, output := range []string{
		model, model + " - ", model + " - label\n" + model + " - other", model + "-high - label", "prefix " + model + " - label",
		model + " - bad\x1blabel", model + " - label\r\n", model + " - label\t", strings.Repeat("x", (64<<10)+1),
	} {
		if _, err := CatalogDisplay([]byte(output), model); err == nil {
			t.Fatal("malformed catalog accepted")
		}
	}
	if _, err := CatalogDisplay([]byte("auto - Auto"), "auto"); err == nil {
		t.Fatal("auto admitted")
	}
}

func TestSessionDisplayBindsMeasuredLunaContext(t *testing.T) {
	got, err := SessionDisplay("gpt-5.6-luna-low", "GPT-5.6 Luna 1M Low")
	if err != nil || got != "GPT-5.6 Luna 272K Low" {
		t.Fatal("measured session mapping rejected")
	}
	for _, changed := range []string{"GPT-5.6 Luna 272K Low", "GPT-5.6 Luna 1M High", "GPT-5.6 Luna 1M Low Fast", "", "GPT-5.6 Luna 1M Low\n"} {
		if _, err := SessionDisplay("gpt-5.6-luna-low", changed); err == nil {
			t.Fatal("changed catalog silently accepted")
		}
	}
	if FixtureDigest("gpt-5.6-luna-low", got) == FixtureDigest("gpt-5.6-luna-low", "GPT-5.6 Luna 1M Low") {
		t.Fatal("context label not bound")
	}
}

func TestSessionDisplayBindsMeasuredSonnetContextAndThinking(t *testing.T) {
	const model = "claude-sonnet-5-low"
	got, err := SessionDisplay(model, "Claude Sonnet 5 1M Low")
	if err != nil || got != "Claude Sonnet 5 300K Low No Thinking" {
		t.Fatal("measured session identity not bound")
	}
	for _, changed := range []string{"Claude Sonnet 5 300K Low No Thinking", "Claude Sonnet 5 1M High", "Claude Sonnet 5 1M Low Thinking", "", "Claude Sonnet 5 1M Low\n"} {
		if _, err := SessionDisplay(model, changed); err == nil {
			t.Fatal("changed catalog admitted")
		}
	}
	if FixtureDigest(model, got) == FixtureDigest(model, "Claude Sonnet 5 1M Low") {
		t.Fatal("session parameters not fixture bound")
	}
}
