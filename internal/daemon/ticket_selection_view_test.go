package daemon

import (
	"context"
	"encoding/json"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
	"os"
	"testing"
)

func TestTicketListViewIncludesSelectionIdentityAndTitle(t *testing.T) {
	ticket := store.Ticket{Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-one"}, Title: "Count items", State: domain.StateQueued}
	view := ticketView(ticket)
	if view["title"] != ticket.Title || view["channel"] != ticket.Ref.Channel || view["project"] != ticket.Ref.Project || view["ticket"] != ticket.Ref.Ticket || view["state"] != ticket.State {
		t.Fatalf("view: %+v", view)
	}
}

func TestStatusInventoryCarriesTitlesForCLIPicker(t *testing.T) {
	d, _, _ := testDaemon(t)
	submitted := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "picker-submit", Method: "ticket.submit", OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","source":"# Count items\n\nCount the items without modifying them.\n"}`)})
	if !submitted.OK {
		t.Fatalf("submit: %+v", submitted)
	}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "picker-titles", Method: "ticket.status", OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","watch":false}`)})
	var inventory struct {
		Tickets []struct {
			ID    string `json:"ticket"`
			Title string `json:"title"`
		} `json:"tickets"`
	}
	if !response.OK || json.Unmarshal(response.Data, &inventory) != nil || len(inventory.Tickets) != 1 || inventory.Tickets[0].ID == "" || inventory.Tickets[0].Title != "Count items" {
		t.Fatalf("inventory: %s", response.Data)
	}
}
