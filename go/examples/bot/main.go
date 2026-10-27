// A Go bot behind a brokapi gateway: connect, subscribe, reconcile, trade, and survive a new gateway process.
//
// The code of the page "Подключить бота" of the book, the Go part. The bot buys one lot far below the market, so that the
// order rests, and follows the order to its end. The strategy is a stub. What a bot behind a gateway needs is here: the
// order of the calls, setup that runs at the start and again when a new gateway process has no memory of the bot, the Gap
// that tells it so, and connecting again when the stream ends.
//
//	go run ./examples/bot -dir brokapi.state/gateways/bcs-main
//
// Without BROKAPI_LIVE=1 the bot only reads and stops before the order: on a real account the order is real. The tests of
// the gateway (crates/gateway/tests/go.rs) run it against the imitation of BCS with BROKAPI_LIVE=1, restart the gateway
// under it and fill the order.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/google/uuid"
	brokapi "github.com/runinfra/brokapi/go"
	gatewayv1 "github.com/runinfra/brokapi/go/gateway/v1"
)

func main() {
	dir := flag.String("dir", "brokapi.state/gateways/bcs-main", "the directory of the gateway: <state_dir>/gateways/<name>")
	name := flag.String("bot", "my-bot", "the name of this bot, as brokapi status shows it")
	ticker := flag.String("ticker", "SBER", "the ticker; \"SBER@TQBR\" names the board too")
	price := flag.String("price", "100.00", "the price of the order: far below the market, so that it rests")
	flag.Parse()

	bot := &Bot{dir: *dir, name: *name, ticker: *ticker, price: *price, ledger: map[string]bool{}, live: os.Getenv("BROKAPI_LIVE") == "1"}
	if err := bot.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// ANCHOR: bot
// Bot holds what it must not lose: the orders it believes are alive (a real bot keeps them in a file and writes the cid
// BEFORE Place) and where it is in the stream of events.
type Bot struct {
	dir, name, ticker, price string
	live                     bool

	conn       *brokapi.Conn
	instrument *gatewayv1.InstrumentId
	ledger     map[string]bool
	cid        string
	ended      gatewayv1.OrderStateKind
	// The epoch of the gateway and the number of the last event handled: Events continues from there.
	epoch, lastSeq uint64
}

// setup tells a gateway what this bot needs: the subscriptions first, then the reconcile. It runs at the start and again
// when a new gateway process took the place of the old one (it holds none of the subscriptions of this bot). The
// subscriptions go BEFORE the reconcile: an event between the snapshot of the reconcile and the subscription would be lost.
func (b *Bot) setup(ctx context.Context) error {
	if _, err := b.conn.Gateway.SubscribeOrderState(ctx, &gatewayv1.SubscribeRequest{}); err != nil {
		return err
	}
	return b.reconcile(ctx)
}

// reconcile compares the ledger with the broker. Until it has run, the gateway refuses to trade (NOT_RECONCILED).
func (b *Bot) reconcile(ctx context.Context) error {
	ledger := make([]string, 0, len(b.ledger))
	for cid := range b.ledger {
		ledger = append(ledger, cid)
	}
	sort.Strings(ledger)
	report, err := b.conn.Gateway.Reconcile(ctx, &gatewayv1.ReconcileRequest{Ledger: ledger})
	if err != nil {
		return err
	}
	for _, o := range report.Confirmed {
		b.onOrder(o)
	}
	// Not active at the broker: it ended while the bot was away. Ask how.
	for _, cid := range report.ClosedOffline {
		o, err := b.conn.Gateway.OrderState(ctx, &gatewayv1.OrderStateRequest{Cid: cid})
		if err != nil {
			return err
		}
		b.onOrder(o)
	}
	return nil
}

func (b *Bot) onOrder(o *gatewayv1.OrderSnapshot) {
	if !b.ledger[o.Cid] {
		return // the order of another bot
	}
	switch k := o.GetState().GetKind(); k {
	case gatewayv1.OrderStateKind_ORDER_STATE_KIND_FILLED, gatewayv1.OrderStateKind_ORDER_STATE_KIND_CANCELLED,
		gatewayv1.OrderStateKind_ORDER_STATE_KIND_REJECTED:
		delete(b.ledger, o.Cid)
		b.ended = k
	}
}

// ANCHOR_END: bot

// ANCHOR: place
// place puts one lot far below the market: the order rests. The cid is in the ledger before the call.
func (b *Bot) place(ctx context.Context) error {
	params, err := b.conn.Gateway.TradingParams(ctx, &gatewayv1.InstrumentRequest{Instrument: b.instrument})
	if err != nil {
		return err
	}
	cid := uuid.NewString()
	b.ledger[cid] = true
	_, err = b.conn.Gateway.Place(ctx, &gatewayv1.PlaceOrderRequest{
		Cid: cid, Instrument: b.instrument, Side: gatewayv1.Side_SIDE_BUY,
		OrderType: gatewayv1.OrderType_ORDER_TYPE_LIMIT, Qty: params.MinQty, Price: &b.price,
	})
	if d := brokapi.Detail(err); d != nil && d.Kind == gatewayv1.ErrorDetail_KIND_UNKNOWN {
		// The answer was lost: the order may stand at the broker. Never send it again, the reconcile will tell.
		fmt.Println("no answer to the order: the reconcile will tell whether it stands")
		b.cid = cid
		return b.reconcile(ctx)
	}
	if err != nil {
		delete(b.ledger, cid) // refused: the order is not at the broker
		return err
	}
	b.cid = cid
	fmt.Println("placed", cid)
	return nil
}

// ANCHOR_END: place

// ANCHOR: loop
// Run connects, sets up, places the order and reads the events until the order has ended. When the stream ends (the
// gateway was stopped or restarted), it connects again and continues from the last event.
func (b *Bot) Run(ctx context.Context) error {
	first := true
	for {
		conn, err := brokapi.Connect(ctx, b.dir, b.name)
		if err != nil {
			return err
		}
		b.conn = conn
		done, err := b.session(ctx, first)
		conn.Close()
		if done || err != nil {
			return err
		}
		first = false
		fmt.Println("the stream ended: connecting again")
	}
}

// session is one connection: open Events, wait for Ready, set up when needed, trade, read. It returns done when the order
// has ended, and false with no error when the stream ended and the bot should connect again.
func (b *Bot) session(ctx context.Context, first bool) (done bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Events first, then the subscriptions: the events between them would be lost the other way round.
	events, err := b.conn.Gateway.Events(ctx, &gatewayv1.EventsRequest{Epoch: b.epoch, LastSeq: b.lastSeq})
	if err != nil {
		return false, err
	}
	frame, err := events.Recv()
	if err != nil {
		return false, err
	}
	ready := frame.GetReady()
	if ready == nil {
		return false, errors.New("the first frame of Events is not Ready")
	}
	if first {
		b.epoch = ready.Epoch
		b.instrument, err = b.conn.Gateway.ResolveInstrument(ctx, &gatewayv1.ResolveInstrumentRequest{Ticker: b.ticker})
		if err != nil {
			return false, err
		}
		if err := b.setup(ctx); err != nil {
			return false, err
		}
		if !b.live {
			fmt.Println("read only: set BROKAPI_LIVE=1 to place the order (on a real account it is real)")
			return true, nil
		}
		if err := b.place(ctx); err != nil {
			return false, err
		}
	}
	// A new connection to the same gateway: the stream replays what the bot missed, nothing else to do. A new gateway
	// process says so with a Gap, read below.

	for b.ended == gatewayv1.OrderStateKind_ORDER_STATE_KIND_UNSPECIFIED {
		frame, err := events.Recv()
		if err != nil {
			// io.EOF or an error: the gateway stopped or is restarting. What was missed, the Gap will tell.
			return false, nil
		}
		switch f := frame.Frame.(type) {
		case *gatewayv1.EventFrame_Event:
			b.lastSeq = f.Event.Seq
			switch p := f.Event.Payload.(type) {
			case *gatewayv1.Event_Order:
				fmt.Println("order", p.Order.Cid, p.Order.GetState().GetKind())
				b.onOrder(p.Order)
			case *gatewayv1.Event_Overflow, *gatewayv1.Event_Reconnected:
				// The connector of the gateway lost events of the broker: the same remedy as for a Gap.
				if err := b.reconcile(ctx); err != nil {
					return false, err
				}
			}
		case *gatewayv1.EventFrame_Gap:
			// Events were lost between the gateway and the bot. A new gateway process (NEW_EPOCH) has no subscriptions
			// of this bot and numbers its events anew.
			if f.Gap.Reason == gatewayv1.Gap_REASON_NEW_EPOCH {
				b.epoch, b.lastSeq = ready.Epoch, 0
				if err := b.setup(ctx); err != nil {
					return false, err
				}
			} else if err := b.reconcile(ctx); err != nil {
				return false, err
			}
			fmt.Printf("gap %s: subscribed again and reconciled\n", f.Gap.Reason)
		case *gatewayv1.EventFrame_Lagged:
			// The bot read too slowly: connect again with the last number, the gateway replays the rest.
			return false, nil
		}
	}
	fmt.Println("the order ended:", b.ended)
	return true, nil
}

// ANCHOR_END: loop
