package controller

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
	"github.com/boanlab/multus-service/internal/obs"
)

// fakeSync delivers the given envelopes, then blocks until the stream's
// context ends -- like an agent that stays connected until it goes away.
type fakeSync struct {
	grpc.ServerStream
	ctx  context.Context
	envs chan *healthpb.HealthEnvelope
}

func (f *fakeSync) Context() context.Context       { return f.ctx }
func (f *fakeSync) Send(*healthpb.HealthAck) error { return nil }
func (f *fakeSync) Recv() (*healthpb.HealthEnvelope, error) {
	select {
	case e := <-f.envs:
		return e, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

// With producer authentication off there is no agent identity. A stream that
// ends by context cancellation -- which is what the agent's max-session cap
// produces -- must end that stream, not dereference the missing identity and
// crash the controller.
func TestUnauthenticatedStreamEndDoesNotPanic(t *testing.T) {
	ev, _ := obs.NewRecorder("")
	s := &HealthServer{
		Store:       NewHealthStore(5 * time.Second),
		Registry:    NewRegistry(),
		Events:      ev,
		RequireAuth: false,
	}
	ctx, cancel := context.WithCancel(context.Background())
	st := &fakeSync{ctx: ctx, envs: make(chan *healthpb.HealthEnvelope, 1)}
	st.envs <- &healthpb.HealthEnvelope{NodeName: "node-a", AgentInstanceId: "i1", Sequence: 1}

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Sync panicked when the stream ended: %v", r)
				done <- nil
			}
		}()
		done <- s.Sync(st)
	}()
	time.Sleep(50 * time.Millisecond) // let the first envelope be adopted
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sync did not return after the stream context ended")
	}
}
