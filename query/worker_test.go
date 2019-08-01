package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
)

var (
	req          = &wire.MsgGetData{}
	progressResp = &wire.MsgTx{
		Version: 111,
	}
	finalResp = &wire.MsgTx{
		Version: 222,
	}
)

type mockPeer struct {
	requests      chan wire.Message
	responses     chan<- wire.Message
	subscriptions chan chan wire.Message
	quit          chan struct{}
}

var _ Peer = (*mockPeer)(nil)

func (m *mockPeer) QueueMessageWithEncoding(msg wire.Message,
	doneChan chan<- struct{}, encoding wire.MessageEncoding) {

	m.requests <- msg
}

func (m *mockPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
	msgChan := make(chan wire.Message)
	m.subscriptions <- msgChan

	return msgChan, func() {}
}

func (m *mockPeer) OnDisconnect() <-chan struct{} {
	return m.quit
}

func (m *mockPeer) Addr() string {
	return "mock"
}

// makeTask returns a new query task that will be done when it is given the
// finalResp message. Similarly ot will progress on being given the
// progressResp message, while any other message will be ignored.
func makeTask() *queryTask {
	q := &Query{
		Req: req,
		HandleResp: func(req, resp wire.Message, _ string) (bool, bool) {
			if resp == finalResp {
				return true, true
			}

			if resp == progressResp {
				return false, true
			}

			return false, false
		},
	}
	return &queryTask{
		index:   123,
		options: defaultQueryOptions(),
		Query:   q,
	}
}

type testCtx struct {
	nextJob    chan *queryTask
	jobResults chan *jobResult
	peer       *mockPeer
	workerDone chan struct{}
}

// startWorker creates and starts a worker for a new mockPeer. A test context
// containg channels to hand the worker new tasks and receiving the job
// results, in addition to the mockPeer, is returned.
func startWorker() (*testCtx, error) {
	peer := &mockPeer{
		requests:      make(chan wire.Message),
		subscriptions: make(chan chan wire.Message),
		quit:          make(chan struct{}),
	}
	nextJob := make(chan *queryTask)
	results := make(chan *jobResult)
	quit := make(chan struct{})

	wk := &worker{
		peer:    peer,
		nextJob: nextJob,
		results: results,
		quit:    quit,
	}

	// Start worker.
	done := make(chan struct{})
	go func() {
		defer close(done)
		wk.run()
	}()

	// Wait for it to subscribe to peer messages.
	var sub chan wire.Message
	select {
	case sub = <-peer.subscriptions:
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("did not subscribe to msgs")
	}
	peer.responses = sub

	return &testCtx{
		nextJob:    nextJob,
		jobResults: results,
		peer:       peer,
		workerDone: done,
	}, nil
}

// TestWorkerIgnoreMsgs tests that the worker handles being given the response
// to its query after first receiving some non-matching messages.
func TestWorkerIgnoreMsgs(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	nextJob := ctx.nextJob
	jobResults := ctx.jobResults
	peer := ctx.peer

	// Create a new task and give it to the worker.
	task := makeTask()

	select {
	case nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be sent to the peer.
	select {
	case <-peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// First give the worker a few random responses. These will all be
	// ignored.
	select {
	case peer.responses <- &wire.MsgTx{}:
	case <-time.After(time.Second):
		t.Fatalf("resp not received")
	}

	// Answer the query with the correct response.
	select {
	case peer.responses <- finalResp:
	case <-time.After(time.Second):
		t.Fatalf("resp not received")
	}

	// The worker should respond with a job finished.
	var result *jobResult
	select {
	case result = <-jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != nil {
		t.Fatalf("response error: %v", result.err)
	}

	// Make sure the result was given for the intended task.
	if result.task != task {
		t.Fatalf("got result for unexpected job")
	}
}

// TestWorkerTimeout tests that the worker will eventually return a Timeout
// error if the query is not answered before the time limit.
func TestWorkerTimeout(t *testing.T) {
	t.Parallel()

	const (
		timeout  = 50 * time.Millisecond
		cooldown = 200 * time.Millisecond
	)

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	nextJob := ctx.nextJob
	jobResults := ctx.jobResults
	peer := ctx.peer

	// Create a task with a small timeout, and a custom peer cooldown.
	task := makeTask()
	Timeout(timeout)(task.options)
	task.options.peerCooldown = cooldown

	// Give the worker the new job.
	select {
	case nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Don't anwer the query. This should trigger a timeout, and the worker
	// should respond with an error result.
	var result *jobResult
	select {
	case result = <-jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != ErrQueryTimeout {
		t.Fatalf("expected timeout, got: %v", result.err)
	}

	// Make sure the result was given for the intended task.
	if result.task != task {
		t.Fatalf("got result for unexpected job")
	}

	// Check that the failed worker will wait the cooldown period before
	// attempting to fetch a new job.
	select {
	case nextJob <- &queryTask{}:
		t.Fatalf("Worker fetched new job within cooldown period")
	case <-time.After(cooldown / 2):
		// Expected.
	}

	// After cooldown is over, it will fetch the job again.
	select {
	case nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}
}

// TestWorkerDisconnect tests that the worker will return an error if the peer
// disconnects, and that the worker itself is then shut down.
func TestWorkerDisconnect(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	nextJob := ctx.nextJob
	jobResults := ctx.jobResults
	peer := ctx.peer

	// Give the worker a new job.
	task := makeTask()
	select {
	case nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Disconnect the peer.
	close(peer.quit)

	// The worker should respond with a job failure.
	var result *jobResult
	select {
	case result = <-jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != ErrPeerDisconnected {
		t.Fatalf("expected peer disconnect, got: %v", result.err)
	}

	// Make sure the result was given for the intended task.
	if result.task != task {
		t.Fatalf("got result for unexpected job")
	}

	// Finally, make sure the worker go routine exits.
	select {
	case <-ctx.workerDone:
	case <-time.After(time.Second):
		t.Fatalf("worker did not exit")
	}
}

// TestWorkerProgress tests that the query won't timeout as long as it is
// making progress.
func TestWorkerProgress(t *testing.T) {
	t.Parallel()

	ctx, err := startWorker()
	if err != nil {
		t.Fatalf("unable to start worker: %v", err)
	}

	nextJob := ctx.nextJob
	jobResults := ctx.jobResults
	peer := ctx.peer

	// Create a task with a small timeout, and give it to the worker.
	task := makeTask()
	Timeout(50 * time.Millisecond)(task.options)

	select {
	case nextJob <- task:
	case <-time.After(1 * time.Second):
		t.Fatalf("did not pick up job")
	}

	// The request should be given to the peer.
	select {
	case <-peer.requests:
	case <-time.After(time.Second):
		t.Fatalf("request not sent")
	}

	// Send a few other responses that indicates progress, but not success.
	// We add a small delay between each time we send a response. In total
	// the delay will be larger than the query timeout, but since we are
	// making progress, the timeout won't trigger.
	for i := 0; i < 5; i++ {
		select {
		case peer.responses <- progressResp:
		case <-time.After(time.Second):
			t.Fatalf("resp not received")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// Finally send the final response.
	select {
	case peer.responses <- finalResp:
	case <-time.After(time.Second):
		t.Fatalf("resp not received")
	}

	// The worker should respond with a job finised.
	var result *jobResult
	select {
	case result = <-jobResults:
	case <-time.After(time.Second):
		t.Fatalf("response not received")
	}

	if result.err != nil {
		t.Fatalf("expected no error, got: %v", result.err)
	}

	// Make sure the result was given for the intended task.
	if result.task != task {
		t.Fatalf("got result for unexpected job")
	}
}
