package query

import (
	"fmt"
	"testing"
	"time"
)

type mockWorker struct {
	peer    Peer
	nextJob <-chan *queryTask
	results chan<- *jobResult
}

// TestWorkManagerPeerListener tests that the peerListener starts new workers
// as peers are connecting.
func TestWorkManagerPeerListener(t *testing.T) {
	const numQueries = 100
	const numWorkers = 10

	peerChan := make(chan Peer)
	wm := New(&Config{
		ConnectedPeers: func() (<-chan Peer, func(), error) {
			return peerChan, func() {}, nil
		},
	})

	// We set up a custom runWorker closure for the WorkManager, such that
	// we can start mockWorkers when it is called.
	workerChan := make(chan *mockWorker)
	wm.runWorker = func(peer Peer, nextJob <-chan *queryTask,
		results chan<- *jobResult, quit chan struct{}) {
		workerChan <- &mockWorker{
			peer:    peer,
			nextJob: nextJob,
			results: results,
		}
	}

	nextJob := make(chan *queryTask)
	responses := make(chan *jobResult)

	// Start the peer listener.
	wm.wg.Add(1)
	go wm.peerListener(nextJob, responses)

	// We'll notify about a set of connected peers, and expect the
	// peerListener to start a new worker for each.
	var workers [numWorkers]*mockWorker
	for i := 0; i < numWorkers; i++ {
		peer := &mockPeer{}
		select {
		case peerChan <- peer:
		case <-time.After(time.Second):
			t.Fatal("work manager did not receive peer")
		}

		// Wait for the worker to be started.
		var w *mockWorker
		select {
		case w = <-workerChan:
		case <-time.After(time.Second):
			t.Fatalf("no worker")
		}

		if w.peer != peer {
			t.Fatalf("unexpected peer")
		}

		workers[i] = w
	}

	// Send jobs on the nextJob channel to test that they properly arrive
	// at the workers that were created.
	for i := 0; i < numQueries; i++ {
		job := &queryTask{
			index: i,
		}

		go func() {
			select {
			case nextJob <- job:
			case <-time.After(time.Second):
				t.Fatalf("next job not received")
			}
		}()

		// All workers should listn on the same channel, so iterate
		// though them all.
		select {
		case j := <-workers[i%numWorkers].nextJob:
			if j != job {
				t.Fatalf("wrong job")
			}
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}
}

// TestWorkManagerWorkDispatcher tests that the workDispatcher goroutine
// propely schedules the incoming queries in the order of their batch and sends
// them to the workers.
func TestWorkManagerWorkDispatcher(t *testing.T) {
	const numQueries = 100

	nextJob := make(chan *queryTask)
	responses := make(chan *jobResult)

	// Start the workDispatcher goroutine.
	wm := New(&Config{})
	wm.wg.Add(1)
	go wm.workDispatcher(nextJob, responses)

	// Schedule a batch of queries.
	var queries []*Query
	for i := 0; i < numQueries; i++ {
		q := &Query{}
		queries = append(queries, q)
	}

	errChan := wm.Query(queries)

	// Each query should be sent on the nextJob queue, in the order they
	// had in their batch.
	for i := 0; i < numQueries; i++ {
		var job *queryTask
		select {
		case job = <-nextJob:
			if job.index != i {
				t.Fatalf("wrong index")
			}
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		// Respond with a success result.
		select {
		case responses <- &jobResult{
			task: job,
			err:  nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// The query should exit with a non-error.
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("got error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}

// TestWorkManagerDispatcherFailure tests that queries that fail gets resent to
// workers.
func TestWorkManagerWorkDispatcherFailures(t *testing.T) {
	const numQueries = 100

	wm := New(&Config{})

	nextJob := make(chan *queryTask)
	responses := make(chan *jobResult)

	go wm.workDispatcher(nextJob, responses)

	// Schedule a batch of queries.
	var queries []*Query
	for i := 0; i < numQueries; i++ {
		q := &Query{}
		queries = append(queries, q)
	}

	// Send the batch, and Retrieve all jobs immediately.
	errChan := wm.Query(queries)

	var jobs []*queryTask
	for i := 0; i < numQueries; i++ {
		var job *queryTask
		select {
		case job = <-nextJob:
			if job.index != i {
				t.Fatalf("unexpected index.")
			}
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}

		jobs = append(jobs, job)
	}

	// Go backwards, and fail half of them.
	for i := numQueries - 1; i >= 0; i-- {
		var err error
		if i%2 == 0 {
			err = fmt.Errorf("failed job")
		}

		select {
		case responses <- &jobResult{
			task: jobs[i],
			err:  err,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// Finally, make sure the failed jobs are being retried, in the same
	// order as they were originally scheduled.
	for i := 0; i < numQueries; i += 2 {
		var job *queryTask
		select {
		case job = <-nextJob:
			if job.index != i {
				t.Fatalf("wrong index")
			}
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("next job not received")
		}
		select {
		case responses <- &jobResult{
			task: job,
			err:  nil,
		}:
		case <-errChan:
			t.Fatalf("did not expect on errChan")
		case <-time.After(time.Second):
			t.Fatalf("result not handled")
		}
	}

	// The query should ultimately succeed.
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("got error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("nothing received on errChan")
	}
}
