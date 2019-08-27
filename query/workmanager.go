package query

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
)

type batch struct {
	queries []*Query
	options *queryOptions
	errChan chan error
}

// Config holds the configuration options for a new WorkManager.
type Config struct {
	// ConnectedPeers is a function that returns a channel where all
	// connected peers will be sent. It is assumed that all current peers
	// will be sent imemdiately, and new peers as they connect.
	ConnectedPeers func() (<-chan Peer, func(), error)
}

// WorkManager is the main access point for outside callers, and satisfy the
// QueryAccess API. It receives queries to pass to peers, and schedules them
// among available workers, orchestrating where to send them.
type WorkManager struct {
	cfg *Config

	// newBatches is a channel where new batches of queries will be sent to
	// the workDispatcher.
	newBatches chan *batch

	// runWorker is function closure that should start a new worker for the
	// given peer. We make this configurable to easily mock the worker used
	// during tests.
	runWorker func(peer Peer, nextJob <-chan *queryTask,
		results chan<- *jobResult, quit chan struct{})

	quit chan struct{}
	wg   sync.WaitGroup
}

// Compile time check to ensure WorkManager satisfies the QueryAccess
// interface.
var _ QueryAccess = (*WorkManager)(nil)

// New returns a new WorkManager with the regular worker implementation.
func New(cfg *Config) *WorkManager {
	return &WorkManager{
		cfg:        cfg,
		newBatches: make(chan *batch),

		// We set the runWorker closure to start an internal worker
		// with the passed params.
		runWorker: func(peer Peer, nextJob <-chan *queryTask,
			results chan<- *jobResult, quit chan struct{}) {
			(&worker{
				peer:    peer,
				nextJob: nextJob,
				results: results,
				quit:    quit,
			}).run()
		},
		quit: make(chan struct{}),
	}
}

// Start starts the WorkManager.
func (w *WorkManager) Start() error {
	// TODO(halseth): any performance gain from buffering these?
	nextJob := make(chan *queryTask)
	responses := make(chan *jobResult)

	w.wg.Add(2)
	go w.peerListener(nextJob, responses)
	go w.workDispatcher(nextJob, responses)

	return nil
}

// Stop stops the WorkManager and all underlying goroutines.
func (w *WorkManager) Stop() error {
	close(w.quit)
	w.wg.Wait()

	return nil
}

// peerListener listens for connected peers, and starts a new worker each time
// a peer connects.
//
// NOTE: MUST be run as a goroutine.
func (w *WorkManager) peerListener(nextJob <-chan *queryTask,
	jobResults chan<- *jobResult) {

	defer w.wg.Done()

	// Get a peer subscription.
	peersConnected, cancel, err := w.cfg.ConnectedPeers()
	if err != nil {
		log.Errorf("Unable to get connected peers: %v", err)
		return
	}
	defer cancel()

	// Spin up a goroutine that runs a worker each time a peer
	// connects.
	for {
		select {
		case peer := <-peersConnected:
			log.Debugf("Starting worker for peer %v",
				peer.Addr())

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()

				w.runWorker(
					peer, nextJob, jobResults,
					w.quit,
				)
			}()

		case <-w.quit:
			return
		}
	}
}

// workDispatcher receives batches of queries to be performed from external
// callers, and dispatches these to active workers through the nextJob channel.
// It makes sure to prioritize the queries in the order they come in, such that
// early queries will be attemped completed first.
//
// NOTE: MUST be run as a goroutine.
func (w *WorkManager) workDispatcher(nextJob chan<- *queryTask,
	jobResults <-chan *jobResult) {

	defer w.wg.Done()

	// Init a work queue which will be used to sort the incoming queries in
	// a first come first serverd fashion.
	work := &workQueue{}
	heap.Init(work)

	type batchProgress struct {
		rem     int
		errChan chan error
	}

	// We set up a batch index counter to keep track of batches that still
	// have queries in in flight. This let us track when all queries for a
	// batch has been finished, and return an (non-)error to the caller.
	batchIndex := 0
	currentBatches := make(map[int]*batchProgress)

	// When the work dispatcher exits, we'll loop trhough the remaining
	// batches and send on their error channel.
	defer func() {
		for _, b := range currentBatches {
			b.errChan <- errors.New("WorkManager shutting down")
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := 0
	currentQueries := make(map[int]int)

	for {
		// Wait for a result of a previous query to come back, or for a
		// new batch of queries to be scheduled.
		var (
			result *jobResult
			batch  *batch
		)

		switch {

		// If the work queue is non-empty, we'll take out the first
		// element in order for distributing it to a worker.
		case work.Len() > 0:
			next := work.Peek().(*queryTask)

			select {
			// A worker picked up the job, we can remove it from
			// the work queue.
			case nextJob <- next:
				heap.Pop(work)

			// A new result came back.
			case result = <-jobResults:

			// A new batch of queries where scheduled.
			case batch = <-w.newBatches:
			case <-w.quit:
				return
			}

		// Otherwise the work queue is empty, so we'll just wait for a
		// new result or more queries.
		default:
			select {
			case result = <-jobResults:
			case batch = <-w.newBatches:
			case <-w.quit:
				return
			}

		}

		// If we had a new result, inspect the result.
		if result != nil {
			// Get the index of this query's batch, and delete it
			// from the map of current queries, since we don't have
			// to track it anymore.
			batchNum := currentQueries[result.task.index]
			delete(currentQueries, result.task.index)

			switch {
			case result.err != nil:
				switch {
				// If the query task ended because it was
				// canceled, drop it.
				case result.err == ErrJobCanceled:
					log.Warnf("Query(%d) was canceled "+
						"before result was available",
						result.task.index)

					// If this is the first job in this
					// batch that was canceled, forward the
					// error on the batch's error channel.
					// We do this since a cancellation
					// applies to the whole batch.
					b, ok := currentBatches[batchNum]
					if ok {
						b.errChan <- result.err
						delete(currentBatches, batchNum)
					}

				// If the query task ended with any other
				// error, put it back into the work queue.
				default:
					log.Warnf("Query(%d) failed with "+
						"error: %v. Rescheduling",
						result.task.index, result.err)
					heap.Push(work, result.task)
					currentQueries[result.task.index] = batchNum
				}

			// Otherwise we update the status of the batch this
			// query is a part of.
			default:
				// Get the batch and decrement the number of
				// queries remaining.
				b, ok := currentBatches[batchNum]
				if ok {
					b.rem--

					// If this was the last query in flight
					// for this batch, we can notify that
					// it finished, and delete it.
					if b.rem == 0 {
						b.errChan <- nil
						delete(currentBatches, batchNum)
					}
				}
			}
		}

		// Add all new queries in the batch to our work queue, with
		// priority given by the order they were scheduled.
		if batch != nil {
			log.Debugf("Adding new batch(%d) of %d queries to "+
				"work queue", batchIndex, len(batch.queries))

			for _, q := range batch.queries {
				heap.Push(work, &queryTask{
					index:   queryIndex,
					options: batch.options,
					Query:   q,
				})
				currentQueries[queryIndex] = batchIndex
				queryIndex++
			}

			currentBatches[batchIndex] = &batchProgress{
				rem:     len(batch.queries),
				errChan: batch.errChan,
			}
			batchIndex++
		}
	}
}

// Query distributes the slice of queries to the set of connected peers.
//
// NOTO: Part of the QueryAccess interface.
func (w *WorkManager) Query(queries []*Query,
	options ...QueryOption) chan error {

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	errChan := make(chan error, 1)

	// Add query messages to the task queue.
	select {
	case w.newBatches <- &batch{
		queries: queries,
		options: qo,
		errChan: errChan,
	}:
	case <-w.quit:
		errChan <- fmt.Errorf("workmanager shutting down")
	}

	return errChan
}
