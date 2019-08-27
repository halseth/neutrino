package query

import (
	"errors"
	"time"

	"github.com/btcsuite/btcd/wire"
)

var (
	// ErrQueryTimeout is an error returned if the worker doesn't respond
	// with a valid response to the request within the timeout.
	ErrQueryTimeout = errors.New("did not get response before timeout")

	// ErrPeerDisconnected is returned if the worker's peer disconnect
	// before the query has been answered.
	ErrPeerDisconnected = errors.New("peer disconnected")

	// ErrJobCanceled is returned if the job is canceled before the query
	// has been answered.
	ErrJobCanceled = errors.New("job canceled")
)

// queryTask is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type queryTask struct {
	index   int
	options *queryOptions
	*Query
}

// queryTask should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*queryTask)(nil)

// Index returns the queryTask's index within the work queue.
func (q *queryTask) Index() int {
	return q.index
}

// jobResult is the final result of the worker's handling of the queryTask.
type jobResult struct {
	task *queryTask
	err  error
}

// worker is responsible for polling work from the work queue, and hand it to
// the associated peer. It validates incoming responses with the current
// query's response handler, and poll more work for the peer when it has
// successfully received a response to the request.
type worker struct {
	// peer is the peer this worker will supply with queries, and handle
	// responses from.
	peer Peer

	// nextJob is a channel of queries to be distributed, where the worker
	// will poll new work tasks from.
	nextJob <-chan *queryTask

	// results is a channel where the worker will deliver the final result
	// of the last polled task.
	results chan<- *jobResult

	quit <-chan struct{}
}

// run starts the worker. The method is blocking, and should be started in a
// goroutine.
func (w *worker) run() {
	// Subscribe to messages from the peer.
	msgChan, cancel := w.peer.SubscribeRecvMsg()
	defer cancel()

	for {
		var job *queryTask

		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:

		// Ignore any message received while not working on a task.
		case <-msgChan:
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-w.peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected",
				w.peer.Addr())
			return

		case <-w.quit:
			return
		}

		select {
		// There is no point in queieing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.options.cancelChan:

		// We received a non-canceled query job, send it to the peer.
		default:
			w.peer.QueueMessageWithEncoding(
				job.Req, nil, job.options.encoding,
			)
		}

		// Wait for the correct response to be received from the peer,
		// or an error happening.
		var (
			resp    wire.Message
			jobErr  error
			timeout = time.After(job.options.timeout)
		)

	Loop:
		for {

			select {
			// A message was received from the peer, use the
			// response handler to check whether it was answering
			// our request.
			case resp = <-msgChan:
				done, progress := job.HandleResp(
					job.Req, resp, w.peer.Addr(),
				)

				// If the response did not answer our query, we
				// check whether it did progress it.
				if !done {
					// If it did make progress we reset the
					// timeout. This ensures that the
					// queries with multiple responses
					// expected won't timeout before all
					// responses have been handled.
					// TODO(halseth): separete progress
					// timeout value.
					if progress {
						timeout = time.After(
							job.options.timeout,
						)
					}
					continue Loop
				}

				// We did get a valid response, and can break
				// the loop.
				break Loop

			// If the timeout is reached before a valid response
			// has been received, we exit with an error.
			case <-timeout:
				// The query did experience a timeout and will
				// be given to someone else.
				jobErr = ErrQueryTimeout
				break Loop

			// If the peer disconnectes before giving us a valid
			// answer, we'll also exit with an error.
			case <-w.peer.OnDisconnect():
				log.Debugf("Peer %v for worker disconnected, "+
					"cancelling task", w.peer.Addr())

				jobErr = ErrPeerDisconnected
				break Loop

			// If the job was canceled, we report this back to the
			// work manager.
			case <-job.options.cancelChan:
				jobErr = ErrJobCanceled
				break Loop

			case <-w.quit:
				return
			}
		}

		// We have a result ready for the query task, hand it off
		// before getting a new task.
		select {
		case w.results <- &jobResult{
			task: job,
			err:  jobErr,
		}:
		case <-w.quit:
			return
		}

		switch jobErr {

		// If the worker experienced a timeout trying to query this
		// peer, we wait here for a given cooldown period before
		// fetching the next job. This is to give other peers the
		// chance to pick up the jobs.
		case ErrQueryTimeout:
			select {
			case <-time.After(job.options.peerCooldown):
			case <-w.peer.OnDisconnect():
				log.Debugf("Peer %v for worker disconnected",
					w.peer.Addr())
				return
			case <-w.quit:
				return
			}

		// If the peer disconnected, we can exit immediately.
		case ErrPeerDisconnected:
			return
		}
	}
}
