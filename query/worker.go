package query

import (
	"errors"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/ticker"
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

// queryJob is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type queryJob struct {
	index      uint64
	timeout    time.Duration
	encoding   wire.MessageEncoding
	cancelChan <-chan struct{}
	*Request
}

// queryJob should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*queryJob)(nil)

// Index returns the queryJob's index within the work queue.
//
// NOTE: Part of the Task interface.
func (q *queryJob) Index() uint64 {
	return q.index
}

type activeJob struct {
	timeout <-chan time.Time
	*queryJob
}

// jobResult is the final result of the worker's handling of the queryJob.
type jobResult struct {
	job  *queryJob
	peer Peer
	err  error
}

// worker is responsible for polling work from its work queue, and hand it to
// the associated peer. It validates incoming responses with the current
// query's response handler, and poll more work for the peer when it has
// successfully received a response to the request.
type worker struct {
	// nextJob is a channel of queries to be distributed, where the worker
	// will poll new work from.
	nextJob chan *queryJob
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker.
func NewWorker() Worker {
	return &worker{
		nextJob: make(chan *queryJob),
	}
}

// Run starts the worker and associates it with the given peer. peer is the
// peer this worker will supply with queries, and handle responses from.
// Results for any query handled by this worker will be delivered on the
// results channel. quit can be closed to immediately make the worker exit.
//
// The method is blocking, and should be started in a goroutine. It will run
// until the peer disconnects or the worker is told to quit.
//
// NOTE: Part of the Worker interface.
func (w *worker) Run(peer Peer, results chan<- *jobResult,
	quit <-chan struct{}) {

	// Subscribe to messages from the peer.
	msgChan, cancel := peer.SubscribeRecvMsg()
	defer cancel()

	// Wait for the correct response to be received from the peer,
	// or an error happening.
	var (
		jobs          = make(map[uint64]*activeJob)
		timeoutTicker = ticker.New(1 * time.Second)
	)

	timeoutTicker.Resume()
	defer timeoutTicker.Stop()

Loop:
	for {

		select {
		// Poll a new job from the nextJob channel.
		case job := <-w.nextJob:
			log.Tracef("Worker %v picked up job with index %v",
				peer.Addr(), job.Index())

			select {
			// There is no point in queueing the request if the job already
			// is canceled, so we check this quickly.
			case <-job.cancelChan:
				log.Tracef("Worker %v found job with index %v "+
					"already canceled", peer.Addr(), job.Index())
				continue Loop

			default:
			}

			// We received a non-canceled query job, send it to the peer.
			log.Tracef("Worker %v queuing job %T with index %v",
				peer.Addr(), job.Req, job.Index())

			peer.QueueMessageWithEncoding(job.Req, nil, job.encoding)

			jobs[job.Index()] = &activeJob{
				timeout:  time.After(job.timeout),
				queryJob: job,
			}

		// A message was received from the peer, use the
		// response handler to check whether it was answering
		// our request.
		case resp := <-msgChan:
			// TODO: optimize by assuming in-order replies are common?
			// can possibly also require i to be handled before
			// handling i+1, but must avoid deadlocks.
			for i, job := range jobs {
				progress := job.HandleResp(
					job.Req, resp, peer.Addr(),
				)

				log.Tracef("Worker %v handled msg %T while "+
					"waiting for response to %T (job=%v). "+
					"Finished=%v, progressed=%v",
					peer.Addr(), resp, job.Req, job.Index(),
					progress.Finished, progress.Progressed)

				// If the response did not answer our query, we
				// check whether it did progress it.
				if !progress.Finished {
					// If it did make progress we reset the
					// timeout. This ensures that the
					// queries with multiple responses
					// expected won't timeout before all
					// responses have been handled.
					// TODO(halseth): separete progress
					// timeout value.
					if progress.Progressed {
						job.timeout = time.After(
							job.queryJob.timeout,
						)
					}
					continue Loop
				}

				delete(jobs, i)

				// We have a result ready for the query, hand it off before
				// getting a new job.
				select {
				case results <- &jobResult{
					job:  job.queryJob,
					peer: peer,
					err:  nil,
				}:
				case <-quit:
					return
				}
			}

		// If the timeout is reached before a valid response
		// has been received, we exit with an error.
		case <-timeoutTicker.Ticks():
			for i, job := range jobs {
				var jobErr error
				select {

				// The query did experience a timeout and will
				// be given to someone else.
				case <-job.timeout:
					jobErr = ErrQueryTimeout
					log.Tracef("Worker %v timeout for request %T "+
						"with job index %v", peer.Addr(),
						job.Req, job.Index())

				// If the job was canceled, we report this back to the
				// work manager.
				case <-job.cancelChan:
					log.Tracef("Worker %v job %v canceled",
						peer.Addr(), job.Index())

					jobErr = ErrJobCanceled

				default:
					continue
				}

				delete(jobs, i)

				// We have a result ready for the query, hand it off before
				// getting a new job.
				select {
				case results <- &jobResult{
					job:  job.queryJob,
					peer: peer,
					err:  jobErr,
				}:
				case <-quit:
					return
				}
			}

		// If the peer disconnectes before giving us a valid
		// answer, we'll also exit with an error.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected, "+
				"cancelling %d jobs", peer.Addr(),
				len(jobs))

			for _, job := range jobs {
				select {
				case results <- &jobResult{
					job:  job.queryJob,
					peer: peer,
					err:  ErrPeerDisconnected,
				}:
				case <-quit:
					return
				}
			}

			// If the peer disconnected, we can exit immediately.
			return

		case <-quit:
			return
		}
	}
}

// NewJob returns a channel where work that is to be handled by the worker can
// be sent. If the worker reads a queryJob from this channel, it is guaranteed
// that a response will eventually be deliverd on the results channel (except
// when the quit channel has been closed).
//
// NOTE: Part of the Worker interface.
func (w *worker) NewJob() chan<- *queryJob {
	return w.nextJob
}
