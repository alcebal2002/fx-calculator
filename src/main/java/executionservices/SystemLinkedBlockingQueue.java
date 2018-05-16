package executionservices;

import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("hiding")
public class SystemLinkedBlockingQueue<Runnable> extends LinkedBlockingQueue<Runnable> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public boolean offer(Runnable e) {
        /*
         * Offer it to the queue if there is 1 or 0 items already queued, else
         * return false so the TPE will add another thread. If we return false
         * and max threads have been reached then the RejectedExecutionHandler
         * will be called which will do the put into the queue.
         *
         * NOTE: I chose 1 to protect against race conditions where a task had
         * been added to the queue but the threads had not dequeued it yet. But
         * if there were more than 1, chances were greater that the current
         * threads were not keeping up with the load.  If you want to be more
         * aggressive about creating threads, then change this to: size() == 0
         */
		//return super.offer(e);
		
        if (size() <= 1) {
            return super.offer(e);
        } else {
            return false;
        }
    }
}
