package executionservices;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {
	
	// Logger
	private static Logger logger = LoggerFactory.getLogger(RejectedExecutionHandlerImpl.class);
	
	public RejectedExecutionHandlerImpl () {
	}
	
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    	 try {
    		 /*
              * This does the actual put into the queue. Once the max threads have been reached, the tasks will then queue up.
              */
    		 executor.getQueue().put(r);
         } catch (InterruptedException e) {
        	 logger.error (r.toString() + " Rejected and Discarded due to InterruptedException");
         }
    	
		 /*
		  * Option to set numAttempts per RunnableWorkerThread
		 */
/*
    	int numAttempts=((RunnableWorkerThread)r).getRetryMaxAttempts();
		int retrySleepTime=((RunnableWorkerThread)r).getRetrySleepTime();

		if (numAttempts > 0) {
			if (printDetails) SystemUtils.printLog(r.toString() + " Rejected. Retry in " + retrySleepTime + " ms. Updating retries to " + (numAttempts-1),true);
			((RunnableWorkerThread)r).setRetryMaxAttempts(numAttempts-1);
			
			try {
				Thread.sleep(retrySleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			executor.execute (r);
		} else {
			if (printDetails) SystemUtils.printLog(r.toString() + " Discarded. No more retries",true);
		}
*/
    }
}