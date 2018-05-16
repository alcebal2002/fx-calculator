package executionservices;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit; 
  
public class SystemThreadPoolExecutor extends ThreadPoolExecutor { 
        
	private long minExecutionTime = Long.MAX_VALUE; 
    private long maxExecutionTime = 0;
    private long totalExecutionTime = 0;
    private int totalExecutions = 0;

    public SystemThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, 
    							BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) { 
    	super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    } 

    protected void afterExecute (Runnable r, Throwable t ) { 
            
	    try {
	    	totalExecutions++;
		    long elapsedTimeMillis = ((RunnableWorkerThread)r).getElapsedTimeMillis(); 
		    totalExecutionTime = totalExecutionTime + elapsedTimeMillis;
		    if (elapsedTimeMillis < minExecutionTime) minExecutionTime = elapsedTimeMillis; 
		    if (elapsedTimeMillis > maxExecutionTime) maxExecutionTime = elapsedTimeMillis;
	    } finally { 
	    	super.afterExecute(r, t); 
	    } 
    } 
    
    public long getMinExecutionTime () { 
    	return this.minExecutionTime; 
    } 
    
    public long getMaxExecutionTime () { 
    	return this.maxExecutionTime; 
    }
    
    public long getTotalExecutionTime () { 
    	return this.totalExecutionTime; 
    }

    public long getTotalExecutions () { 
    	return this.totalExecutions; 
    }

    public long getAvgExecutionTime () {
    	long result = 0L;
    	
    	if (this.totalExecutions > 0) {
    		result = this.totalExecutionTime / this.totalExecutions;
    	}
    	
    	return result;
    } 
} 