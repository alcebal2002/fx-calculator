package executionservices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemMonitorThread implements Runnable {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(SystemMonitorThread.class);
	
	private SystemThreadPoolExecutor executor;
    private int seconds;
    private boolean run=true;

    private String inetAddress;

    public SystemMonitorThread(SystemThreadPoolExecutor executor, int delay, String inetAddress) {
        this.executor=executor;
        this.seconds=delay;
        this.inetAddress = inetAddress;
    }

    public void shutdown() {
        this.run=false;
    }

    @Override
    public void run() {
        while(run) {
					
			try {
				logger.info (String.format("["+inetAddress+"][monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, Queued: %d, isShutdown: %s, isTerminated: %s",
					this.executor.getPoolSize(),
					this.executor.getCorePoolSize(),
					this.executor.getActiveCount(),
					this.executor.getCompletedTaskCount(),
					this.executor.getTaskCount(),
					this.executor.getQueue().size(),
					this.executor.isShutdown(),
					this.executor.isTerminated()));
				Thread.sleep(seconds*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
    }
}