import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datamodel.FxRate;

public class WorkerRunnable implements Runnable {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(WorkerRunnable.class);

	private Map<String, List<FxRate>> historicalDataMap;
	private Map<String, Integer> resultsMap = new HashMap<String, Integer>();
	private String currentCurrency;
	
	private float increase;
	private float decrease;
	private long calculationStartTime;	
	private long calculationStopTime;
	private int maxLevels;
	private long totalCalculations;
	private CountDownLatch latch;
	
	public WorkerRunnable (final Map<String,List<FxRate>> historicalDataMap, final String currentCurrency, Map<String, Integer> resultsMap, final float increase, final float decrease, final int maxLevels, long totalCalculations, CountDownLatch latch){
		this.historicalDataMap = historicalDataMap;
		this.resultsMap = resultsMap;
		this.currentCurrency = currentCurrency;
		this.increase = increase;
		this.decrease = decrease;
		this.maxLevels = maxLevels;
		this.totalCalculations = totalCalculations;
		this.latch = latch;
	}
	
	@Override
	public void run() {
		try { 

			logger.info ("Started calculations for " + currentCurrency);
			calculationStartTime = System.currentTimeMillis();

			if (historicalDataMap.containsKey(currentCurrency)) {

				for (FxRate originalFxRate : historicalDataMap.get(currentCurrency)) {
					
					int positionId = originalFxRate.getPositionId();
					String currencyPair = originalFxRate.getCurrencyPair();
					float opening = originalFxRate.getOpen();
					
					logger.debug ("Processing " + currencyPair + "-" + positionId);
					
					FxRate targetFxRate = null;
					String previousFound = "";
					
					int indexUp = 1;
					int indexDown = 1;
	
					for (int i=positionId+1; i<historicalDataMap.get(currentCurrency).size(); i++) {
						targetFxRate = historicalDataMap.get(currentCurrency).get(i);
						
						if (originalFxRate.getCurrencyPair().equals(targetFxRate.getCurrencyPair())) {
						
							logger.debug ("Comparing against " + targetFxRate.getCurrencyPair() + "-" + targetFxRate.getPositionId());
							
							if ((targetFxRate.getHigh() > opening * increase) && (indexUp <= maxLevels)) {
								if (("down").equals(previousFound)) {
									break;
								}
								
								if (resultsMap.containsKey(currencyPair+"-UP["+indexUp+"]")) {
									resultsMap.put(currencyPair+"-UP["+indexUp+"]",resultsMap.get(currencyPair+"-UP["+indexUp+"]")+1);
								} else {
									resultsMap.put(currencyPair+"-UP["+indexUp+"]",1);
								}
								
								previousFound = "up";
								opening = opening * increase;
								indexUp++;
							} else if ((targetFxRate.getLow() < opening * decrease) && (indexDown <= maxLevels)) {
								if (("up").equals(previousFound)) {
									break;
								}
								
								if (resultsMap.containsKey(currencyPair+"-DOWN["+indexDown+"]")) {
									resultsMap.put(currencyPair+"-DOWN["+indexDown+"]",resultsMap.get(currencyPair+"-DOWN["+indexDown+"]")+1);
								} else {
									resultsMap.put(currencyPair+"-DOWN["+indexDown+"]",1);
								}
			
								previousFound = "down";
								opening = opening * decrease;
								indexDown++;			
							}
							totalCalculations++;
						}
					}
				}
			}
			
			calculationStopTime = System.currentTimeMillis();
			logger.info ("Finished calculations [" + totalCalculations + "] in " + (calculationStopTime - calculationStartTime) + " ms");
			latch.countDown();
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
	}

}
