import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datamodel.CalcResult;
import datamodel.FxRate;

public class WorkerRunnable implements Runnable {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(WorkerRunnable.class);

	private Map<String, List<FxRate>> historicalDataMap;
	private Map<String, Integer> resultsMap = new HashMap<String, Integer>();
	private Map<String, CalcResult> calcResultsMap;

	private String currentCurrency;
	private float increase;
	private float decrease;
	private long calculationStartTime;	
	private long calculationStopTime;
	private int maxLevels;
	private long totalCalculations;
	private CountDownLatch latch;
	
	public WorkerRunnable (final Map<String,List<FxRate>> historicalDataMap, final String currentCurrency, Map<String, CalcResult> calcResultsMap, final float increase, final float decrease, final int maxLevels, CountDownLatch latch){
		this.historicalDataMap = historicalDataMap;
		this.currentCurrency = currentCurrency;
		this.calcResultsMap = calcResultsMap;
		this.increase = increase;
		this.decrease = decrease;
		this.maxLevels = maxLevels;
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
								if (("DOWN").equals(previousFound)) {
									break;
								}
								
								if (resultsMap.containsKey("UP-"+indexUp)) {
									resultsMap.put("UP-"+indexUp,resultsMap.get("UP-"+indexUp)+1);
								} else {
									resultsMap.put("UP-"+indexUp,1);
								}
								
								previousFound = "UP";
								opening = opening * increase;
								indexUp++;
							} else if ((targetFxRate.getLow() < opening * decrease) && (indexDown <= maxLevels)) {
								if (("UP").equals(previousFound)) {
									break;
								}
								
								if (resultsMap.containsKey("DOWN-"+indexDown)) {
									resultsMap.put("DOWN-"+indexDown,resultsMap.get("DOWN-"+indexDown)+1);
								} else {
									resultsMap.put("DOWN-"+indexDown,1);
								}
			
								previousFound = "DOWN";
								opening = opening * decrease;
								indexDown++;			
							}
							totalCalculations++;
						}
					}
				}
				
			}
			calculationStopTime = System.currentTimeMillis();
			calcResultsMap.put(currentCurrency, new CalcResult(currentCurrency, increase, decrease, calculationStartTime, calculationStopTime, maxLevels, totalCalculations, resultsMap));
			
			logger.info ("Finished calculations [" + totalCalculations + "] in " + (calculationStopTime - calculationStartTime) + " ms");
			latch.countDown();
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
	}

}
