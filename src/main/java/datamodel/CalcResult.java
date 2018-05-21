package datamodel;
import java.util.Map;

public class CalcResult {

	private String currencyPair;
	private float increase;
	private float decrease;
	private int maxLevels;
	private long histDataStartTime;	
	private long histDataStopTime;
	private long totalHistDataLoaded;
	private long calculationStartTime;	
	private long calculationStopTime;
	private long totalCalculations;
	private Map<String,Integer> levelResults;
	
	public CalcResult(final String currencyPair, final float increase, final float decrease, final int maxLevels, final long histDataStartTime, final long histDataStopTime, final long totalHistDataLoaded, final long calculationStartTime, final long calculationStopTime, final long totalCalculations, final Map<String,Integer> levelResults) {
		this.currencyPair = currencyPair;
		this.increase = increase;
		this.decrease = decrease;
		this.maxLevels = maxLevels;
		this.histDataStartTime = histDataStartTime;
		this.histDataStopTime = histDataStopTime;
		this.totalHistDataLoaded = totalHistDataLoaded;
		this.calculationStartTime = calculationStartTime;
		this.calculationStopTime = calculationStopTime;
		this.totalCalculations = totalCalculations;
		this.levelResults = levelResults;
	}
	
	public final String getCurrencyPair() { return currencyPair; }
	public final float getIncrease() { return increase; }
	public final float getDecrease() { return decrease; }
	public final int getmaxLevels() { return maxLevels; }
	public final long getHistDataStartTime() { return histDataStartTime; }
	public final long getHistDataStopTime() { return histDataStopTime; }
	public final long getTotalHistDataLoaded() { return totalHistDataLoaded; }
	public final long getCalculationStartTime() { return calculationStartTime; }
	public final long getCalculationStopTime() { return calculationStopTime; }
	public final long getTotalCalculations() { return totalCalculations; }
	public final Map<String,Integer> getLevelResults() { return levelResults; }
} 