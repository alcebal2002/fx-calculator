package datamodel;
import java.io.Serializable;
import java.util.Map;

public class CalcResult implements Serializable {

	private static final long serialVersionUID = 1L;
	private String currencyPair;
	private float increase;
	private float decrease;
	private long calculationStartTime;	
	private long calculationStopTime;
	private int maxLevels;
	private long totalCalculations;
	private Map<String,Integer> levelResults;
	
	public CalcResult(final String currencyPair, final float increase, final float decrease, final long calculationStartTime, final long calculationStopTime, final int maxLevels, final long totalCalculations, final Map<String,Integer> levelResults) {
		this.currencyPair = currencyPair;
		this.increase = increase;
		this.decrease = decrease;
		this.calculationStartTime = calculationStartTime;
		this.calculationStopTime = calculationStopTime;
		this.maxLevels = maxLevels;
		this.totalCalculations = totalCalculations;
		this.levelResults = levelResults;
	}
	
	public final String getCurrencyPair() {
		return currencyPair;
	}
	public final float getIncrease() {
		return increase;
	}
	public final float getDecrease() {
		return decrease;
	}
	public final long getCalculationStartTime() {
		return calculationStartTime;
	}
	public final long getCalculationStopTime() {
		return calculationStopTime;
	}
	public final long getTotalCalculations() {
		return totalCalculations;
	}
	public final int getmaxLevels() {
		return maxLevels;
	}
	public final Map<String,Integer> getLevelResults() {
		return levelResults;
	}
} 