package datamodel;
import java.io.Serializable;

public class FxRate implements Serializable {

	private static final long serialVersionUID = 1L;
	private String currencyPair;
	private int positionId;
	private String conversionDate;
	private String conversionTime;
	private float open;
	private float high;
	private float low;
	private float close;
	
	/**
	 * @param id
	 * @param currencyPair
	 * @param conversionDate
	 * @param conversionTime
	 * @param open
	 * @param high
	 * @param low
	 * @param close
	 */
	public FxRate(final int positionId, final String currencyPair, final String conversionDate, final String conversionTime, final String open, final String high, final String low, final String close) 
		throws NumberFormatException {
		this.currencyPair = currencyPair;
		this.positionId = positionId;
		this.conversionDate = conversionDate;
		this.conversionTime = conversionTime;
		this.open = Float.parseFloat(open);
		this.high = Float.parseFloat(high);
		this.low = Float.parseFloat(low);
		this.close = Float.parseFloat(close);
	}
	
	public FxRate(final String currencyPair, final String[] line, final int positionId)
		throws NumberFormatException {
		this.currencyPair = currencyPair;
		this.positionId = positionId;
		this.conversionDate = line[0];
		this.conversionTime = line[1];
		this.open = Float.parseFloat(line[2]);
		this.high = Float.parseFloat(line[3]);
		this.low = Float.parseFloat(line[4]);
		this.close = Float.parseFloat(line[5]);
	}

	public final String getCurrencyPair() {
		return currencyPair;
	}
	public final int getPositionId() {
		return positionId;
	}
	public final String getConversionDate() {
		return conversionDate;
	}
	public final String getConversionTime() {
		return conversionTime;
	}
	public final float getOpen() {
		return open;
	}
	public final float getHigh() {
		return high;
	}
	public final float getLow() {
		return low;
	}
	public final float getClose() {
		return close;
	}
	
	public final String toCsvFormat () {
		return  this.getPositionId() + ";" +
				this.getCurrencyPair() + ";" +
				this.getConversionDate() + ";" +
				this.getConversionTime() + ";" +
				this.getOpen() + ";" +
				this.getHigh() + ";" +
				this.getLow() + ";" +
				this.getClose() + ";"; 
	}
} 