package datamodel;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FxRate {

	private String currencyPair;
	private int positionId;
	private String conversionDate;
	private String conversionTime;
	private float open;
	private float high;
	private float low;
	private float close;
	
	/**
	 * @param positionId
	 * @param currencyPair
	 * @param conversionDate
	 * @param conversionTime
	 * @param open
	 * @param high
	 * @param low
	 * @param close
	 */
	public FxRate(final int positionId, final String currencyPair, final String conversionDate, final String conversionTime, final float open, final float high, final float low, final float close) 
		throws NumberFormatException {
		this.currencyPair = currencyPair;
		this.positionId = positionId;
		this.conversionDate = conversionDate;
		this.conversionTime = conversionTime;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
	}
	
	public FxRate(final String currencyPair, final String[] line, final int positionId, final String startDate, final String endDate)
		throws Exception {
		
		if (checkFilter(line[0],startDate,endDate)) {
			this.currencyPair = currencyPair;
			this.positionId = positionId;
			this.conversionDate = line[0];
			this.conversionTime = line[1];
			this.open = Float.parseFloat(line[2]);
			this.high = Float.parseFloat(line[3]);
			this.low = Float.parseFloat(line[4]);
			this.close = Float.parseFloat(line[5]);
		}
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
	
	private final boolean checkFilter (final String rowDate, final String startDate, final String endDate) throws Exception {
		boolean result = false;
		
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date fileDate = format.parse(rowDate);
		Date filterStartDate = format.parse(startDate);
		Date filerEndDate = format.parse(endDate);
		
		if ((filterStartDate.compareTo(fileDate) * fileDate.compareTo(filerEndDate)) >= 0) {
			result = true;
		}
		
		return result;
		
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