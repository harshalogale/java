package models;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StockTransaction {
	private String symbol;
	private Date activityDate;
	private BigDecimal price;
	private BigDecimal quantity;
	private String transactionType;
	private BigDecimal commission;
	private String longShort;
	private String orderId;
	private String voucherNo;

	public StockTransaction(String symbol,
			Date activityDate,
			BigDecimal price,
			BigDecimal quantity,
			String transactionType,
			BigDecimal commission,
			String longShort,
			String orderId,
			String voucherNo) {
		super();
		this.symbol = symbol;
		this.activityDate = activityDate;
		this.price = price;
		this.quantity = quantity;
		this.transactionType = transactionType;
		this.commission = commission;
		this.longShort = longShort;
		this.orderId = orderId;
		this.voucherNo = voucherNo;
	}

	public static List<StockTransaction> parseCSV(List<String> csv) throws ParseException {
		List<StockTransaction> items = new ArrayList<>();

		SimpleDateFormat dfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSS");
		SimpleDateFormat dfMedium = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat dfShort = new SimpleDateFormat("yyyy-MM-dd");

		for (String line : csv) {
			// ex. "AMZN",2020-03-22,1150,7,"SELL",5.25,"LONG"
			String[] properties = line.split(",");

			String transactionType = properties[0].equals("Reinvestment") ? "BUY" : properties[0];
			String symbol = properties[1];
			String strDate = properties[2];
			Date activityDate;
			try {
				activityDate = dfLong.parse(strDate);
			} catch (ParseException e) {
				try {
					activityDate = dfMedium.parse(strDate);
				} catch (ParseException e1) {
					activityDate = dfShort.parse(strDate);
					e1.printStackTrace();
				}
			}
			BigDecimal price = new BigDecimal(properties[3]);
			BigDecimal quantity = new BigDecimal(properties[4]).abs();
			BigDecimal commission = new BigDecimal(properties[5]);
			String orderId = properties[6];
			String voucherNo = properties[7];
			String longShort = properties.length >= 9 ? properties[8] : "LONG";

			StockTransaction stockTransaction = new StockTransaction(symbol,
					activityDate,
					price,
					quantity,
					transactionType,
					commission,
					longShort,
					orderId,
					voucherNo
					);

			items.add(stockTransaction);
		}

		return items;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Date getActivityDate() {
		return activityDate;
	}

	public void setActivityDate(Date activityDate) {
		this.activityDate = activityDate;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public BigDecimal getQuantity() {
		return quantity;
	}

	public void setQuantity(BigDecimal quantity) {
		this.quantity = quantity;
	}

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public BigDecimal getCommission() {
		return commission;
	}

	public void setCommission(BigDecimal commission) {
		this.commission = commission;
	}

	public String getLongShort() {
		return longShort;
	}

	public void setLongShort(String longShort) {
		this.longShort = longShort;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getVoucherNo() {
		return voucherNo;
	}

	public void setVoucherNo(String voucherNo) {
		this.voucherNo = voucherNo;
	}

	@Override
	public String toString() {
		return "StockTransaction [symbol=" + symbol + ", activityDate=" + activityDate + ", price=" + price
				+ ", quantity=" + quantity + ", transactionType=" + transactionType + ", commission=" + commission
				+ ", longShort=" + longShort + ", orderId=" + orderId + ", voucherNo="
				+ voucherNo + "]";
	}

}
