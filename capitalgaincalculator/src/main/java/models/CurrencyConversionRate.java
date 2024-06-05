package models;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

public class CurrencyConversionRate {
	private String from;
	private String to;
	private TreeMap<Date, Double> rates;

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public TreeMap<Date, Double> getRates() {
		return rates;
	}

	public void setRates(TreeMap<Date, Double> rates) {
		this.rates = rates;
	}

	public static TreeMap<String, BigDecimal> parseCSV(List<String> csv) throws ParseException {
		TreeMap<String, BigDecimal> conversionRates = new TreeMap<>();

		for (String line : csv) {
			// ex. 2020-03-22,74.25
			String[] properties = line.split(",");

			String dateKey = properties[0];
			BigDecimal rate = new BigDecimal(properties[1]);

			conversionRates.put(dateKey, rate);
		}

		return conversionRates;
	}
}
