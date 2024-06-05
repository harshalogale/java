package calculator;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import models.CurrencyConversionRate;
import models.StockTransaction;
import utils.TextFileReader;

public class ClosingBalanceCalculator {

	//static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static SimpleDateFormat dateFormatter_yyyyMM = new SimpleDateFormat("yyyy-MM");
	static SimpleDateFormat dateFormatter_yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");
	final static BigDecimal zero = new BigDecimal(0);

	final static boolean shouldPrintSellBuyPairs = true;

	public static void main(String[] args) throws ParseException {
		//Parsing the given String to Date object
		String[] dateStart = {"2020-04-01", "2020-06-16", "2020-09-16", "2020-12-16", "2021-03-16"};
		String[] dateEnd = {"2020-06-15", "2020-09-15", "2020-12-15", "2021-03-15", "2021-03-31"};
		
		// Quarter-wise
		dateStart = new String[]{"2021-04-01", "2021-06-16", "2021-09-16", "2021-12-16", "2022-03-16"};
		dateEnd   = new String[]{"2021-06-15", "2021-09-15", "2021-12-15", "2022-03-15", "2022-03-31"};
		
		// Annual
		dateStart = new String[]{"2024-04-01"};
		dateEnd = new String[]{"2025-03-31"};
		
		List<Date> dateRangeFrom = new ArrayList<Date>();
		List<Date> dateRangeTo = new ArrayList<Date>();

		Calendar c = GregorianCalendar.getInstance();

		for (int i = 0; i < dateStart.length; ++i) {
			Date dateFrom = dateFormatter_yyyyMMdd.parse(dateStart[i]);
			Date dateTo = dateFormatter_yyyyMMdd.parse(dateEnd[i]);
			c.setTime(dateTo);
			c.add(Calendar.DATE, 1);
			dateTo = c.getTime();
			dateRangeFrom.add(dateFrom);
			dateRangeTo.add(dateTo);
		}

		String fileNameUSDINR = "usd_inr_yyyyMM.csv";
		ClassLoader classLoader = ClosingBalanceCalculator.class.getClassLoader();
		final File fileUSDINR = new File(classLoader.getResource(fileNameUSDINR).getFile());
		final List<String> fileContentsUSDINR = TextFileReader.readLines(fileUSDINR);
		TreeMap<String, BigDecimal> ratesUSDINR = null;

		try {
			ratesUSDINR = CurrencyConversionRate.parseCSV(fileContentsUSDINR);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		final String fileName = "transactions.csv";
		final File file = new File(classLoader.getResource(fileName).getFile());
		final List<String> fileContents = TextFileReader.readLines(file);
		List<StockTransaction> transactions = null;

		final TreeMap<String, BigDecimal> rates = ratesUSDINR;

		boolean shouldConvertCurrency = null != rates;

		try {
			transactions = StockTransaction.parseCSV(fileContents);
		} catch (ParseException e) {
			System.err.println("Unable to calculate capital gain. Error while parsing transactions CSV file.");
			e.printStackTrace();
			return;
		}

		if (shouldConvertCurrency) {
			transactions.stream().forEach(t -> {
				Calendar cal = Calendar.getInstance();
				cal.setTime(t.getActivityDate());
				cal.add(Calendar.MONTH, -1);
				Date convDate = cal.getTime();
				String lastAvailableDate = dateFormatter_yyyyMM.format(convDate);
				BigDecimal conversionRate = rates.get(lastAvailableDate);
				BigDecimal convertedPrice = t.getPrice().multiply(conversionRate);
				BigDecimal convertedCommission = t.getCommission().multiply(conversionRate);

				t.setPrice(convertedPrice);
				t.setCommission(convertedCommission);
				//t.setCurrencyConversionDate(lastAvailableDate);
				//t.setCurrencyConversionRate(conversionRate);
			});
		}

		ClosingBalanceCalculator calc = new ClosingBalanceCalculator();
		calc.calculateClosingBalance(transactions, dateRangeFrom, dateRangeTo);
	}

	public BigDecimal calculateClosingBalance(List<StockTransaction> transactions, List<Date> dateRangeFrom, List<Date> dateRangeTo) {
		Date reportEndDate = dateRangeTo.get(dateRangeTo.size() - 1);

		transactions = transactions.stream()
				.filter(entry -> entry.getActivityDate().before(reportEndDate))
				.collect(Collectors.toList());

		System.out.println("LONG opening balance:");
		calculateOpeningBalanceForLong(transactions.stream()
			.filter(entry -> entry.getLongShort().equalsIgnoreCase("LONG"))
			.collect(Collectors.toList()), dateRangeFrom);
		System.out.println();
		System.out.println("LONG closing balance:");
		calculateOpeningBalanceForLong(transactions.stream()
				.filter(entry -> entry.getLongShort().equalsIgnoreCase("LONG"))
				.collect(Collectors.toList()), dateRangeTo);
		System.out.println();
		System.out.println("SHORT opening balance:");
		calculateOpeningBalanceForShort(transactions.stream()
			.filter(entry -> entry.getLongShort().equalsIgnoreCase("SHORT"))
			.collect(Collectors.toList()), dateRangeFrom);
		System.out.println();
		System.out.println("SHORT closing balance:");
		calculateOpeningBalanceForShort(transactions.stream()
				.filter(entry -> entry.getLongShort().equalsIgnoreCase("SHORT"))
				.collect(Collectors.toList()), dateRangeTo);

		return zero;
	}

	public BigDecimal calculateOpeningBalanceForLong(List<StockTransaction> transactions, List<Date> startDates) {
		TreeSet<String> symbols = new TreeSet<>();

		transactions.stream()
			.forEach(entry -> symbols.add(entry.getSymbol()));

		Map<String, Map<Date, BigDecimal>> symbolDateOpeningBalance = new HashMap<String, Map<Date,BigDecimal>>();

		for (String symbol: symbols) {
			System.out.println(symbol);

			Map<Date, BigDecimal> dateOpeningBalance = new HashMap<Date, BigDecimal>();
			for (Date reportStartDate: startDates) {
				Comparator<StockTransaction> compareByDateType = Comparator
						.comparing(StockTransaction::getActivityDate)
						.thenComparing(StockTransaction::getTransactionType);

				List<StockTransaction> sortedTransactions = transactions
						.stream()
						.filter(entry -> entry.getSymbol().equals(symbol))
						.sorted(compareByDateType)
						.collect(Collectors.toList());

				// opening balance -- sum of stock-qty up to the last day prior to the reportStartDate
				List<StockTransaction> openingTransactions = sortedTransactions.stream()
						.filter(entry -> entry.getActivityDate().before(reportStartDate))
						.collect(Collectors.toList());

				final BigDecimal openingQty = openingTransactions.stream()
						.map(entry -> entry.getQuantity().multiply(new BigDecimal(entry.getTransactionType().equals("Sell") ? -1 : 1)))
						.reduce(BigDecimal.ZERO, BigDecimal::add);

				BigDecimal remainingQty = openingQty;

				List<StockTransaction> openingBuyTransactions = openingTransactions.stream()
						.filter(entry -> entry.getActivityDate().before(reportStartDate) && entry.getTransactionType().equals("Buy"))
						.collect(Collectors.toList());

				BigDecimal openingAmt = new BigDecimal(0);

				for (int i = openingBuyTransactions.size() - 1; i >= 0; --i) {
					StockTransaction buyEntry = openingBuyTransactions.get(i);

					BigDecimal buyQty = buyEntry.getQuantity();

					if (remainingQty.compareTo(buyQty) >= 0) {
						remainingQty = remainingQty.subtract(buyQty);
						BigDecimal buyAmt = buyQty.multiply(buyEntry.getPrice()).add(buyEntry.getCommission());
						openingAmt = openingAmt.add(buyAmt);
					} else {
						BigDecimal consumedCommission = buyEntry.getCommission().multiply(remainingQty).divide(buyEntry.getQuantity(), 3, RoundingMode.HALF_UP);
						BigDecimal buyAmt = buyEntry.getPrice().multiply(remainingQty).add(consumedCommission);
						openingAmt = openingAmt.add(buyAmt);
						break;
					}
				}

				System.out.println(dateFormatter_yyyyMMdd.format(reportStartDate)
						+ "\t"+ openingQty.intValue()
						+ "\t"+ openingAmt);

				dateOpeningBalance.put(reportStartDate, openingAmt);
			}
			symbolDateOpeningBalance.put(symbol, dateOpeningBalance);
		}

		return zero;
	}


	public BigDecimal calculateOpeningBalanceForShort(List<StockTransaction> transactions, List<Date> startDates) {
		TreeSet<String> symbols = new TreeSet<>();

		transactions.stream()
			.forEach(entry -> symbols.add(entry.getSymbol()));

		Map<String, Map<Date, BigDecimal>> symbolDateOpeningBalance = new HashMap<String, Map<Date,BigDecimal>>();

		for (String symbol: symbols) {
			System.out.println(symbol);

			Map<Date, BigDecimal> dateOpeningBalance = new HashMap<Date, BigDecimal>();
			for (Date reportStartDate: startDates) {
				Comparator<StockTransaction> compareByDateType = Comparator
						.comparing(StockTransaction::getActivityDate)
						.thenComparing(StockTransaction::getTransactionType);

				List<StockTransaction> sortedTransactions = transactions
						.stream()
						.filter(entry -> entry.getSymbol().equals(symbol))
						.sorted(compareByDateType)
						.collect(Collectors.toList());

				// opening balance -- sum of stock-qty up to the last day prior to the reportStartDate
				List<StockTransaction> openingTransactions = sortedTransactions.stream()
						.filter(entry -> entry.getActivityDate().before(reportStartDate))
						.collect(Collectors.toList());

				final BigDecimal openingQty = openingTransactions.stream()
						.map(entry -> entry.getQuantity().multiply(new BigDecimal(entry.getTransactionType().equals("Sell") ? -1 : 1)))
						.reduce(BigDecimal.ZERO, BigDecimal::add);

				BigDecimal remainingQty = openingQty;

				List<StockTransaction> openingSellTransactions = openingTransactions.stream()
						.filter(entry -> entry.getActivityDate().before(reportStartDate) && entry.getTransactionType().equals("Sell"))
						.collect(Collectors.toList());

				BigDecimal openingAmt = new BigDecimal(0);

				for (int i = openingSellTransactions.size() - 1; i >= 0; --i) {
					StockTransaction sellEntry = openingSellTransactions.get(i);

					BigDecimal sellQty = sellEntry.getQuantity().multiply(new BigDecimal(-1));

					if (remainingQty.compareTo(sellQty) <= 0) {
						remainingQty = remainingQty.subtract(sellQty);
						BigDecimal sellAmt = sellQty.multiply(sellEntry.getPrice()).subtract(sellEntry.getCommission());
						openingAmt = openingAmt.add(sellAmt);
					} else {
						BigDecimal consumedCommission = sellEntry.getCommission().multiply(remainingQty).divide(sellEntry.getQuantity(), 3, RoundingMode.HALF_UP);
						BigDecimal sellAmt = sellEntry.getPrice().multiply(remainingQty).subtract(consumedCommission);
						openingAmt = openingAmt.add(sellAmt);
						break;
					}
				}

				System.out.println(dateFormatter_yyyyMMdd.format(reportStartDate)
						+ "\t" + openingQty.intValue()
						+ "\t" + openingAmt);

				dateOpeningBalance.put(reportStartDate, openingAmt);
			}
			symbolDateOpeningBalance.put(symbol, dateOpeningBalance);
		}

		return zero;
	}
}
