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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import models.CurrencyConversionRate;
import models.StockTransaction;
import utils.TextFileReader;

public class GainLossCalculator {

	//static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static SimpleDateFormat dateFormatter_yyyyMM = new SimpleDateFormat("yyyy-MM");
	static SimpleDateFormat dateFormatter_yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");
	final static BigDecimal zero = new BigDecimal(0);

	final static boolean shouldPrintSellBuyPairs = true;

	public static void main(String[] args) throws ParseException {
		//Parsing the given String to Date object
		String[] dateStart = {"2020-04-01", "2020-06-16", "2020-09-16", "2020-12-16", "2021-03-16"};
		String[] dateEnd   = {"2020-06-15", "2020-09-15", "2020-12-15", "2021-03-15", "2021-03-31"};

		dateStart = new String[]{"2024-04-01", "2024-05-01", "2024-06-01", "2024-06-16", "2024-07-01", "2024-08-01", "2024-09-01", "2024-09-16", "2024-10-01", "2024-11-01", "2024-12-01", "2024-12-16", "2025-01-01", "2025-02-01", "2025-03-01", "2025-03-16"};
		dateEnd   = new String[]{"2024-04-30", "2024-05-31", "2024-06-15", "2024-06-30", "2024-07-31", "2024-08-31", "2024-09-15", "2024-09-30", "2024-10-31", "2024-11-30", "2024-12-15", "2024-12-31", "2025-01-31", "2025-02-28", "2025-03-15", "2025-03-31"};

		List<Date> dateRangeFrom = new ArrayList<Date>();
		List<Date> dateRangeTo = new ArrayList<Date>();

		Calendar c = GregorianCalendar.getInstance();

		for (int i = 0; i < dateStart.length; ++i) {
			Date dateFrom = dateFormatter_yyyyMMdd.parse(dateStart[i]);
			Date dateTo = dateFormatter_yyyyMMdd.parse(dateEnd[i]);
			c.setTime(dateTo);
			c.add(Calendar.HOUR, 23);
			c.add(Calendar.MINUTE, 59);
			c.add(Calendar.SECOND, 59);
			c.add(Calendar.MILLISECOND, 999);
			dateTo = c.getTime();
			dateRangeFrom.add(dateFrom);
			dateRangeTo.add(dateTo);
		}

		String fileNameUSDINR = "usd_inr_yyyyMMdd.csv";
		ClassLoader classLoader = GainLossCalculator.class.getClassLoader();
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
		List<StockTransaction> transactions1 = new ArrayList<>();

		final TreeMap<String, BigDecimal> rates = ratesUSDINR;

		boolean shouldConvertCurrency = false;//null != rates;

		boolean shouldSeparateCommission = false;

		try {
			transactions = StockTransaction.parseCSV(fileContents);
			transactions1 = StockTransaction.parseCSV(fileContents);
		} catch (ParseException e) {
			System.err.println("Unable to calculate capital gain. Error while parsing transactions CSV file.");
			e.printStackTrace();
			return;
		}

		if (shouldConvertCurrency) {
			transactions.stream().forEach( t -> {
				Calendar cal = Calendar.getInstance();
				cal.setTime(t.getActivityDate());
				Date convDate = cal.getTime();
				String ratesKey = dateFormatter_yyyyMMdd.format(convDate);
				BigDecimal conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
				BigDecimal convertedPrice = t.getPrice().multiply(conversionRate);
				BigDecimal convertedCommission = t.getCommission().multiply(conversionRate);
				t.setPrice(convertedPrice);
				t.setCommission(convertedCommission);
			});

			transactions1.stream().forEach( t -> {
				Calendar cal = Calendar.getInstance();
				cal.setTime(t.getActivityDate());
				Date convDate = cal.getTime();
				String ratesKey = dateFormatter_yyyyMMdd.format(convDate);
				BigDecimal conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
				BigDecimal convertedPrice = t.getPrice().multiply(conversionRate);
				BigDecimal convertedCommission = t.getCommission().multiply(conversionRate);
				t.setPrice(convertedPrice);
				t.setCommission(convertedCommission);
			});
		}

		GainLossCalculator calc = new GainLossCalculator();
		BigDecimal totalGain = calc.calculateGainLoss(transactions, dateRangeFrom, dateRangeTo, rates, shouldConvertCurrency, shouldSeparateCommission);
		System.out.println();
		System.out.println("Capital Gain\t" + totalGain);

		System.out.println();
		calc.calculateTotalBuysSells(transactions1, dateRangeFrom, dateRangeTo);
	}

	private void calculateTotalBuysSells(List<StockTransaction> transactions,
										 List<Date> dateRangeFrom,
										 List<Date> dateRangeTo) {
		System.out.println();
		System.out.println("Per date range Buys and Sells:");
		System.out.println("From\tTo\tTotal Buy Qty\tTotal Sell Qty\tTotal Buy Amt\tTotal Sell Amt");
		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			Date reportStartDate = dateRangeFrom.get(i);
			Date reportEndDate = dateRangeTo.get(i);

			BigDecimal qtyBuys = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Buy")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.map(entry -> entry.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add);

			BigDecimal qtySells = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Sell")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.map(entry -> entry.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add);

			BigDecimal amtBuys = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Buy")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.map(entry -> {
						return entry.getPrice().multiply(entry.getQuantity()).add(entry.getCommission());
					})
					.reduce(BigDecimal.ZERO, BigDecimal::add);

			BigDecimal amtSells = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Sell")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.map(entry -> {
						return entry.getPrice().multiply(entry.getQuantity()).subtract(entry.getCommission());
					})
					.reduce(BigDecimal.ZERO, BigDecimal::add);

			System.out.println(dateFormatter_yyyyMMdd.format(reportStartDate)
					+ "\t" + dateFormatter_yyyyMMdd.format(reportEndDate)
					+ "\t" + qtyBuys
					+ "\t" + qtySells
					+ "\t" + amtBuys
					+ "\t" + amtSells
					);
		}
		System.out.println();
		System.out.println("---------");

		Date firstStartDate = dateRangeFrom.get(0);
		Date lastEndDate = dateRangeTo.get(dateRangeTo.size() - 1);

		System.out.println("Per month Buys and Sells:");
		System.out.println("Month\tTotal Buy Qty\tTotal Sell Qty\tTotal Buy Amt\tTotal Sell Amt");

		Map<String, BigDecimal> mapMonthBuyQty = new TreeMap<>();
		Map<String, BigDecimal> mapMonthBuyVal = new TreeMap<>();

		transactions.stream()
		.filter(entry -> entry.getTransactionType().equals("Buy")
				&& entry.getActivityDate().compareTo(firstStartDate) >= 0
				&& entry.getActivityDate().compareTo(lastEndDate) <= 0)
		.forEach(entry -> {
			BigDecimal newQty = entry.getQuantity();
			String monthYear = dateFormatter_yyyyMM.format(entry.getActivityDate());
			BigDecimal cumulativeQtyForMonth = Optional.ofNullable(mapMonthBuyQty.get(monthYear)).orElse(zero).add(newQty);
			mapMonthBuyQty.put(monthYear, cumulativeQtyForMonth);
		});

		transactions.stream()
		.filter(entry -> entry.getTransactionType().equals("Buy")
				&& entry.getActivityDate().compareTo(firstStartDate) >= 0
				&& entry.getActivityDate().compareTo(lastEndDate) <= 0)
		.forEach(entry -> {
			BigDecimal newVal = entry.getPrice().multiply(entry.getQuantity()).add(entry.getCommission());
			String monthYear = dateFormatter_yyyyMM.format(entry.getActivityDate());
			BigDecimal cumulativeValForMonth = Optional.ofNullable(mapMonthBuyVal.get(monthYear)).orElse(zero).add(newVal);
			mapMonthBuyVal.put(monthYear, cumulativeValForMonth);
		});

		Map<String, BigDecimal> mapMonthSellQty = new TreeMap<>();
		Map<String, BigDecimal> mapMonthSellVal = new TreeMap<>();

		transactions.stream()
		.filter(entry -> entry.getTransactionType().equals("Sell")
				&& entry.getActivityDate().compareTo(firstStartDate) >= 0
				&& entry.getActivityDate().compareTo(lastEndDate) <= 0)
		.forEach(entry -> {
			BigDecimal newQty = entry.getQuantity();
			String monthYear = dateFormatter_yyyyMM.format(entry.getActivityDate());
			BigDecimal cumulativeQtyForMonth = Optional.ofNullable(mapMonthSellQty.get(monthYear)).orElse(zero).add(newQty);
			mapMonthSellQty.put(monthYear, cumulativeQtyForMonth);
		});

		transactions.stream()
		.filter(entry -> entry.getTransactionType().equals("Sell")
				&& entry.getActivityDate().compareTo(firstStartDate) >= 0
				&& entry.getActivityDate().compareTo(lastEndDate) <= 0)
		.forEach(entry -> {
			BigDecimal newVal = entry.getPrice().multiply(entry.getQuantity()).subtract(entry.getCommission());
			String monthYear = dateFormatter_yyyyMM.format(entry.getActivityDate());
			BigDecimal cumulativeValForMonth = Optional.ofNullable(mapMonthSellVal.get(monthYear)).orElse(zero).add(newVal);
			mapMonthSellVal.put(monthYear, cumulativeValForMonth);
		});

		TreeSet<String> allMonthKeys = new TreeSet<>();
		allMonthKeys.addAll(mapMonthBuyQty.keySet());
		allMonthKeys.addAll(mapMonthBuyVal.keySet());
		allMonthKeys.addAll(mapMonthSellQty.keySet());
		allMonthKeys.addAll(mapMonthSellVal.keySet());

		allMonthKeys.forEach(key ->
			System.out.println(key
				+ "\t" + Optional.ofNullable(mapMonthBuyQty.get(key)).orElse(zero)
				+ "\t" + Optional.ofNullable(mapMonthSellQty.get(key)).orElse(zero)
				+ "\t" + Optional.ofNullable(mapMonthBuyVal.get(key)).orElse(zero)
				+ "\t" + Optional.ofNullable(mapMonthSellVal.get(key)).orElse(zero)));

		System.out.println("---------");
		System.out.println();
		System.out.println("Per Symbol Per Date Range Buys and Sells:");
		System.out.println("From\tTo\tSymbol\tTotal Buy Qty\tTotal Sell Qty\tTotal Buy Amt\tTotal Sell Amt");
		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			Date reportStartDate = dateRangeFrom.get(i);
			Date reportEndDate = dateRangeTo.get(i);

			Map<String, List<StockTransaction>> groupedBuys = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Buy")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.collect(Collectors.groupingBy(StockTransaction::getSymbol));
			Map<String, BigDecimal> symbolQtyBuys = new TreeMap<>();
			Map<String, BigDecimal> symbolValueBuys = new TreeMap<>();

			groupedBuys.forEach((key, value) -> {
				symbolQtyBuys.put(key, value.stream()
						.map(entry -> entry.getQuantity())
						.reduce(BigDecimal.ZERO, BigDecimal::add));
				symbolValueBuys.put(key, value.stream()
						.map(entry -> entry.getPrice().multiply(entry.getQuantity()).add(entry.getCommission()))
						.reduce(BigDecimal.ZERO, BigDecimal::add));
			});

			Map<String, List<StockTransaction>> groupedSells = transactions.stream()
					.filter(entry -> entry.getTransactionType().equals("Sell")
							&& entry.getActivityDate().compareTo(reportStartDate) >= 0
							&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
					.collect(Collectors.groupingBy(StockTransaction::getSymbol));

			Map<String, BigDecimal> symbolQtySells = new TreeMap<>();
			Map<String, BigDecimal> symbolValueSells = new TreeMap<>();

			groupedSells.forEach((key, value) -> {
				symbolQtySells.put(key, value.stream()
						.map(entry -> entry.getQuantity())
						.reduce(BigDecimal.ZERO, BigDecimal::add));
				symbolValueSells.put(key, value.stream()
						.map(entry -> entry.getPrice().multiply(entry.getQuantity()).subtract(entry.getCommission()))
						.reduce(BigDecimal.ZERO, BigDecimal::add));
			});

			TreeSet<String> allSymbols = new TreeSet<>(groupedBuys.keySet());
			allSymbols.addAll(groupedSells.keySet());

			allSymbols.forEach(key ->
				System.out.println(dateFormatter_yyyyMMdd.format(reportStartDate)
					+ "\t" + dateFormatter_yyyyMMdd.format(reportEndDate)
					+ "\t" + key
					+ "\t" + (symbolQtyBuys.containsKey(key) ? symbolQtyBuys.get(key) : 0)
					+ "\t" + (symbolQtySells.containsKey(key) ? symbolQtySells.get(key) : 0)
					+ "\t" + (symbolValueBuys.containsKey(key) ? symbolValueBuys.get(key) : 0)
					+ "\t" + (symbolValueSells.containsKey(key) ? symbolValueSells.get(key) : 0)));
		}

		System.out.println("---------");
		System.out.println();
		System.out.println("Per Symbol Buys and Sells:");
		System.out.println("Symbol\tTotal Buy Qty\tTotal Sell Qty\tTotal Buy Amt\tTotal Sell Amt");
		Date reportStartDate = dateRangeFrom.get(0);
		Date reportEndDate = dateRangeTo.get(dateRangeTo.size() - 1);

		Map<String, List<StockTransaction>> groupedBuys = transactions.stream()
				.filter(entry -> entry.getTransactionType().equals("Buy")
						&& entry.getActivityDate().compareTo(reportStartDate) >= 0
						&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
				.collect(Collectors.groupingBy(StockTransaction::getSymbol));
		Map<String, BigDecimal> symbolQtyBuys = new TreeMap<>();
		Map<String, BigDecimal> symbolValueBuys = new TreeMap<>();

		groupedBuys.forEach((key, value) -> {
			symbolQtyBuys.put(key, value.stream()
					.map(entry -> entry.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add));
			symbolValueBuys.put(key, value.stream()
					.map(entry -> {
						return entry.getPrice().multiply(entry.getQuantity()).add(entry.getCommission());
					})
					.reduce(BigDecimal.ZERO, BigDecimal::add));
		});

		Map<String, List<StockTransaction>> groupedSells = transactions.stream()
				.filter(entry -> entry.getTransactionType().equals("Sell")
						&& entry.getActivityDate().compareTo(reportStartDate) >= 0
						&& entry.getActivityDate().compareTo(reportEndDate) <= 0)
				.collect(Collectors.groupingBy(StockTransaction::getSymbol));

		Map<String, BigDecimal> symbolQtySells = new TreeMap<>();
		Map<String, BigDecimal> symbolValueSells = new TreeMap<>();

		groupedSells.forEach((key, value) -> {
			symbolQtySells.put(key, value.stream()
					.map(entry -> entry.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add));
			symbolValueSells.put(key, value.stream()
					.map(entry -> entry.getPrice().multiply(entry.getQuantity()).subtract(entry.getCommission()))
					.reduce(BigDecimal.ZERO, BigDecimal::add));
		});

		TreeSet<String> allSymbols = new TreeSet<>(groupedBuys.keySet());
		allSymbols.addAll(groupedSells.keySet());

		allSymbols.forEach(key ->
			System.out.println(key
				+ "\t" + (symbolQtyBuys.containsKey(key) ? symbolQtyBuys.get(key) : 0)
				+ "\t" + (symbolQtySells.containsKey(key) ? symbolQtySells.get(key) : 0)
				+ "\t" + (symbolValueBuys.containsKey(key) ? symbolValueBuys.get(key) : 0)
				+ "\t" + (symbolValueSells.containsKey(key) ? symbolValueSells.get(key) : 0)));
	}

	public BigDecimal calculateGainLoss(List<StockTransaction> transactions,
										List<Date> dateRangeFrom,
										List<Date> dateRangeTo,
										TreeMap<String, BigDecimal> rates,
										boolean shouldConvertCurrency,
										boolean shouldSeparateCommission) {
		Date reportEndDate = dateRangeTo.get(dateRangeTo.size() - 1);
		transactions = transactions.stream()
				.filter(entry -> entry.getActivityDate().before(reportEndDate))
				.collect(Collectors.toList());

		List<StockTransaction> longTransactions = transactions.stream()
												  .filter(entry -> entry.getLongShort().equalsIgnoreCase("LONG"))
												  .collect(Collectors.toList());
		BigDecimal longCG = longTransactions.isEmpty()
							? zero
							: calculateGainLossForLong(longTransactions,
													   dateRangeFrom,
													   dateRangeTo,
													   rates,
													   shouldConvertCurrency,
													   shouldSeparateCommission);
		System.out.println("LONG CG\t" + longCG);
		System.out.println();

		List<StockTransaction> shortTransactions = transactions.stream()
												   .filter(entry -> entry.getLongShort().equalsIgnoreCase("SHORT"))
												   .collect(Collectors.toList());
		BigDecimal shortCG = shortTransactions.isEmpty()
							 ? zero
							 : calculateGainLossForShort(shortTransactions,
														 dateRangeFrom,
														 dateRangeTo,
														 rates,
														 shouldConvertCurrency,
														 shouldSeparateCommission);
		System.out.println("SHORT CG\t" + shortCG);
		System.out.println();

		BigDecimal gain = longCG.add(shortCG);
		return gain;
	}

	public BigDecimal calculateGainLossForLong(List<StockTransaction> transactions,
											   List<Date> dateRangeFrom,
											   List<Date> dateRangeTo,
											   TreeMap<String, BigDecimal> rates,
											   boolean shouldConvertCurrency,
											   boolean shouldSeparateCommission) {
		Date reportStartDate = dateRangeFrom.get(0);
		Date reportEndDate = dateRangeTo.get(dateRangeTo.size() - 1);
		Comparator<StockTransaction> compareByDateType = Comparator
				.comparing(StockTransaction::getActivityDate)
				.thenComparing(StockTransaction::getTransactionType);
		List<StockTransaction> sortedTransactions = transactions
				.stream()
				.sorted(compareByDateType)
				.collect(Collectors.toList());

		TreeMap<String, Queue<StockTransaction>> queueMap = new TreeMap<>();
		TreeMap<String, BigDecimal> mapSymbolGain = new TreeMap<>();
		TreeMap<String, BigDecimal> mapSymbolCommission = new TreeMap<>();
		TreeMap<String, BigDecimal> mapDateRangeGain = new TreeMap<>();
		TreeMap<String, BigDecimal> mapDateRangeCommission = new TreeMap<>();
		TreeMap<String, BigDecimal> mapMonthGain = new TreeMap<>();
		TreeMap<String, BigDecimal> mapMonthCommission = new TreeMap<>();

		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
			mapDateRangeGain.put(key, new BigDecimal(0));
		}
		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
			mapDateRangeCommission.put(key, new BigDecimal(0));
		}
		Map<String, BigDecimal> positions = new HashMap<>();
		int lastMonth = -1;
		BigDecimal gain = new BigDecimal(0);
		BigDecimal gainUSD = new BigDecimal(0);
		BigDecimal commission = new BigDecimal(0);
		BigDecimal commissionUSD = new BigDecimal(0);
		BigDecimal turnoverIntraday = new BigDecimal(0);
		BigDecimal turnoverDelivery = new BigDecimal(0);

		System.out.println("--------- LONG ---------");
		if (shouldPrintSellBuyPairs) {
			String[] arr = {"Symbol",
							"TotalSaleQty",
							"BuyPrice",
							"SellPrice",
							"CloseDate",
							"OpenDate",
							"PartialQtySold",
							"CapitalGain",
							"CommissionSell",
							"CommissionTotal",
							"BuyOrderId",
							"SellOrderID",
							"BuyVoucherNo",
							"SellVoucherNo",
							"BuyExchangeRate",
							"SellExchangeRate",
							"Intraday",
							"Turnover"};
			String headerRow = String.join("\t", arr);
			System.out.println(headerRow);
		}

		for (StockTransaction transaction : sortedTransactions) {
			Queue<StockTransaction> queueBuys = queueMap.get(transaction.getSymbol());
			if (queueBuys == null) {
				queueBuys = new LinkedList<StockTransaction>();
				queueMap.put(transaction.getSymbol(), queueBuys);
			}

			BigDecimal cumulativeQty = Optional.ofNullable(positions.get(transaction.getSymbol())).orElse(zero);

			if (transaction.getTransactionType().equalsIgnoreCase("BUY")) {
				queueBuys.add(transaction);
				cumulativeQty = cumulativeQty.add(transaction.getQuantity());
			} else {
				cumulativeQty = cumulativeQty.subtract(transaction.getQuantity());
				StockTransaction sellEntry = transaction;
				BigDecimal remainingSellQty = sellEntry.getQuantity();

				while (remainingSellQty.compareTo(zero) > 0 && !queueBuys.isEmpty()) {
					StockTransaction buyEntry = queueBuys.peek();
					BigDecimal buyQty = buyEntry.getQuantity();
					BigDecimal consumedQty = zero;
					BigDecimal consumedCommission = zero;
					BigDecimal consumedSellCommission = zero;

					// Some possible scenarios:
					// buy 10, sell 10
					// buy 30, sell 10
					// buy 10, buy 10, sell 20
					// buy 10, buy 20, sell 20

					if (remainingSellQty.compareTo(buyQty) >= 0) {
						queueBuys.remove();
						consumedQty = buyQty;
						remainingSellQty = remainingSellQty.subtract(consumedQty);
						consumedSellCommission = sellEntry.getCommission().multiply(consumedQty).divide(sellEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedCommission = buyEntry.getCommission().add(consumedSellCommission);
					} else {
						consumedQty = remainingSellQty;
						BigDecimal qtyRatio = consumedQty.divide(sellEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedSellCommission = sellEntry.getCommission().multiply(qtyRatio);
						BigDecimal partialBuyCommission = buyEntry.getCommission().multiply(consumedQty).divide(buyEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedCommission = consumedSellCommission.add(partialBuyCommission);
						buyEntry.setQuantity(buyQty.subtract(remainingSellQty));
						buyEntry.setCommission(buyEntry.getCommission().subtract(partialBuyCommission));
						remainingSellQty = zero;
					}
					String symbol = sellEntry.getSymbol();
					BigDecimal sellPrice = sellEntry.getPrice();
					BigDecimal buyPrice = buyEntry.getPrice();

					if (!sellEntry.getActivityDate().before (reportStartDate) && !sellEntry.getActivityDate().after (reportEndDate)) {
						BigDecimal priceDiff = sellPrice.subtract(buyPrice);

						BigDecimal newGain = consumedQty.multiply(priceDiff);
						if (!shouldSeparateCommission) {
							newGain = newGain.subtract(consumedCommission);
						}
						commissionUSD = commissionUSD.add(consumedCommission);
						gainUSD = gainUSD.add(newGain);
						BigDecimal conversionRate = new BigDecimal(1);
						String strConversionRate = "1\t1";

						BigDecimal newCommission = commissionUSD;

						if (shouldConvertCurrency) {
							Calendar cal = Calendar.getInstance();

							// buy date conversion rate
							cal.setTime(buyEntry.getActivityDate());
							Date convDate = cal.getTime();
							String ratesKey = dateFormatter_yyyyMMdd.format(convDate);
							conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
							strConversionRate = "" + conversionRate;

							// sell date conversion rate
							cal.setTime(sellEntry.getActivityDate());
							convDate = cal.getTime();
							ratesKey = dateFormatter_yyyyMMdd.format(convDate);
							conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
							strConversionRate = strConversionRate + "\t" + conversionRate;
						}
						gain = gain.add(newGain);
						commission = commission.add(newCommission);

						BigDecimal countSold = consumedQty.compareTo(sellEntry.getQuantity()) > 0 ? sellEntry.getQuantity() : consumedQty;
						String strBuyEntry = dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate());
						String strSellEntry = dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate());
						boolean isIntraday = strBuyEntry.equals(strSellEntry);

						BigDecimal turnover = isIntraday ? newGain.abs() : countSold.multiply(sellEntry.getPrice());

						if (isIntraday) {
							turnoverIntraday = turnoverIntraday.add(turnover);
						} else {
							turnoverDelivery = turnoverDelivery.add(turnover);
						}

						if (shouldPrintSellBuyPairs) {
							System.out.println(symbol
									+ "\t" + sellEntry.getQuantity()
									+ "\t" + buyPrice
									+ "\t" + sellPrice
									+ "\t" + dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate())
									+ "\t" + dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate())
									+ "\t" + countSold
									+ "\t" + newGain
									+ "\t" + consumedSellCommission
									+ "\t" + consumedCommission
									+ "\t" + buyEntry.getOrderId()
									+ "\t" + sellEntry.getOrderId()
									+ "\t" + buyEntry.getVoucherNo()
									+ "\t" + sellEntry.getVoucherNo()
									+ "\t" + strConversionRate
									+ "\t" + (isIntraday ? 1 : 0)
									+ "\t" + turnover);
						}

						BigDecimal cumulativeGainForSymbol = Optional.ofNullable(mapSymbolGain.get(symbol)).orElse(zero).add(newGain);
						mapSymbolGain.put(symbol, cumulativeGainForSymbol);
						BigDecimal cumulativeCommissionForSymbol = Optional.ofNullable(mapSymbolCommission.get(symbol)).orElse(zero).add(consumedCommission);
						mapSymbolCommission.put(symbol, cumulativeCommissionForSymbol);

						String monthYear = dateFormatter_yyyyMM.format(sellEntry.getActivityDate());
						BigDecimal cumulativeGainForMonth = Optional.ofNullable(mapMonthGain.get(monthYear)).orElse(zero).add(newGain);
						mapMonthGain.put(monthYear, cumulativeGainForMonth);
						BigDecimal cumulativeCommissionForMonth = Optional.ofNullable(mapMonthCommission.get(monthYear)).orElse(zero).add(consumedCommission);
						mapMonthCommission.put(monthYear, cumulativeCommissionForMonth);

						Date sellDate = sellEntry.getActivityDate();
						for (int i = 0; i < dateRangeFrom.size(); ++i) {
							Date dateFrom = dateRangeFrom.get(i);
							Date dateTo = dateRangeTo.get(i);
							if (!sellDate.before(dateFrom) && !sellDate.after(dateTo)) {
								String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
								mapDateRangeGain.put(key, mapDateRangeGain.get(key).add(newGain));
								mapDateRangeCommission.put(key, mapDateRangeCommission.get(key).add(consumedCommission));
								break;
							}
						}
					}
				}

				if (remainingSellQty.compareTo(zero) > 0) {
					System.err.println("Sell qty cannot be greater than current LONG position. Extra " + sellEntry.getSymbol() + ": " + remainingSellQty);
				}
			}

			Calendar cal = Calendar.getInstance();
			cal.setTime(transaction.getActivityDate());
			int curMonth = cal.get(Calendar.MONTH);
			if (lastMonth != curMonth) {
//				System.out.println("Positions at end of month: " + lastMonth);
//				System.out.println(positions);
				lastMonth = curMonth;
			}

			if (0 == cumulativeQty.compareTo(zero)) {
				positions.remove(transaction.getSymbol());
			} else {
				positions.put(transaction.getSymbol(), cumulativeQty);
			}
		}

		System.out.println("---------");
		System.out.println("Stock-in-hand");
		System.out.println("Symbol\tQty\tPrincipalAmt\tCommission");
		queueMap.forEach((k, v) -> {
			BigDecimal qty = v.stream()
					.map(t -> t.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			BigDecimal principalAmt = v.stream()
					.map(t -> t.getPrice().multiply(t.getQuantity()))
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			BigDecimal comm = v.stream()
					.map(t -> t.getCommission())
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			System.out.println(k + "\t" + qty + "\t" + principalAmt + "\t" + comm);
		});
		BigDecimal commissionStockInHand = queueMap
				.values()
				.stream()
				.map(q -> q.stream()
						.map(t -> t.getCommission())
						.reduce(BigDecimal.ZERO, BigDecimal::add))
				.reduce(BigDecimal.ZERO, BigDecimal::add);
		System.out.println("Commission for stock-in-hand\t" + commissionStockInHand);

		BigDecimal countStockInHand = queueMap
				.values()
				.stream()
				.map(q -> q.stream()
						.map(t -> t.getQuantity())
						.reduce(BigDecimal.ZERO, BigDecimal::add))
				.reduce(BigDecimal.ZERO, BigDecimal::add);
				System.out.println("Count of stock-in-hand\t" + countStockInHand);

		System.out.println("---------");
		System.out.println("Per Symbol");
		System.out.println("Symbol\tCapitalGain");
		mapSymbolGain.forEach((k, v) -> System.out.println(k + "\t" + v));
		System.out.println("---------");
		if (mapMonthGain.isEmpty()) {
			System.out.println("No LONG transactions");
		} else {
			System.out.println("Per Month");
			System.out.println("Month\tCapitalGain");
			mapMonthGain.forEach((k, v) -> System.out.println(k + "\t" + v));
			System.out.println("---------");
		}
		System.out.println("Per Date Range Gain");
		System.out.println("DateFrom\tDateTo\tType\tCapitalGain");
		mapDateRangeGain.forEach((k, v) -> System.out.println(k + "\tLONG\t" + v));
		System.out.println("---------");
		System.out.println("Per Date Range Commission");
		System.out.println("DateFrom\tDateTo\tType\tCommission");
		mapDateRangeCommission.forEach((k, v) -> System.out.println(k + "\tLONG\t" + v));
		System.out.println("---------");
		System.out.println((shouldConvertCurrency ? ("Gain converted\t" + gain + "\t") : "") + "Gain USD\t" + gainUSD);
		System.out.println("---------");
		System.out.println((shouldConvertCurrency ? ("Commission converted\t" + commission + "\t") : "") + "Commission USD\t" + commissionUSD);
		System.out.println("---------");
		System.out.println("Turnover Amt IntraDay\t" + turnoverIntraday);
		System.out.println("Turnover Amt Delivery\t" + turnoverDelivery);
		System.out.println("---------");

		return gain;
	}

	public BigDecimal calculateGainLossForShort(List<StockTransaction> transactions,
												List<Date> dateRangeFrom,
												List<Date> dateRangeTo,
												TreeMap<String, BigDecimal> rates,
												boolean shouldConvertCurrency,
												boolean shouldSeparateCommission) {
		Date reportStartDate = dateRangeFrom.get(0);
		Date reportEndDate = dateRangeTo.get(dateRangeTo.size() - 1);
		Comparator<StockTransaction> compareByDateType = Comparator
				.comparing(StockTransaction::getActivityDate)
				.thenComparing(StockTransaction::getTransactionType, Comparator.reverseOrder());
		List<StockTransaction> sortedTransactions = transactions
				.stream()
				.sorted(compareByDateType)
				.collect(Collectors.toList());

		Map<String, Queue<StockTransaction>> queueMap = new HashMap<>();
		Map<String, BigDecimal> mapSymbolGain = new HashMap<>();
		Map<String, BigDecimal> mapSymbolCommission = new HashMap<>();
		TreeMap<String, BigDecimal> mapDateRangeGain = new TreeMap<>();
		TreeMap<String, BigDecimal> mapDateRangeCommission = new TreeMap<>();
		TreeMap<String, BigDecimal> mapMonthGain = new TreeMap<>();
		TreeMap<String, BigDecimal> mapMonthCommission = new TreeMap<>();

		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
			mapDateRangeGain.put(key, new BigDecimal(0));
		}
		for (int i = 0; i < dateRangeFrom.size(); ++i) {
			String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
			mapDateRangeCommission.put(key, new BigDecimal(0));
		}
		Map<String, BigDecimal> positions = new HashMap<>();
		int lastMonth = -1;
		BigDecimal gain = new BigDecimal(0);
		BigDecimal gainUSD = new BigDecimal(0);
		BigDecimal commission = new BigDecimal(0);
		BigDecimal commissionUSD = new BigDecimal(0);
		BigDecimal turnoverIntraday = new BigDecimal(0);
		BigDecimal turnoverDelivery = new BigDecimal(0);

		System.out.println("--------- SHORT ---------");
		if (shouldPrintSellBuyPairs) {
			String[] arr = {"Symbol",
							"TotalBuyQty",
							"BuyPrice",
							"SellPrice",
							"CloseDate",
							"OpenDate",
							"PartialQtyBought",
							"CapitalGain",
							"CommissionBuy",
							"CommissionTotal",
							"BuyOrderId",
							"SellOrderID",
							"BuyVoucherNo",
							"SellVoucherNo",
							"BuyExchangeRate",
							"SellExchangeRate",
							"Intraday",
							"Turnover"};
			String headerRow = String.join("\t", arr);
			System.out.println(headerRow);
		}

		for (StockTransaction transaction : sortedTransactions) {
			Queue<StockTransaction> queue = queueMap.get(transaction.getSymbol());
			if (queue == null) {
				queue = new LinkedList<StockTransaction>();
				queueMap.put(transaction.getSymbol(), queue);
			}

			BigDecimal cumulativeQty = Optional.ofNullable(positions.get(transaction.getSymbol())).orElse(zero);

			if (transaction.getTransactionType().equalsIgnoreCase("SELL")) {
				queue.add(transaction);
				cumulativeQty = cumulativeQty.add(transaction.getQuantity());
			} else {
				cumulativeQty = cumulativeQty.subtract(transaction.getQuantity());
				StockTransaction buyEntry = transaction;
				BigDecimal remainingBuyQty = buyEntry.getQuantity();

				while (remainingBuyQty.compareTo(zero) > 0 && !queue.isEmpty()) {
					StockTransaction sellEntry = queue.peek();
					BigDecimal buyQty = remainingBuyQty;
					BigDecimal sellQty = sellEntry.getQuantity();
					BigDecimal consumedQty = zero;
					BigDecimal consumedCommission = zero;
					BigDecimal consumedBuyCommission = zero;

					if (buyQty.compareTo(sellQty) >= 0) {
						queue.remove();
						consumedQty = sellQty;
						remainingBuyQty = remainingBuyQty.subtract(consumedQty);
						consumedCommission = sellEntry.getCommission();
						consumedBuyCommission = buyEntry.getCommission().multiply(consumedQty).divide(buyEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedCommission = consumedCommission.add(consumedBuyCommission);
					} else {
						consumedQty = remainingBuyQty;
						BigDecimal qtyRatio = consumedQty.divide(buyEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedBuyCommission = buyEntry.getCommission().multiply(qtyRatio);
						BigDecimal partialSellCommission = sellEntry.getCommission().multiply(consumedQty).divide(sellEntry.getQuantity(), 4, RoundingMode.HALF_UP);
						consumedCommission = consumedBuyCommission.add(partialSellCommission);
						sellEntry.setQuantity(sellQty.subtract(buyQty));
						sellEntry.setCommission(sellEntry.getCommission().subtract(partialSellCommission));
						remainingBuyQty = zero;
					}
					String symbol = buyEntry.getSymbol();
					BigDecimal sellPrice = sellEntry.getPrice();
					BigDecimal buyPrice = buyEntry.getPrice();

					if (!buyEntry.getActivityDate().before (reportStartDate) && !buyEntry.getActivityDate().after (reportEndDate)) {
						BigDecimal priceDiff = sellPrice.subtract(buyPrice);
						BigDecimal newGain = consumedQty.multiply(priceDiff);
						if (!shouldSeparateCommission) {
							newGain = newGain.subtract(consumedCommission);
						}
						gainUSD = gainUSD.add(newGain);
						commissionUSD = commissionUSD.add(consumedCommission);

						BigDecimal conversionRate = new BigDecimal(1);
						String strConversionRate = "1";
						BigDecimal newCommission = commissionUSD;

						if (shouldConvertCurrency) {
							Calendar cal = Calendar.getInstance();

							// buy entry conversion rate
							cal.setTime(buyEntry.getActivityDate());
							Date convDate = cal.getTime();
							String ratesKey = dateFormatter_yyyyMMdd.format(convDate);
							conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
							strConversionRate = "" + conversionRate;
//							newGain = newGain.multiply(conversionRate);
//							newCommission = commissionUSD.multiply(conversionRate);

							// sell entry conversion rate
							cal.setTime(sellEntry.getActivityDate());
							convDate = cal.getTime();
							ratesKey = dateFormatter_yyyyMMdd.format(convDate);
							conversionRate = rates.containsKey(ratesKey) ? rates.get(ratesKey) : rates.lowerEntry(ratesKey).getValue();
							strConversionRate = strConversionRate + "\t" + conversionRate;
						}
						gain = gain.add(newGain);
						commission = commission.add(newCommission);

						BigDecimal countBought = consumedQty.compareTo(buyEntry.getQuantity()) >= 0 ? buyEntry.getQuantity() : consumedQty;
						String strBuyEntry = dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate());
						String strSellEntry = dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate());
						boolean isIntraday = strBuyEntry.equals(strSellEntry);

						BigDecimal turnover = isIntraday ? newGain.abs() : countBought.multiply(buyEntry.getPrice());

						if (isIntraday) {
							turnoverIntraday = turnoverIntraday.add(turnover);
						} else {
							turnoverDelivery = turnoverDelivery.add(turnover);
						}

						if (shouldPrintSellBuyPairs) {
							System.out.println(symbol
									+ "\t" + buyEntry.getQuantity()
									+ "\t" + buyPrice
									+ "\t" + sellPrice
									+ "\t" + dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate())
									+ "\t" + dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate())
									+ "\t" + countBought
									+ "\t" + newGain
									+ "\t" + consumedBuyCommission
									+ "\t" + consumedCommission
									+ "\t" + buyEntry.getOrderId()
									+ "\t" + sellEntry.getOrderId()
									+ "\t" + buyEntry.getVoucherNo()
									+ "\t" + sellEntry.getVoucherNo()
									+ "\t" + strConversionRate
									+ "\t" + (isIntraday ? 1 : 0)
									+ "\t" + turnover);
						}

						BigDecimal cumulativeGainForSymbol = Optional.ofNullable(mapSymbolGain.get(symbol)).orElse(zero).add(newGain);
						mapSymbolGain.put(symbol, cumulativeGainForSymbol);
						BigDecimal cumulativeCommissionForSymbol = Optional.ofNullable(mapSymbolCommission.get(symbol)).orElse(zero).add(consumedCommission);
						mapSymbolCommission.put(symbol, cumulativeCommissionForSymbol);

						String monthYear = dateFormatter_yyyyMM.format(buyEntry.getActivityDate());
						BigDecimal cumulativeGainForMonth = Optional.ofNullable(mapMonthGain.get(monthYear)).orElse(zero).add(newGain);
						mapMonthGain.put(monthYear, cumulativeGainForMonth);
						BigDecimal cumulativeCommissionForMonth = Optional.ofNullable(mapMonthCommission.get(monthYear)).orElse(zero).add(consumedCommission);
						mapMonthCommission.put(monthYear, cumulativeCommissionForMonth);

						Date buyDate = buyEntry.getActivityDate();
						for (int i = 0; i < dateRangeFrom.size(); ++i) {
							Date dateFrom = dateRangeFrom.get(i);
							Date dateTo = dateRangeTo.get(i);
							if (!buyDate.before(dateFrom) && !buyDate.after(dateTo)) {
								String key = dateFormatter_yyyyMMdd.format(dateRangeFrom.get(i)) + "\t" + dateFormatter_yyyyMMdd.format(dateRangeTo.get(i));
								mapDateRangeGain.put(key, mapDateRangeGain.get(key).add(newGain));
								mapDateRangeCommission.put(key, mapDateRangeCommission.get(key).add(consumedCommission));
								break;
							}
						}
					}
				}

				if (remainingBuyQty.compareTo(zero) > 0) {
					System.err.println("Buy qty cannot be greater than current SHORT position. Extra " + buyEntry.getSymbol() + ": " + remainingBuyQty);
				}
			}

			Calendar cal = Calendar.getInstance();
			cal.setTime(transaction.getActivityDate());
			int curMonth = cal.get(Calendar.MONTH);
			if (lastMonth != curMonth) {
//				System.out.println("Positions at end of month: " + lastMonth);
//				System.out.println(positions);
				lastMonth = curMonth;
			}

			if (0 == cumulativeQty.compareTo(zero)) {
				positions.remove(transaction.getSymbol());
			} else {
				positions.put(transaction.getSymbol(), cumulativeQty);
			}
		}

		System.out.println("---------");
		System.out.println("Stock-in-hand");
		System.out.println("Symbol\tQty\tPrincipalAmt\tCommission");
		queueMap.forEach((k, v) -> {
			BigDecimal qty = v.stream()
					.map(t -> t.getQuantity())
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			BigDecimal principalAmt = v.stream()
					.map(t -> t.getPrice().multiply(t.getQuantity()))
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			BigDecimal comm = v.stream()
					.map(t -> t.getCommission())
					.reduce(BigDecimal.ZERO, BigDecimal::add);
			System.out.println(k + "\t" + qty + "\t" + principalAmt + "\t" + comm);
		});
		BigDecimal commissionStockInHand = queueMap
		.values()
		.stream()
		.map(q -> q.stream()
				.map(t -> t.getCommission())
				.reduce(BigDecimal.ZERO, BigDecimal::add))
		.reduce(BigDecimal.ZERO, BigDecimal::add);
		System.out.println("Commission for stock-in-hand\t" + commissionStockInHand);

		BigDecimal countStockInHand = queueMap
				.values()
				.stream()
				.map(q -> q.stream()
						.map(t -> t.getQuantity())
						.reduce(BigDecimal.ZERO, BigDecimal::add))
				.reduce(BigDecimal.ZERO, BigDecimal::add);
				System.out.println("Count of stock-in-hand\t" + countStockInHand);


		System.out.println("---------");
		System.out.println("Per Symbol");
		System.out.println("Symbol\tCapitalGain");
		mapSymbolGain.forEach((k, v) -> System.out.println(k + "\t" + v));
		System.out.println("---------");
		if (mapMonthGain.isEmpty()) {
			System.out.println("No SHORT transactions");
		} else {
			System.out.println("Per Month");
			System.out.println("Month\tCapitalGain");
			mapMonthGain.forEach((k, v) -> System.out.println(k + "\t" + v));
			System.out.println("---------");
		}

		System.out.println("Per Date Range CapitalGain");
		System.out.println("DateFrom\tDateTo\tType\tCapitalGain");
		mapDateRangeGain.forEach((k, v) -> System.out.println(k + "\tSHORT\t" + v));
		System.out.println("---------");
		System.out.println("Per Date Range Commission");
		System.out.println("DateFrom\tDateTo\tType\tCommission");
		mapDateRangeCommission.forEach((k, v) -> System.out.println(k + "\tSHORT\t" + v));
		System.out.println("---------");
		System.out.println((shouldConvertCurrency ? ("Gain converted\t" + gain + "\t") : "") + "Gain USD\t" + gainUSD);
		System.out.println("---------");
		System.out.println((shouldConvertCurrency ? ("Commission converted\t" + commission + "\t") : "") + "Commission USD\t" + commissionUSD);
		System.out.println("---------");
		System.out.println("Turnover Amt IntraDay\t" + turnoverIntraday);
		System.out.println("Turnover Amt Delivery\t" + turnoverDelivery);
		System.out.println("---------");

		return gain;
	}

	private static Double roundedToTwoDecimals(Double amount) {
		return Math.round(amount * 100.0) / 100.0;
	}
}
