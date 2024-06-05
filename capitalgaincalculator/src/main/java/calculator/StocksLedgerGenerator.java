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
import java.util.stream.Collectors;

import models.CurrencyConversionRate;
import models.StockTransaction;
import utils.TextFileReader;

public class StocksLedgerGenerator {

    //static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat dateFormatter_yyyyMM = new SimpleDateFormat("yyyy-MM");
    static SimpleDateFormat dateFormatter_yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");
    final static BigDecimal zero = new BigDecimal(0);
    final static BigDecimal minusOne = new BigDecimal(-1);

    public static void main(String[] args) throws ParseException {
        //Parsing the given String to Date object
        String[] dateStart = {"2020-04-01", "2020-06-16", "2020-09-16", "2020-12-16", "2021-03-16"};
        String[] dateEnd = {"2020-06-15", "2020-09-15", "2020-12-15", "2021-03-15", "2021-03-31"};

        dateStart = new String[]{"2024-04-01"};
        dateEnd   = new String[]{"2025-03-31"};

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
        ClassLoader classLoader = StocksLedgerGenerator.class.getClassLoader();
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

        boolean shouldConvertCurrency = null != rates;

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

        StocksLedgerGenerator calc = new StocksLedgerGenerator();
        calc.generateStocksLedger(transactions, dateRangeFrom, dateRangeTo, rates, shouldConvertCurrency, shouldSeparateCommission);
    }

    public void generateStocksLedger(List<StockTransaction> transactions,
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
        if (!longTransactions.isEmpty()) {
            generateStocksLedgerForLong(longTransactions,
                    dateRangeFrom,
                    dateRangeTo,
                    rates,
                    shouldConvertCurrency,
                    shouldSeparateCommission);
        }

        List<StockTransaction> shortTransactions = transactions.stream()
                                                   .filter(entry -> entry.getLongShort().equalsIgnoreCase("SHORT"))
                                                   .collect(Collectors.toList());
        if (!shortTransactions.isEmpty()) {
            System.out.println();
            generateStocksLedgerForShort(shortTransactions,
                    dateRangeFrom,
                    dateRangeTo,
                    rates,
                    shouldConvertCurrency,
                    shouldSeparateCommission);
        }
    }

    public void generateStocksLedgerForLong(List<StockTransaction> transactions,
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

        Map<String, BigDecimal> positions = new HashMap<>();
        int lastMonth = -1;

        System.out.println("--------- LONG ---------");
        String[] arr = {"Type",
                        "Symbol",
                        "BuyDate",
                        "BuyCost",
                        "Qty",
                        "RemainingQty",
                        "BuyOrderId",
                        "BuyVoucherNo",
                        "SellDate",
                        "SellOrderID",
                        "SellVoucherNo",
                        "Long/Short"};
        String headerRow = String.join("\t", arr);
        System.out.println(headerRow);

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

                System.out.println("Buy"
                                   + "\t" + transaction.getSymbol()
                                   + "\t" + dateFormatter_yyyyMMdd.format(transaction.getActivityDate())
                                   + "\t" + transaction.getPrice().multiply(transaction.getQuantity()).subtract(transaction.getCommission())
                                   + "\t" + transaction.getQuantity()
                                   + "\t" + cumulativeQty
                                   + "\t" + transaction.getOrderId()
                                   + "\t" + transaction.getVoucherNo()
                                   + "\t" + ""
                                   + "\t" + ""
                                   + "\t" + ""
                                   + "\t" + transaction.getLongShort());
            } else {
                StockTransaction sellEntry = transaction;
                BigDecimal remainingSellQty = sellEntry.getQuantity();

                while (remainingSellQty.compareTo(zero) > 0 && !queueBuys.isEmpty()) {
                    StockTransaction buyEntry = queueBuys.peek();
                    BigDecimal buyQty = buyEntry.getQuantity();
                    BigDecimal consumedQty = zero;
                    BigDecimal consumedCommission = zero;

                    if (remainingSellQty.compareTo(buyQty) >= 0) {
                        queueBuys.remove();
                        consumedQty = buyQty;
                        remainingSellQty = remainingSellQty.subtract(consumedQty);
                        consumedCommission = buyEntry.getCommission();
                    } else {
                        consumedQty = remainingSellQty;
                        BigDecimal partialBuyCommission = buyEntry.getCommission().multiply(consumedQty.divide(buyEntry.getQuantity(), 4, RoundingMode.HALF_UP));
                        consumedCommission = consumedCommission.add(partialBuyCommission);
                        buyEntry.setQuantity(buyQty.subtract(remainingSellQty));
                        buyEntry.setCommission(buyEntry.getCommission().subtract(partialBuyCommission));
                        remainingSellQty = zero;
                    }

                    String symbol = sellEntry.getSymbol();
                    BigDecimal buyPrice = buyEntry.getPrice();

                    if (!sellEntry.getActivityDate().before (reportStartDate) && !sellEntry.getActivityDate().after (reportEndDate)) {
                        BigDecimal countSold = consumedQty.compareTo(sellEntry.getQuantity()) > 0 ? sellEntry.getQuantity() : consumedQty;
                        BigDecimal buyCost = buyPrice.multiply(countSold).subtract(consumedCommission);
                        cumulativeQty = cumulativeQty.subtract(countSold);

                        System.out.println("Sell"
                                            + "\t" + symbol
                                            + "\t" + dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate())
                                            + "\t" + buyCost
                                            + "\t" + countSold.multiply(minusOne)
                                            + "\t" + cumulativeQty
                                            + "\t" + buyEntry.getOrderId()
                                            + "\t" + buyEntry.getVoucherNo()
                                            + "\t" + dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate())
                                            + "\t" + sellEntry.getOrderId()
                                            + "\t" + sellEntry.getVoucherNo()
                                            + "\t" + sellEntry.getLongShort());
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
                lastMonth = curMonth;
            }

            if (0 == cumulativeQty.compareTo(zero)) {
                positions.remove(transaction.getSymbol());
            } else {
                positions.put(transaction.getSymbol(), cumulativeQty);
            }
        }
    }

    public void generateStocksLedgerForShort(List<StockTransaction> transactions,
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

        Map<String, BigDecimal> positions = new HashMap<>();
        int lastMonth = -1;

        System.out.println("--------- SHORT ---------");
        String[] arr = {"Type",
                        "Symbol",
                        "BuyDate",
                        "SellCost",
                        "Qty",
                        "RemainingQty",
                        "BuyOrderId",
                        "BuyVoucherNo",
                        "SellDate",
                        "SellOrderID",
                        "SellVoucherNo",
                        "Long/Short"};
        String headerRow = String.join("\t", arr);
        System.out.println(headerRow);

        for (StockTransaction transaction : sortedTransactions) {
            Queue<StockTransaction> queue = queueMap.get(transaction.getSymbol());
            if (queue == null) {
                queue = new LinkedList<StockTransaction>();
                queueMap.put(transaction.getSymbol(), queue);
            }

            BigDecimal cumulativeQty = Optional.ofNullable(positions.get(transaction.getSymbol())).orElse(zero);

            if (transaction.getTransactionType().equalsIgnoreCase("SELL")) {
                queue.add(transaction);
                cumulativeQty = cumulativeQty.subtract(transaction.getQuantity());

                System.out.println("Sell"
                                   + "\t" + transaction.getSymbol()
                                   + "\t" + ""
                                   + "\t" + transaction.getPrice().multiply(transaction.getQuantity()).subtract(transaction.getCommission())
                                   + "\t" + transaction.getQuantity().multiply(minusOne)
                                   + "\t" + cumulativeQty
                                   + "\t" + ""
                                   + "\t" + ""
                                   + "\t" + dateFormatter_yyyyMMdd.format(transaction.getActivityDate())
                                   + "\t" + transaction.getOrderId()
                                   + "\t" + transaction.getVoucherNo()
                                   + "\t" + transaction.getLongShort());
            } else {
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
                        consumedBuyCommission = buyEntry.getCommission().multiply(consumedQty.divide(buyEntry.getQuantity(), 4, RoundingMode.HALF_UP));
                        BigDecimal partialSellCommission = sellEntry.getCommission().multiply(consumedQty).divide(sellEntry.getQuantity(), 4, RoundingMode.HALF_UP);
                        consumedCommission = consumedBuyCommission.add(partialSellCommission);
                        sellEntry.setQuantity(sellQty.subtract(buyQty));
                        sellEntry.setCommission(sellEntry.getCommission().subtract(partialSellCommission));
                        remainingBuyQty = zero;
                    }
                    String symbol = buyEntry.getSymbol();
                    BigDecimal sellPrice = sellEntry.getPrice();

                    if (!buyEntry.getActivityDate().before (reportStartDate) && !buyEntry.getActivityDate().after (reportEndDate)) {
                        BigDecimal countBought = consumedQty.compareTo(buyEntry.getQuantity()) >= 0 ? buyEntry.getQuantity() : consumedQty;
                        BigDecimal sellCost = sellPrice.multiply(countBought).subtract(consumedCommission);
                        cumulativeQty = cumulativeQty.add(countBought);

                        System.out.println("Buy"
                                           + "\t" + symbol
                                           + "\t" + dateFormatter_yyyyMMdd.format(buyEntry.getActivityDate())
                                           + "\t" + sellCost
                                           + "\t" + countBought
                                           + "\t" + cumulativeQty
                                           + "\t" + buyEntry.getOrderId()
                                           + "\t" + buyEntry.getVoucherNo()
                                           + "\t" + dateFormatter_yyyyMMdd.format(sellEntry.getActivityDate())
                                           + "\t" + sellEntry.getOrderId()
                                           + "\t" + sellEntry.getVoucherNo()
                                           + "\t" + sellEntry.getLongShort());
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
                lastMonth = curMonth;
            }

            if (0 == cumulativeQty.compareTo(zero)) {
                positions.remove(transaction.getSymbol());
            } else {
                positions.put(transaction.getSymbol(), cumulativeQty);
            }
        }
    }
}
