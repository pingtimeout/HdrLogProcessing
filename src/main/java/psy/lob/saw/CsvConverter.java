package psy.lob.saw;

import org.HdrHistogram.Histogram;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Locale;

public class CsvConverter
{
    private final InputStream inputStream;
    private final PrintStream outputStream;

    public CsvConverter(InputStream inputStream, PrintStream outputStream)
    {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    public void convert() throws FileNotFoundException
    {
        OrderedHistogramLogReader reader = new OrderedHistogramLogReader(inputStream);
        outputStream.println(
                "#Absolute timestamp,Relative timestamp,Throughput,Min,Avg,p50,p90,p95,p99,p999,p9999,Max");
        while (reader.hasNext())
        {
            Histogram interval = (Histogram) reader.nextIntervalHistogram();
            outputStream.printf(Locale.US,
                    "%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    interval.getStartTimeStamp() / 1000.0,
                    interval.getStartTimeStamp() / 1000 - (long) reader.getStartTimeSec(),
                    interval.getTotalCount(), interval.getMinValue(),
                    (long) interval.getMean(),
                    interval.getValueAtPercentile(50),
                    interval.getValueAtPercentile(90),
                    interval.getValueAtPercentile(95),
                    interval.getValueAtPercentile(99),
                    interval.getValueAtPercentile(99.9),
                    interval.getValueAtPercentile(99.99),
                    interval.getMaxValue());
        }
    }
}
