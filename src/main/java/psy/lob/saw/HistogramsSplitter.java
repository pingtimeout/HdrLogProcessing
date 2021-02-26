package psy.lob.saw;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static psy.lob.saw.HdrHistogramUtil.logHistogramForVerbose;

public class HistogramsSplitter
{
    private final String inputFileName;
    private final InputStream inputStream;
    private final double start;
    private final double end;
    private final boolean verbose;
    private final Predicate<String> tagExclusionPredicate;

    public HistogramsSplitter(String inputFileName, InputStream inputStream,
                              double start, double end,
                              boolean verbose, Predicate<String> tagExclusionPredicate)
    {
        this.inputFileName = inputFileName;
        this.inputStream = inputStream;
        this.start = start;
        this.end = end;
        this.verbose = verbose;
        this.tagExclusionPredicate = tagExclusionPredicate;
    }

    public void split() throws FileNotFoundException
    {
        OrderedHistogramLogReader reader = new OrderedHistogramLogReader(
                inputStream,
                start,
                end,
                tagExclusionPredicate);
        Map<String, HistogramLogWriter> writerByTag = new HashMap<>();
        Histogram interval;
        int i = 0;
        while (reader.hasNext())
        {
            interval = (Histogram) reader.nextIntervalHistogram();
            if (interval == null)
            {
                continue;
            }
            String ntag = interval.getTag();
            if (tagExclusionPredicate.test(ntag))
            {
                throw new IllegalStateException("Should be filtered upfront by the reader");
            }
            if (this.verbose)
            {
                logHistogramForVerbose(System.out, interval, i++);
            }
            interval.setTag(null);
            HistogramLogWriter writer = writerByTag.computeIfAbsent(ntag, k -> createWriterForTag(reader, k, inputFileName));
            writer.outputIntervalHistogram(interval);
        }
    }

    private HistogramLogWriter createWriterForTag(OrderedHistogramLogReader reader, String tag, String inputFileName)
    {
        tag = (tag == null) ? "default" : tag;
        File outputFile = new File(tag + "." + inputFileName);
        String comment = "Splitting of:" + inputFileName + " start:" + start + " end:" + end;
        return HdrHistogramUtil.createLogWriter(outputFile, comment, reader.getStartTimeSec());
    }
}