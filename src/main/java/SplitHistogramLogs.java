import org.kohsuke.args4j.Option;
import psy.lob.saw.HistogramsSplitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static psy.lob.saw.HdrHistogramUtil.logHistogramForVerbose;

public class SplitHistogramLogs implements Runnable
{
    @Option(name = "-start", aliases = "-s", usage = "relative log start time in seconds, (default: 0.0)", required = false)
    public double start = 0.0;

    @Option(name = "-end", aliases = "-e", usage = "relative log end time in seconds, (default: MAX_DOUBLE)", required = false)
    public double end = Double.MAX_VALUE;

    @Option(name = "-verbose", aliases = "-v", usage = "verbose logging, (default: false)", required = false)
    public boolean verbose = false;
    private File inputPath = new File(".");
    private File inputFile;
    private Set<String> excludeTags = new HashSet<>();
    private Set<String> includeTags = new HashSet<>();
    private String fileName;

    public static void main(String[] args) throws Exception
    {
        ParseAndRunUtil.parseParamsAndRun(args, new SplitHistogramLogs());
    }

    @Option(name = "-inputPath", aliases = "-ip", usage = "set path to use for input files, defaults to current folder", required = false)
    public void setInputPath(String inputFolderName)
    {
        inputPath = new File(inputFolderName);
        if (!inputPath.exists())
        {
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must exist!");
        }
        if (!inputPath.isDirectory())
        {
            throw new IllegalArgumentException("inputPath:" + inputFolderName + " must be a directory!");
        }
    }

    @Option(name = "-inputFile", aliases = "-if", usage = "set the input hdr log from input path", required = true)
    public void setInputFile(String inputFileName)
    {
        inputFile = new File(inputPath, inputFileName);
        if (!inputFile.exists())
        {

            inputFile = new File(inputFileName);
            if (!inputFile.exists())
            {
                throw new IllegalArgumentException("inputFile:" + inputFileName + " must exist!");
            }
        }

    }

    @Option(name = "-excludeTag", aliases = "-et", usage = "add a tag to filter from input, 'default' is a special tag for the null tag.", required = false)
    public void addExcludeTag(String tag)
    {
        excludeTags.add(tag);
    }

    @Option(name = "-includeTag", aliases = "-it", usage = "when include tags are used only the explicitly included will be split out, 'default' is a special tag for the null tag.", required = false)
    public void addIncludeTag(String tag)
    {
        includeTags.add(tag);
    }

    @Override
    public void run()
    {
        if (verbose)
        {
            String absolutePath = inputPath.getAbsolutePath();
            String name = inputFile.getName();
            if (end != Double.MAX_VALUE)
            {
                System.out.printf("start:%.2f end:%.2f path:%s file:%s \n", start, end, absolutePath, name);
            }
            else
            {
                System.out.printf("start:%.2f end: MAX path:%s file:%s \n", start, absolutePath, name);
            }
        }
        try(InputStream inputStream = new FileInputStream(inputFile))
        {
            Path outputDir = inputFile.toPath().getParent();
            fileName = inputFile.getName();
            new HistogramsSplitter(fileName, inputStream, start, end, verbose, this::shouldSkipTag, outputDir).split();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private boolean shouldSkipTag(String ntag)
    {
        ntag = (ntag == null) ? "default" : ntag;
        return excludeTags.contains(ntag) || (!includeTags.isEmpty() && !includeTags.contains(ntag));
    }
}