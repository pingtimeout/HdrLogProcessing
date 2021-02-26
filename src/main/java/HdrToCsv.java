import org.kohsuke.args4j.Option;
import psy.lob.saw.CsvConverter;

import java.io.*;
import java.nio.file.Paths;

public class HdrToCsv implements Runnable
{
    private File inputFile;

    public static void main(String[] args)
    {
        ParseAndRunUtil.parseParamsAndRun(args, new HdrToCsv());
    }

    @Option(name = "--input-file",
        aliases = "-i",
        usage = "Relative or absolute path to the input file to read",
        required = true)
    public void setInputFile(String fileName)
    {
        File in = Paths.get(fileName).toFile();
        if (!in.exists())
        {
            throw new IllegalArgumentException(
                "Input file " + fileName + " does not exist");
        }
        inputFile = in;
    }

    @Override
    public void run()
    {
        try(InputStream inputStream = new FileInputStream(inputFile))
        {
            new CsvConverter(inputStream).convert();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}