package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An inverted index is a foundational data structure used in information
 *  retrieval systems like Google Search. Given a large collection of text
 *  files (e.g., articles, books, web pages), the inverted index maps each
 *  detected word to the files in which it appears, along with the number
 *  of times it appears in each file
 */
public class InvertedIndex
{
    private static final String matchPunctuation = "(^'+)|('+[^A-Za-z']+)";

    /**
     * For each input line of text, it splits the line into words,
     *  cleans the words by removing punctuation marks and lowercasing them,
     *  and then outputs couples Key:word, Value:(filename, 1)
     *
     *  Example:
     *      Input - Key::FilenameAndOffset, Value::Text (a line)
     *         file1.txt:0  CLOUD!, cloud computing.
     *         file2.txt:0  Cloud -Computing-
     *     Output - Key::Text (a word), Value::FilenameAndCount
     *         cloud        (file1.txt, 1)
     *         cloud        (file1.txt, 1)
     *         computing    (file1.txt, 1)
     *         cloud        (file2.txt, 1)
     *         computing    (file2.txt, 1)
     */
    public static class SimpleMapper
            extends Mapper<FilenameAndOffset, Text, Text, FilenameAndCount>
    {
        // Reusing the same objects for all the map() calls should be more efficient
        private final Text word = new Text();
        private final FilenameAndCount one = new FilenameAndCount(1);

        public void map(FilenameAndOffset key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String filename = key.getFilename();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken().toLowerCase()
                        .replaceAll(matchPunctuation, "");
                if (token.isEmpty()) { continue; }
                word.set(token);
                one.setFilename(filename);
                context.write(word, one);
            }
        }
    }

    /**
     * Behaves exactly like a SimpleMapper but also implements an
     *  in-mapper combiner.
     *
     *  Example:
     *      Input - Key::FilenameAndOffset, Value::Text (a line)
     *         file1.txt:0  CLOUD!, cloud computing.
     *         file2.txt:0  Cloud -Computing-
     *     Output - Key::Text (a word), Value::FilenameAndCount
     *         cloud        (file1.txt, 2)
     *         computing    (file1.txt, 1)
     *         cloud        (file2.txt, 1)
     *         computing    (file2.txt, 1)
     */
    public static class CombinerMapper
            extends Mapper<FilenameAndOffset, Text, Text, FilenameAndCount>
    {
        private final Map<WordAndFilename, Integer> counts = new HashMap<>();

        // Memory management
        private final Runtime runtime = Runtime.getRuntime();
        private int observedLines = 0;
        private static final int CHECK_FREQUENCY = 10000;
        private static final float MEMORY_THRESHOLD = 0.8f;

        public void map(FilenameAndOffset key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String filename = key.getFilename();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken().toLowerCase()
                        .replaceAll(matchPunctuation, "");
                if (token.isEmpty()) { continue; }
                WordAndFilename wordAndFilename = new WordAndFilename(token, filename);
                int previousCount = counts.getOrDefault(wordAndFilename, 0);
                counts.put(wordAndFilename, previousCount + 1);
            }

            if (observedLines % CHECK_FREQUENCY == 0)
            {
                context.getCounter("Memory Management",
                        "Number of in-mapper combiner memory checks").increment(1);
                float memoryUsageRatio =
                        (float)(runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory();
                if (memoryUsageRatio > MEMORY_THRESHOLD)
                {
                    flushCounts(context);
                    context.getCounter("Memory Management",
                            "Number of in-mapper combiner flushes").increment(1);
                }
            }
            observedLines++;
        }

        private void flushCounts(Context context) throws IOException, InterruptedException
        {
            Text word = new Text();
            FilenameAndCount result = new FilenameAndCount();
            for (Map.Entry<WordAndFilename, Integer> entry : counts.entrySet())
            {
                word.set(entry.getKey().getWord());
                result.setFilename(entry.getKey().getFilename());
                result.setCount(entry.getValue());
                context.write(word, result);
            }
            counts.clear();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            flushCounts(context);
        }
    }

    /**
     * A discrete combiner logic to be used with SimpleMapper.
     * Can be useful in scenarios in which performing in-mapper
     *  combining is too complex or memory demanding.
     *
     * Example:
     *     Input - The same as the SimpleMapper's output
     *         cloud        (file1.txt, 1)
     *         cloud        (file1.txt, 1)
     *         computing    (file1.txt, 1)
     *         cloud        (file2.txt, 1)
     *         computing    (file2.txt, 1)
     *     Output - The same as the CombinerMapper's output
     *         cloud        (file1.txt, 2)
     *         computing    (file1.txt, 1)
     *         cloud        (file2.txt, 1)
     *         computing    (file2.txt, 1)
     */
    public static class ExternalCombiner
            extends Reducer<Text, FilenameAndCount, Text, FilenameAndCount>
    {
        private final FilenameAndCount result = new FilenameAndCount();

        /*
         * Same code as the reducer, except it outputs the combined counts
         *  one by one instead of building and output string
         */
        @Override
        public void reduce(Text key, Iterable<FilenameAndCount> values, Context context)
                throws IOException, InterruptedException
        {
            Map<String, Integer> counts = new HashMap<>();

            for (FilenameAndCount value : values)
            {
                String filename = value.getFilename();
                int increment = value.getCount();
                int previousCount = counts.getOrDefault(filename, 0);
                counts.put(filename, previousCount + increment);
            }

            for (Map.Entry<String, Integer> entry : counts.entrySet())
            {
                result.setFilename(entry.getKey());
                result.setCount(entry.getValue());
                context.write(key, result);
            }
        }
    }

    /**
     * For each word, combines the counts received from the mappers
     *  into a clean, well-formatted output string.
     *
     *  Example:
     *      Input - Key::Text (a word), Value::FilenameAndCount[]
     *          cloud       [(file1.txt, 2), (file2.txt, 1)]
     *          computing   [(file1.txt, 1), (file2.txt, 1)]
     *      Output - Key::Text (a word), Value::Text (a formatted output string)
     *          cloud       file1.txt:2 file2.txt:1
     *          computing   file1.txt:2 file2.txt:1
     */
    public static class MainReducer
            extends Reducer<Text, FilenameAndCount, Text, Text>
    {
        private final Text result = new Text();

        private static String buildOutputLine(Map<String, Integer> counts)
        {
            StringJoiner joiner = new StringJoiner("\t");
            for (Map.Entry<String, Integer> entry : counts.entrySet())
            {
                joiner.add(entry.getKey() + ":" + entry.getValue());
            }
            return joiner.toString();
        }

        @Override
        public void reduce(Text key, Iterable<FilenameAndCount> values, Context context)
                throws IOException, InterruptedException
        {
            // Every word (reduce call) has its own map of (filename, count)
            Map<String, Integer> counts = new HashMap<>();

            for (FilenameAndCount value : values)
            {
                String filename = value.getFilename();
                int increment = value.getCount();
                int previousCount = counts.getOrDefault(filename, 0);
                counts.put(filename, previousCount + increment);
            }

            String outputLine = buildOutputLine(counts);
            result.set(outputLine);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");

        // Parse CLI arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5)
        {
            System.err.println(
                    "Usage: InvertedIndex " +
                            "<input folder> " +
                            "<output folder> " +
                            "<configuration> " +
                            "<number of reducers> " +
                            "<max input split size (MB)> " +
                    " Configuration can be: \n" +
                    "  0) SimpleMapper\n" +
                    "  1) SimpleMapper + ExternalCombiner\n" +
                    "  2) CombinerMapper\n" +
                    "  3) CombinerMapper + ExternalCombiner\n");
            System.exit(1);
        }

        /*
         * CombineTextInputFormat is critical when dealing with a lot of small files.
         * Unfortunately, the default CombineTextInputFormat does not provide the mappers
         *  an easy and straightforward way of getting the name of the file being processed
         *  (with TextInputFormat it was easier because each mapper processed only one file).
         * Therefore, we had to re-define the class to keep track of the filenames.
         */
        job.setInputFormatClass(NameAwareCombineTextInputFormat.class);
        int maxInputSplitSize = Integer.parseInt(otherArgs[4]) * 1024 * 1024;
        NameAwareCombineTextInputFormat.setMaxInputSplitSize(job, maxInputSplitSize);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setJarByClass(InvertedIndex.class);
        job.setReducerClass(MainReducer.class);

        int configuration = Integer.parseInt(otherArgs[2]);
        switch (configuration)
        {
            case 0:
                job.setMapperClass(SimpleMapper.class);
                break;
            case 1:
                job.setMapperClass(SimpleMapper.class);
                job.setCombinerClass(ExternalCombiner.class);
                break;
            case 2:
                job.setMapperClass(CombinerMapper.class);
                break;
            case 3:
                job.setMapperClass(CombinerMapper.class);
                job.setCombinerClass(ExternalCombiner.class);
        }

        // Define the (Key, Value) output types of the mappers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FilenameAndCount.class);

        // Define the (Key, Value) output types of the reducers
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

            // Optional: use more than one reducer
        int numReducers = Integer.parseInt(otherArgs[3]);
        job.setNumReduceTasks(numReducers);

        // Start the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
