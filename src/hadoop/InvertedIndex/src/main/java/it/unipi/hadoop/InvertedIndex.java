package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex
{
    public static class SimpleMapper extends Mapper<LongWritable, Text, Text, InvertedIndexItem>
    {
        private final Text word = new Text();
        private final InvertedIndexItem oneItem = new InvertedIndexItem(1);
        private String filename;

        @Override
        protected void setup(Context context)
        {
            InputSplit split = context.getInputSplit();
            if (!(split instanceof FileSplit))
            {
                throw new RuntimeException("The input split received by the mapper is not a FileSplit");
            }
            filename = ((FileSplit) split).getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                oneItem.setFilename(filename);
                context.write(word, oneItem);
            }
        }
    }

    /* Better mapper: ignores punctuation marks and lowercases words */
    public static class AccurateMapper extends Mapper<LongWritable, Text, Text, InvertedIndexItem>
    {
        private final Text word = new Text();
        private final InvertedIndexItem oneItem = new InvertedIndexItem(1);
        private String filename;

        @Override
        protected void setup(Context context)
        {
            InputSplit split = context.getInputSplit();
            if (!(split instanceof FileSplit))
            {
                throw new RuntimeException("The input split received by the mapper is not a FileSplit");
            }
            filename = ((FileSplit) split).getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken().toLowerCase().replaceAll("([^A-Za-z'])+", "");
                if (token.isEmpty())
                {
                    continue;
                }
                word.set(token);
                oneItem.setFilename(filename);
                context.write(word, oneItem);
            }
        }
    }

    /* Implements an in-mapper combiner on top of the AccurateMapper */
    public static class CombinerMapper extends Mapper<LongWritable, Text, Text, InvertedIndexItem>
    {
        private final Text word = new Text();
        private String filename;
        private final Map<String, Integer> wordCounts = new HashMap<>();
        private final InvertedIndexItem result = new InvertedIndexItem();

        @Override
        protected void setup(Context context)
        {
            InputSplit split = context.getInputSplit();
            if (!(split instanceof FileSplit))
            {
                throw new RuntimeException("The input split received by the mapper is not a FileSplit");
            }
            filename = ((FileSplit) split).getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
        {
            // If memory becomes a problem, flush the wordCounts map every N tokens

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken().toLowerCase().replaceAll("([^A-Za-z'])+", "");
                if (token.isEmpty())
                {
                    continue;
                }
                wordCounts.put(token, wordCounts.getOrDefault(token, 0) + 1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            result.setFilename(filename);
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet())
            {
                word.set(entry.getKey());
                result.setCount(entry.getValue());
                context.write(word, result);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, InvertedIndexItem, Text, InvertedIndexItem>
    {
        private final InvertedIndexItem result = new InvertedIndexItem();

        @Override
        public void reduce(Text key, Iterable<InvertedIndexItem> values, Context context)
                throws IOException, InterruptedException
        {
            InvertedIndexItem first = values.iterator().next();
            int sum = first.getCount();
            String filename = first.getFilename();

            for (InvertedIndexItem item : values)
            {
                sum += item.getCount();
            }

            result.setFilename(filename);
            result.setCount(sum);
            context.write(key, result);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, InvertedIndexItem, Text, Text>
    {
        private final Text result = new Text();

        // Builds the output string in the format "filename1:count1\tfilename2:count2\t..."
        private static StringBuilder getStringBuilder(Map<String, Integer> countsPerFile)
        {
            StringBuilder outputBuilder = new StringBuilder();
            boolean firstEntry = true;
            for (Map.Entry<String, Integer> entry : countsPerFile.entrySet())
            {
                if (!firstEntry)
                {
                    outputBuilder.append("\t");
                }
                outputBuilder.append(entry.getKey());   // Filename
                outputBuilder.append(":");
                outputBuilder.append(entry.getValue()); // Count
                firstEntry = false;
            }
            return outputBuilder;
        }

        @Override
        public void reduce(Text key, Iterable<InvertedIndexItem> values, Context context)
                throws IOException, InterruptedException
        {
            Map<String, Integer> countsPerFile = new HashMap<>();
            for (InvertedIndexItem item : values)
            {
                String filename = item.getFilename();
                int increment = item.getCount();
                int previousCount = countsPerFile.getOrDefault(filename, 0);
                countsPerFile.put(filename, previousCount + increment);
            }

            StringBuilder outputBuilder = getStringBuilder(countsPerFile);
            result.set(outputBuilder.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        // Initialize the Hadoop application and the MapReduce Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");

        // Parse CLI arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: InvertedIndex <input folder> <output folder>");
            System.exit(1);
        }

        // FileInputFormat assumes by default a textual input:
        //  - keys of type LongWritable (the byte offset)
        //  - values of type Text (a single line of text)
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Assign the classes
        job.setJarByClass(InvertedIndex.class);
            // You can choose between: SimpleMapper, AccurateMapper or CombinerMapper
        job.setMapperClass(CombinerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
            // Optional: Use an external combiner (should be disabled if CombinerMapper is used)
        //job.setCombinerClass(InvertedIndexCombiner.class);

        // Define the (Key, Value) output types of the mappers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InvertedIndexItem.class);

        // Define the (Key, Value) output types of the reducers
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

            // Optional: use more than one reducer
        //job.setNumReduceTasks(N);

        // Start the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
    The inverted index is the foundational data structure used in information retrieval systems like Google Search.
    Given a large collection of text files (e.g., articles, books, web pages),
     the inverted index maps each detected word to the files in which it appears.
    This kind of data structure is fundamental to enable quick search of files containing a specific term.

    For each detected word, the inverted index should report:
        i) any filename where that word is found;
        ii) the number of times the word appears in that file.

    Example:

    Mapper input - Key::LongWritable (byte offset), Value::Text (line of text)
        file1.txt:0, cloud is cloud
        file2.txt:0, cloud

    Mapper output - Key::Text (word), Value::<Text, LongWritable> (filename, count)
        cloud, (file1.txt, 1)
        is, (file1.txt, 1)
        cloud, (file1.txt, 1)
        cloud, (file2.txt, 1)

        OPTIONAL
    Combiner input - The same as the mapper's output
    Combiner output - The same as the reducer's input
        cloud, (file1.txt, 2)
        is, (file1.txt, 1)
        cloud, (file2.txt, 1)

    Reducer input: Key::Text (word), Value::<Text, LongWritable>[] (filename, count)[]
        cloud, [(file1.txt, 2), (file2.txt, 1)]
        is, [(file1.txt, 1)]

    Reducer output: Key::Text (word), Value::Text (filename:count...)
        cloud   file.txt:2  file2.txt:1
        is  file1.txt:1
*/
