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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex
{

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, InvertedIndexItem>
    {
        // Defining variables at class level should be more efficient
        private final Text word = new Text();
        private final InvertedIndexItem oneItem = new InvertedIndexItem("", 1);
        private String currentFilename;

        @Override
        protected void setup(Context context)
        {
            InputSplit split = context.getInputSplit();
            if (split instanceof FileSplit)
            {
                currentFilename = ((FileSplit) split).getPath().getName();
            }
            else
            {
                currentFilename = "unknown_file";
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                oneItem.setFilename(currentFilename);
                context.write(word, oneItem);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, InvertedIndexItem, Text, Text>
    {
        private final Text resultText = new Text();

        // Builds the output string in the format "filename1:count1 filename2:count2 ..."
        private static StringBuilder getStringBuilder(Map<String, Integer> countsPerFile)
        {
            StringBuilder outputBuilder = new StringBuilder();
            boolean firstEntry = true;
            for (Map.Entry<String, Integer> entry : countsPerFile.entrySet())
            {
                if (!firstEntry)
                {
                    outputBuilder.append(" ");
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
            Map<String, Integer> countsPerFile = new HashMap<>(); //FIXME: move it in the class fields?
            for (InvertedIndexItem item : values)
            {
                String filename = item.getFilename();
                int previousCount = countsPerFile.getOrDefault(filename, 0);
                int increment = item.getCount();
                countsPerFile.put(filename, previousCount + increment);
            }

            StringBuilder outputBuilder = getStringBuilder(countsPerFile);
            resultText.set(outputBuilder.toString());
            context.write(key, resultText);
        }
    }

    public static void main(String[] args) throws Exception
    {
        // Initialize the Hadoop application and the MapReduce Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");

        // Parse CLI arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: InvertedIndex <input> <output>");
            System.exit(1);
        }

        // FileInputFormat assumes by default a textual input:
        //  - keys of type LongWritable (the byte offset)
        //  - values of type Text (a single line of text)
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Assign the classes
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        //job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // Define the (Key, Value) output types of the mappers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InvertedIndexItem.class);

        // Define the (Key, Value) output types of the reducers
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the parameters
        //job.getConfiguration().set("KEY", "VAL");
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
The produced inverted index may for example be in the following form,
 where filename and number of occurrences are separated by a “:”:
cloud doc1.txt:1 doc2.txt:1
computing doc1.txt:1 doc2.txt:1
is doc1.txt:1
important doc2.txt:1

# EXAMPLE

Mapper input - Key::LongWritable (byte offset), Value::Text (line of text)
file.txt:0, cloud computing is important cloud
...

Mapper output - Key::Text (word), Value::<Text, LongWritable> (filename, count)
cloud, (file.txt, 2)
computing, (file.txt, 1)
is, (file.txt, 1)
important, (file.txt, 1)
...
cloud, (file2.txt, 1)

Reducer input: Key::Text (word), Value::<Text, LongWritable>[] (filename, count)[]
cloud, [(file.txt, 2), (file2.txt, 1)]

Reducer output: Key::Text (word), Value::Text (filename:count ...)
cloud, file.txt:2, file2.txt:1

*/
