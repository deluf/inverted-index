package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Wraps the standard LineRecordReader to implement filename awareness
 *
 * References:
 *  [1] - https://stackoverflow.com/questions/10380200/how-to-use-combinefileinputformat-in-hadoop
 *  [2] - https://github.com/dryman/Hadoop-CombineFileInputFormat
 *  [3] - https://apachesparktraining.wordpress.com/2014/07/05/process-small-files-on-hadoop-using-combinefileinputformat/
 */
public class NameAwareRecordReader
        extends RecordReader<FilenameAndOffset, Text>
{
    private final LineRecordReader lineReader;
    private final FilenameAndOffset key;

    /*
     * split: An object representing multiple files or parts of files grouped together
     * context: The Hadoop task context
     * index: The index of the specific file within the combined split to process
     *
     * The constructor is called each time the index changes,
     *  i.e., when a new portion of the previous file, or a completely
     *  new file, is processed
     */
    public NameAwareRecordReader(CombineFileSplit split,
                                 TaskAttemptContext context,
                                 Integer index) throws IOException
    {
        lineReader = new LineRecordReader();
        key = new FilenameAndOffset();

        // Create a FileSplit with the current portion of CombineFileSplit
        FileSplit fileSplit = new FileSplit(
                split.getPath(index),
                split.getOffset(index),
                split.getLength(index),
                split.getLocations()
        );

        // Set the key's filename (will stay constant for the whole FileSplit)
        String filename = split.getPath(index).getName();
        key.setFilename(filename);

        // Feed the FileSplit to a standard lineReader
        lineReader.initialize(fileSplit, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
    {
        // Everything is already initialized in the constructor
    }

    // Advances to the next line of the input split
    @Override
    public boolean nextKeyValue() throws IOException
    {
        // Delegates everything to the underlying lineReader
        if (!lineReader.nextKeyValue()) { return false; }

        // The lineReader returned true, i.e., there are more lines to read.
        // We just need to update the file offset (bytes) inside the key
        key.setOffset(lineReader.getCurrentKey().get());
        return true;
    }

    @Override
    public FilenameAndOffset getCurrentKey() { return key; }

    @Override
    public Text getCurrentValue()
        { return lineReader.getCurrentValue(); }

    @Override
    public float getProgress() throws IOException
        { return lineReader.getProgress(); }

    @Override
    public void close() throws IOException { lineReader.close(); }
}
