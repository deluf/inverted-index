package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Implements the CombineFileInputFormat (abstract class) to use the
 *  NameAwareRecordReader instead of the default CombineFileRecordReader,
 *  which does not keep track of the filename
 */
public class NameAwareCombineTextInputFormat
        extends CombineFileInputFormat<FilenameAndOffset, Text>
{
    @Override
    public RecordReader<FilenameAndOffset, Text> createRecordReader
            (InputSplit split, TaskAttemptContext context) throws IOException
    {
        return new CombineFileRecordReader<>
                ((CombineFileSplit)split, context, NameAwareRecordReader.class);
    }
}
