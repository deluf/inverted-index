package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Used to replace the default LongWritable key of (Combine)TextInputFormat.
 * Since it's used as a Hadoop key, it must be both serializable and orderable
 */
public class FilenameAndOffset implements WritableComparable<FilenameAndOffset>
{
    private String filename;
    private long offset;

    public FilenameAndOffset() { }

    public String getFilename() { return filename; }
    public void setFilename(String filename) { this.filename = filename; }
    public void setOffset(long offset) { this.offset = offset; }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(filename);
        dataOutput.writeLong(offset);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        filename = in.readUTF();
        offset = in.readLong();
    }

    @Override
    public int compareTo(FilenameAndOffset o)
    {
        int cmp = filename.compareTo(o.filename);
        if (cmp != 0) { return cmp; }
        return Long.compare(offset, o.offset);
    }
}
