package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

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

    // Not part of the WritableComparable interface but good practice to override for Hadoop keys
    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FilenameAndOffset that = (FilenameAndOffset) o;
        if (offset != that.offset) { return false; }
        return Objects.equals(filename, that.filename);
    }

    // Not part of the WritableComparable interface but good practice to override for Hadoop keys
    // As stated in https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/WritableComparable.html
    //  the default Objects.hash() should be avoided for Hadoop keys as it may not return the same value
    //  across different instances of the JVM, therefore, a custom hashCode implementation is required.
    // Multiplying the result of a hash value by 31 is a long-standing convention in Java
    @Override
    public int hashCode()
    {
        int result = filename != null ? filename.hashCode() : 0;
        result = 31 * result + Long.hashCode(offset);
        return result;
    }
}
