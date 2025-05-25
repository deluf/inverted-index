package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InvertedIndexItem implements Writable, Comparable<InvertedIndexItem>
{
    private String filename;
    private int count;

    public InvertedIndexItem(String filename, int count)
    {
        this.filename = filename;
        this.count = count;
    }

    public String getFilename()
    {
        return filename;
    }

    public int getCount()
    {
        return count;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(filename);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        filename = dataInput.readUTF();
        count = dataInput.readInt();
    }

    @Override
    public int compareTo(@NonNull InvertedIndexItem that)
    {
        if (this == that)
        {
            return 0;
        }

        // Primary sort by filename
        int filenameCompare = this.filename.compareTo(that.filename);
        if (filenameCompare != 0)
        {
            return filenameCompare;
        }

        // Secondary sort by count
        return Long.compare(this.count, that.count);
    }

    @Override
    public String toString()
    {
        return filename + ":" + count;
    }

}