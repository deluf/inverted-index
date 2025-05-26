package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

 /**
  * Represents the output value of a mapper.
  * Must be serializable (i.e., implement Hadoop's Writable interface)
  */
public class FilenameAndCount implements Writable
{
    private String filename;
    private int count;

    public FilenameAndCount() { }
    public FilenameAndCount(int count) { this.count = count; }

    public String getFilename() { return filename; }
    public int getCount() { return count; }
    public void setFilename(String filename) { this.filename = filename; }
    public void setCount(int count) { this.count = count; }

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
}
