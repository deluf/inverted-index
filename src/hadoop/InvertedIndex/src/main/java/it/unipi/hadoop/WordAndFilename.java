package it.unipi.hadoop;

import java.util.Objects;

/**
 * Used by the in-mapper combiner as a key for an HashMap.
 * In order for the HashMap to work correctly, it must
 *  override the equals and hashCode() methods
 */
public class WordAndFilename
{
    private final String word;
    private final String filename;

    public WordAndFilename(String word, String filename)
    {
        this.word = word;
        this.filename = filename;
    }

    public String getWord() { return word; }
    public String getFilename() { return filename; }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        WordAndFilename that = (WordAndFilename) o;
        return word.equals(that.word) && filename.equals(that.filename);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(word, filename);
    }
}
