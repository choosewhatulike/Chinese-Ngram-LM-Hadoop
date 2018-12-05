import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class XMLInputFormat
        extends FileInputFormat<LongWritable, Text> {
    final String[] needNodes = {"content", "contenttitle"};

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new XMLRecordReader(needNodes);
    }
}
