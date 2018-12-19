import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;



public class XMLInputFormat
        extends FileInputFormat<LongWritable, Text> {
    final String[] needNodes = {"content", "contenttitle"};

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        String charsetName = conf.get("charsetName", "UTF-8");
        return new XMLRecordReader(needNodes, charsetName);
    }
}
