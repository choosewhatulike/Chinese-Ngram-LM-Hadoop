import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {
    LongWritable key = new LongWritable();
    Text value = new Text();
    long startPos;
    long endPos;
    int curNode = -1;
    String[] _nodes;
    byte[][] startNodes;
    byte[][] endNodes;
    FSDataInputStream stream;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    XMLRecordReader(String[] nodes) {
        _nodes = nodes.clone();
        int size = nodes.length;
        startNodes = new byte[size][];
        endNodes = new byte[size][];
        for(int i=0; i<size; ++i) {
            startNodes[i] = ("<" + nodes[i] + ">").getBytes();
            endNodes[i] = ("</" + nodes[i] + ">").getBytes();
        }
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fileSystem = path.getFileSystem(conf);
        stream = fileSystem.open(path);
        startPos = fileSplit.getStart();
        stream.seek(startPos);
        endPos = startPos + fileSplit.getLength();
        buffer.reset();
    }

    private int findMatch(byte[][] bytesList, boolean withinNode) throws IOException, InterruptedException {
        int[] match_idx = new int[bytesList.length];
        for(int i=0; i < match_idx.length; ++i){
            match_idx[i] = 0;
        }
        while (true) {
            int b = stream.read();
            if(b == -1) return -1;

            if(withinNode) buffer.write(b);

            for(int i=0; i < match_idx.length; ++i){
                int idx = match_idx[i];
                if (b == bytesList[i][idx]) {
                    match_idx[i]++;
                    if (match_idx[i] >= bytesList[i].length) return i;
                } else match_idx[i] = 0;
            }
            if(! withinNode && stream.getPos() >= endPos) {
                boolean matching = false;
                for(int idx : match_idx){
                    if(idx > 0) {
                        matching = true;
                        break;
                    }
                }
                if(!matching) return -1;
            }
        }
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(startPos < endPos) {
            curNode = findMatch(startNodes, false);
            if(curNode >= 0) {
                key.set(stream.getPos());
                try {
                    // read data
                    int node = findMatch(endNodes, true);
                    value.set(buffer.getData(), 0, buffer.getLength() - endNodes[curNode].length);
                    return true;
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        if(startPos == endPos) {
            return 0.0f;
        }
        return Math.min(1.0f, (float)(stream.getPos() - startPos) / (endPos - startPos));
    }

    public void close() throws IOException {
        stream.close();
    }

}
