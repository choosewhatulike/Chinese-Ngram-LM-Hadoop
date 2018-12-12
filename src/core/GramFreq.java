import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import com.sun.jersey.core.util.StringKeyIgnoreCaseMultivaluedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GramFreq {
    public Job createJob(Configuration conf, Path inPath, Path outPath) throws IOException, InterruptedException {
        Job job = Job.getInstance(conf, "GramFreqCalculation");
        job.setJarByClass(getClass());
        job.setMapperClass(FreqMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(FreqReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, inPath);
        TextOutputFormat.setOutputPath(job, outPath);
        return job;
    }

    public static enum VocabCounter {
        TOTAL_VOCAB;
    }


    public static class FreqMapper
            extends Mapper<Text, Text, Text, MapWritable> {
        private Map<String, MapWritable> charMap = new HashMap<String, MapWritable>();

        private void add(String key, String follow, long val) {
            Text text = new Text(follow);
            if(charMap.containsKey(key)) {
                MapWritable fmap = charMap.get(key);
                if(fmap.containsKey(text)) {
                    LongWritable oldval = (LongWritable) fmap.get(text);
                    oldval.set(oldval.get() + val);
                    fmap.put(text, oldval);
                } else {
                    LongWritable lw = new LongWritable(val);
                    fmap.put(text, lw);
                }
            } else {
                MapWritable fmap = new MapWritable();
                fmap.put(text, new LongWritable(val));
                charMap.put(key, fmap);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            charMap.clear();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            long val = Long.parseLong(value.toString());
            String str = key.toString();
            final int length = str.length();

            if(str.equals("<all>")) {
                context.getCounter(VocabCounter.TOTAL_VOCAB).setValue(val);
                return;
            }

            if(length <= 1) {
                add(str, "<self>", val);
            } else {
                String newkey = str.substring(0, 1);
                String follow = str.substring(1);
                add(newkey, follow, val);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();

            long nWords = context.getCounter(VocabCounter.TOTAL_VOCAB).getValue();

            for(Map.Entry<String, MapWritable> kv : charMap.entrySet()) {
                key.set(kv.getKey());
                MapWritable fmap = kv.getValue();
                fmap.put(new Text("<all>"), new LongWritable(nWords));
                context.write(key, fmap);
            }
            charMap.clear();
        }
    }


    public static class FreqReducer
            extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private Map<String, Long> bin = new HashMap<String, Long>();
        private String curKey;
        private long nWords;
        private Text resultKey = new Text();
        private DoubleWritable resultValue = new DoubleWritable();

        private void add(String follow, Long count) {
            if(follow.equals("<self>")) {
                follow = curKey;
            } else {
                follow = curKey + follow;
            }

            if(bin.containsKey(follow)) {
                Long val = bin.get(follow);
                bin.put(follow, val + count);
            } else {
                bin.put(follow, count);
            }
        }

        private double calcProb(String str) throws IOException{
            final int length = str.length();
            if(length <= 1) {
                long count = bin.get(str);
                return (double) count / nWords;
            } else {
                String base = str.substring(0, length - 1);
                long baseCount = bin.get(base);
                long count = bin.get(str);
                return (double) count / baseCount;
            }
        }

//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            nWords = context.getCounter(VocabCounter.TOTAL_VOCAB).getValue();
//        }

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            bin.clear();
            curKey = key.toString();

            for(MapWritable val : values) {
                for(Map.Entry<Writable, Writable> kv : val.entrySet()) {
                    String follow = ((Text) kv.getKey()).toString();
                    Long count = ((LongWritable) kv.getValue()).get();
                    if (follow.equals("<all>")) nWords = count;
                    else add(follow, count);
                }
            }

            for(Map.Entry<String, Long> kv : bin.entrySet()) {
                resultKey.set(kv.getKey());
                resultValue.set(calcProb(kv.getKey()));
                context.write(resultKey, resultValue);
            }
            bin.clear();
        }
    }
}

