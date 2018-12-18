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
    public static final String NumWordName = "NumofWord";
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
                context.getConfiguration().setLong(NumWordName, val);
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

            for(Map.Entry<String, MapWritable> kv : charMap.entrySet()) {
                key.set(kv.getKey());
                MapWritable fmap = kv.getValue();
                context.write(key, fmap);
            }
            charMap.clear();
        }
    }


    public static class FreqReducer
            extends Reducer<Text, MapWritable, Text, Text> {
        private String curKey;
        private Text resultKey = new Text();
        private Text resultValue = new Text();
        private Utils.Vocab gramFreq = new Utils.Vocab();

        private void add(String follow, Long count) {
            if(follow.equals("<self>")) {
                follow = curKey;
            } else {
                follow = curKey + follow;
            }

            gramFreq.add(follow, count);
        }



        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            gramFreq.bin.clear();
            curKey = key.toString();

            for(MapWritable val : values) {
                for(Map.Entry<Writable, Writable> kv : val.entrySet()) {
                    String follow = ((Text) kv.getKey()).toString();
                    Long count = ((LongWritable) kv.getValue()).get();
                    add(follow, count);
                }
            }

            long nwords = context.getConfiguration().getLong(NumWordName, gramFreq.bin.size());
            nwords = Math.max(nwords, context.getCounter(VocabCounter.TOTAL_VOCAB).getValue());
            gramFreq.nWords = nwords;
            context.getCounter(VocabCounter.TOTAL_VOCAB).setValue(gramFreq.nWords);

            for(Map.Entry<String, Utils.Vocab.Values> kv : gramFreq.bin.entrySet()) {
                resultKey.set(kv.getKey());
                resultValue.set(kv.getValue().freq + "\t" + gramFreq.calcProb(kv.getKey()));
                context.write(resultKey, resultValue);
            }
        }
    }
}

