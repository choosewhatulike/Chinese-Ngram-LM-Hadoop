import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class NGramCount {
    public static class CountMapper
            extends Mapper<Object, Text, Text, LongWritable> {
        private Map<String, Long> wordCount = new HashMap<String, Long>();
        long nWords = 0L;
        int N;
        final static String Num = "NumGram";

        private void add(String str) {
            if (wordCount.containsKey(str)){
                long val = wordCount.get(str);
                wordCount.put(str, val + 1);
            } else {
                wordCount.put(str, 1L);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            wordCount.clear();
            nWords = 0L;
            Configuration conf = context.getConfiguration();
            N = conf.getInt(Num, 3);
        }

        private void addNGram(String str) {
            final int length = str.length();

            for(int i=0; i < length; ++i) {
                add(str.substring(i, i+1));
                nWords += 1;
                for(int j=1; j < N; ++j) {
                    if(i + j < length) {
                        add(str.substring(i, i+j+1));
                    } else break;
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            str = Utils.filter(str);
            String[] tokens = Utils.splitPunct(str);
            for(String t : tokens) {
                addNGram(t);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();
            LongWritable value = new LongWritable();
            key.set("<all>");
            value.set(nWords);
            context.write(key, value);

            for(Map.Entry<String, Long> kv : wordCount.entrySet()) {
                key.set(kv.getKey());
                value.set(kv.getValue());
                context.write(key, value);
            }
            wordCount.clear();
        }
    }


    public static class CountReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
