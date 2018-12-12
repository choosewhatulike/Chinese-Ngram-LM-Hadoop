import java.io.IOException;
import java.util.*;

import com.sun.jersey.core.util.StringKeyIgnoreCaseMultivaluedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriGramCount {

    public static class TriGramMapper
            extends Mapper<Object, Text, Text, LongWritable> {
        private Map<String, Long> wordCount = new HashMap<String, Long>();
        long nWords = 0L;
        final int N = 3;

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
        }

        private void addTri(String str) {
            final int length = str.length();
            for(int i=0; i < length; ++i) {
                add(str.substring(i, i+1));
                nWords += 1;
                if(i+1 < length) {
                    add(str.substring(i, i+2));
                    if(i+2 < length) {
                        add(str.substring(i, i+3));
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            str = Utils.filter(str);
            String[] tokens = Utils.splitPunct(str);
            for(String t : tokens) {
                if(t.length() > 0) addTri(t);
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


    public static class TriGramReducer
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
