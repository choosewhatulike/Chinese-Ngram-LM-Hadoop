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
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            final int length = str.codePointCount(0, str.length());
            int[] buf = new int[length];
            for(int i = 0; i<length; ++i) {
                int idx = str.offsetByCodePoints(0, i);
                buf[i] = str.codePointAt(idx);
            }
            for(int i=0; i < length; ++i) {
                add(new String(buf, i ,1));
                if(i + 1 < length) {
                    add(new String(buf, i, 2));
                    if(i + 2 < length) {
                        add(new String(buf, i, 3));
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();
            LongWritable value = new LongWritable();
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
