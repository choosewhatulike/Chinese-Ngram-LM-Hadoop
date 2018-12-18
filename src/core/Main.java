import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String trainPath = "/user/rocks1/15307130288/input/test/testdata.train";
        String devPath = "/user/rocks1/15307130288/input/test/testdata.dev";
//        /user/rocks1/15307130288/input/data/news_sohusite_xml.dat
        String tmpPath = "/user/rocks1/15307130288/output/tmp/";
        String outPath = "/user/rocks1/15307130288/output/out/";
        String evalPath = "/user/rocks1/15307130288/output/eval/";

        String task = "";

        int nGram = 3;
        if(args.length > 0) {
            if(args[0].equals("train") || args[0].equals("eval") || args[0].equals("all")) {
                task = args[0];
            }else {
                throw new Exception("need set a task for jar");
            }
        }
        if (args.length > 1) { nGram = Integer.parseInt(args[1]); }
        if (args.length > 2) { outPath = args[2] + "out/"; evalPath = args[2] + "eval/"; }
        if (args.length > 3) {
            if(task.equals("eval")) {
                devPath = args[3];
            } else {
                trainPath =args[3];
            }
        }
        if (args.length > 4 && task.equals("all")) { devPath = args[4]; }

        Path in = new Path(trainPath);
        Path dev = new Path(devPath);
        Path tmp1 = new Path(tmpPath + "01/");
        Path tmp2 = new Path(tmpPath + "02/");
        Path out = new Path(outPath);
        Path evalp = new Path(evalPath);


        conf.set("charsetName", "UTF-8");
        conf.setInt(NGramCount.CountMapper.Num, nGram);
        conf.set(Eval.vocabPathName, outPath);
        conf.setInt(Eval.binNumName, 10);

        boolean res = true;
        if(task.equals("train") || task.equals("all")) {
            out.getFileSystem(conf).delete(out, true);
            tmp1.getFileSystem(conf).delete(tmp1, true);

            Job job = Job.getInstance(conf, "count job");
            job.setJarByClass(Main.class);
            job.setMapperClass(NGramCount.CountMapper.class);
            job.setReducerClass(NGramCount.CountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job, in);
            TextOutputFormat.setOutputPath(job, tmp1);

            res = job.waitForCompletion(true);
            if(!res) System.exit(1);

            Job job2 = Job.getInstance(conf, "freq job");
            job2.setJarByClass(Main.class);
            job2.setMapperClass(GramFreq.FreqMapper.class);
            job2.setReducerClass(GramFreq.FreqReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(MapWritable.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            KeyValueTextInputFormat.addInputPath(job2, tmp1);
            TextOutputFormat.setOutputPath(job2, out);

            res = job2.waitForCompletion(true);
            if(!res) System.exit(1);

        }

        if(task.equals("eval") || task.equals("all")) {
            evalp.getFileSystem(conf).delete(evalp, true);
            tmp2.getFileSystem(conf).delete(tmp2, true);

            Job job3 = Job.getInstance(conf, "eval job");
            job3.setJarByClass(Main.class);
            job3.setMapperClass(Eval.EvalMapper.class);
            job3.setReducerClass(Eval.EvalReducer.class);
            job3.setMapOutputKeyClass(IntWritable.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setInputFormatClass(TextInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job3, dev);
            TextOutputFormat.setOutputPath(job3, tmp2);

            res = job3.waitForCompletion(true);
            if(!res) System.exit(1);

            Job job4 = Job.getInstance(conf, "collect job");
            job4.setJarByClass(Main.class);
            job4.setMapperClass(EvalCollector.MyMapper.class);
            job4.setReducerClass(EvalCollector.MyReducer.class);
            job4.setMapOutputKeyClass(IntWritable.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setInputFormatClass(TextInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job4, tmp2);
            TextOutputFormat.setOutputPath(job4, evalp);

            res = job4.waitForCompletion(true);

            System.exit(res ? 0 : 1);
        }

    }
}


// nohup hadoop jar pj/out/artifacts/pj_jar/pj.jar Main /user/rocks1/15307130288/input/data/news_sohusite_xml.dat &
// hadoop fs -get /user/rocks1/15307130288/output/test/part-r-00000  debug02.txt