import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class WordCount2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        public static boolean isNumeric(String str) {
            String bigStr;
            try {
                bigStr = new BigDecimal(str).toString();
            } catch (Exception e) {
                return false;//异常 说明包含非数字。
            }
            return true;
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                int length = word.toString().length();
                if(length < 3) continue;
                if(isNumeric(word.toString())) continue;

                context.write(word, one);
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<Text, IntWritable> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            countMap.put(new Text(key),new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

            int counter = 1;
            for (Text key : sortedMap.keySet()) {
                String skey = counter+":"+key.toString();
                context.write(new Text(skey), sortedMap.get(key));
                if (counter == 100) {
                    break;
                }
                counter += 1;
            }
        }

    }



    public static int driver(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4) && (remainingArgs.length != 6)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile] [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        try{
            MyOutPutFormat.setOutputName(job, otherArgs.get(0).split("/")[otherArgs.get(0).split("/").length-1]);}
        catch (Exception e) {
            MyOutPutFormat.setOutputName(job, "Collection-of-Shakespeare");}
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String path = args[0];
        String uri = "hdfs://172.19.0.2:9000"+path;
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path Path = new Path(uri);
            FileStatus[] status = fs.listStatus(Path);
            for (int i = 0; i < status.length; i++) {
                String f = status[i].getPath().toString();
                int l = f.split("/").length;
                String[] para = new String[]{"-Dwordcount.case.sensitive=false",
                        "-Dmapreduce.output.textoutputformat.separator=,",
                        args[0] + f.split("/")[l - 1],
                        args[1] + f.split("/")[l - 1].split("\\.")[0],
                        "-skip", "stop/punctuation.txt", "-skip", "stop/stop-word-list.txt"};
                driver(para);
                para = new String[]{"-Dwordcount.case.sensitive=false",
                        "-Dmapreduce.output.textoutputformat.separator=,",
                        args[0], args[1] + "summary", "-skip", "stop/punctuation.txt", "-skip", "stop/stop-word-list.txt"};
                driver(para);
            }
            fs.close();
            } catch (IOException e) {
            e.printStackTrace();
            }

//        		//要遍历的路径
//        File file = new File(path);		//获取其file对象
//        File[] fs = file.listFiles();	//遍历path下的文件和目录，放在File数组中
//        for(File f:fs){					//遍历File[]数组

    }
}