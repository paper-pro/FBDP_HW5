import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author FengZhen
 * setOutputName是protected方法，所以无法直接调用，只能自定义TextOutPutFormat重写该方法
 */
public class MyOutPutFormat extends TextOutputFormat<Text, IntWritable>{

    protected static void setOutputName(JobContext job, String name) {
        job.getConfiguration().set(BASE_OUTPUT_NAME, name);
    }
}