import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class Sorting_bak {
    public static class SortingMap extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private IntWritable one = new IntWritable(1);
        private IntWritable merchant_id;
        private int mode;


        protected void setup(Context context) throws IOException,InterruptedException{
            mode = context.getConfiguration().getInt("mode", 1);
        }

        public void map(LongWritable offset, Text line, Context context)throws
                IOException, InterruptedException{

            LogRelation data = new LogRelation(line.toString());
            merchant_id =new IntWritable(data.getMerchant_id());
            int action = data.getAction();
//          记关注的数量
            if(mode == 1) {
                context.write(merchant_id, one);
//                System.out.println("1");
            }
//          记购买的数量
            else if(mode == 2) {
                if (action == 2) {
                    context.write(merchant_id, one);
                }
            }
            else{
                System.err.println("<mode>输入错误(接受值1,2)");
            }
        }
    }

    public static class SortingReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        public void reduce(IntWritable key, Iterable<IntWritable> value, Context context)throws 
                IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val : value){
                sum += val.get();
            }
            IntWritable hits = new IntWritable(sum);
            context.write(key,hits);
        }
    }

    //针对IntWritable的倒排Comparator
    private  static class IntWritableDecreasingComparator extends IntWritable.Comparator{
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1 ,int l1, byte[] b2, int s2, int l2){
            return -super.compare(b1,s1,l1,b2,s2,l2);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassCastException, ClassNotFoundException {

        Path tempDir = new Path("temp_"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Job sortingJob = new Job();
        sortingJob.setJobName("counting");
        sortingJob.setJarByClass(Sorting_bak.class);
        try {
            sortingJob.getConfiguration().setInt("mode", Integer.parseInt(args[2]));

            sortingJob.setMapperClass(SortingMap.class);
            sortingJob.setMapOutputKeyClass(IntWritable.class);
            sortingJob.setMapOutputValueClass(IntWritable.class);

            sortingJob.setCombinerClass(SortingReduce.class);

            sortingJob.setReducerClass(SortingReduce.class);
            sortingJob.setOutputKeyClass(IntWritable.class);
            sortingJob.setOutputValueClass(IntWritable.class);

            sortingJob.setInputFormatClass(TextInputFormat.class);
            sortingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(sortingJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(sortingJob, tempDir);
            if(sortingJob.waitForCompletion(true)){
                Job job = new Job();
                job.setJobName("sorting");
                job.setJarByClass(Sorting_bak.class);

                FileInputFormat.addInputPath(job,tempDir);

                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setMapperClass(InverseMapper.class);
                job.setNumReduceTasks(1);

                FileOutputFormat.setOutputPath(job,new Path(args[1]));

                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(IntWritable.class);
                job.setSortComparatorClass(IntWritableDecreasingComparator.class);
                System.exit(job.waitForCompletion(true)?0:1);
            }
            System.out.println("finished!");
        }finally {
            FileSystem.get(new Configuration()).deleteOnExit(tempDir);
        }
    }
    
}
