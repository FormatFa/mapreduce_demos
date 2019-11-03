package formatfa.bigdata.mrdemos.commonfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

//寻找共同好友 2019-11-03
public class CommonFriend {

    public static class Stage1Map extends Mapper<LongWritable, Text,Text,Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
//            某人和他的朋友列表
            String a_friends[] = line.split(":");
            if(a_friends.length<2)
            {
                System.err .println("错误数据:"+line);
                return;
            }
            String friends []= a_friends[1].split(",");
//            friends 的好友
            for(String friend :friends)
            {
                context.write(new Text(friend),new Text(a_friends[0]));
            }
        }
    }
//
    public static class Stage1Reduce extends Reducer<Text,Text,Text,Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//           写出
            StringBuilder sb = new StringBuilder();
            for(Text item:values)
            {
                sb.append(item.toString());
                sb.append(",");
            }
//            删掉末尾的,
            sb.deleteCharAt(sb.length()-1);
            context.write(key,new Text(sb.toString()));

        }
    }

    static class Stage2Map extends Mapper<LongWritable,Text,Text,Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String as_friend[] = value.toString().split("\t");
            String friend = as_friend[0];
            String as[] = as_friend[1].split(",");
//            as 两两之间输出
//            保证顺序,下面组合时a-b和b-a变成a-b
            Arrays.sort(as);
            for(int i =0;i<as.length-1;i+=1)
            {
                for(int j =i+1;j<as.length;j+=1)
                {
                    String our = as[i]+"-"+as[j];
                    context.write(new Text(our),new Text(friend));
                }
            }

        }
    }

    static class Stage2Reduce extends Reducer<Text,Text,Text,Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder common_friends = new StringBuilder();
            for(Text item :values)
            {

                common_friends.append(item.toString());
                common_friends.append(",");
            }
            common_friends.deleteCharAt(common_friends.length()-1);

            context.write(key,new Text(common_friends.toString()));


        }
    }
//    结果输出到 out/commonfriend/stage2
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(CommonFriend.class);
            job.setMapperClass(Stage1Map.class);
            job.setReducerClass(Stage1Reduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);

            Path out = new Path("out/commonfriend/stage1");
            if(fs.exists(out)){fs.delete(out,true);}

            FileInputFormat.addInputPath(job,new Path("data/commonfriend"));
            FileOutputFormat.setOutputPath(job,out);
            job.waitForCompletion(true);

//            ----------------第二个任务--------------

            Job job2 = Job.getInstance(conf);

            job2.setJarByClass(CommonFriend.class);
            job2.setMapperClass(Stage2Map.class);
            job2.setReducerClass(Stage2Reduce.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            out = new Path("out/commonfriend/stage2");
            if(fs.exists(out)){fs.delete(out,true);}

            FileInputFormat.addInputPath(job2,new Path("out/commonfriend/stage1"));
            FileOutputFormat.setOutputPath(job2,out);
            System.exit(job2.waitForCompletion(true)?0:1);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
