import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Round3 {
    public static String input = Round2.output;
    public static String output = "s3://path-similarity-output/round3-output";
    public static String negative_test_set = "s3://negative-positive-preds/negative-preds.txt";
    public static String positive_test_set = "s3://negative-positive-preds/positive-preds.txt";
//    public static String output = "round3-output";
//    public static String negative_test_set = "negative-preds.txt";
//    public static String positive_test_set = "positive-preds.txt";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text myValue = new Text();
        private final Text myKey = new Text();
        private final HashMap<String, Set<String>> negativeTestSet = new HashMap<String, Set<String>>();
        private final HashMap<String, Set<String>> positiveTestSet = new HashMap<String, Set<String>>();

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                URI s3uriN = new URI(negative_test_set);
                URI s3uriP = new URI(positive_test_set);
                FileSystem fs = FileSystem.get(s3uriN, conf);
                Path negInputFile = new Path(negative_test_set);
                Path posInputFile = new Path(positive_test_set);
                FSDataInputStream fsDataInputStream = fs.open(negInputFile);
                BufferedReader in = new BufferedReader(new InputStreamReader(fsDataInputStream));
                String negativePreds = null;
                while ((negativePreds = in.readLine()) != null) {
                    String[] paths = negativePreds.split("\t");
                    negativeTestSet.computeIfAbsent(paths[0], k -> new HashSet<String>());
                    negativeTestSet.computeIfAbsent(paths[1], k -> new HashSet<String>());
                    negativeTestSet.get(paths[0]).add(negativePreds);
                    negativeTestSet.get(paths[1]).add(negativePreds);
                }
                in.close();
                fsDataInputStream.close();
                fs = FileSystem.get(s3uriP, conf);
                fsDataInputStream = fs.open(posInputFile);
                in = new BufferedReader(new InputStreamReader(fsDataInputStream));
                String positivePreds = null;
                while ((positivePreds = in.readLine()) != null) {
                    String[] paths = positivePreds.split("\t");
                    positiveTestSet.computeIfAbsent(paths[0], k -> new HashSet<String>());
                    positiveTestSet.computeIfAbsent(paths[1], k -> new HashSet<String>());
                    positiveTestSet.get(paths[0]).add(positivePreds);
                    positiveTestSet.get(paths[1]).add(positivePreds);
                }
            } catch (URISyntaxException | IOException e) {
                System.err.println("Caught exception while parsing the stop words file '"
                        + StringUtils.stringifyException(e));
            }
        }
        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String path1 = splits[0];
            String revP = reversePath(path1);
            if (splits.length != 2){
                return;
            }
            String table = splits[1];
            checkAndSendTableToReducer(path1,table,context);
            checkAndSendTableToReducer(revP,table,context);
        }
        private void checkAndSendTableToReducer(String path,String table, Context context) throws IOException, InterruptedException {
            if(positiveTestSet.containsKey(path)) {
                for (String p1p2 : positiveTestSet.get(path)) {
                    myKey.set("POSITIVE:\t"+p1p2);
                    myValue.set(table);
                    context.write(myKey, myValue);
                }
            }
            if(negativeTestSet.containsKey(path)) {
                for (String p1p2 : negativeTestSet.get(path)) {
                    myKey.set("NEGATIVE:\t"+p1p2);
                    myValue.set(table);
                    context.write(myKey, myValue);
                }
            }
        }

        private String reversePath(String path){
            //path: X verb Y
            String[] splits = path.split(" ");
            StringBuilder sb = new StringBuilder(splits[splits.length-1]);
            sb.append(" ");
            for(int i=1;i<splits.length-1;i++){
                sb.append(splits[i]);
                sb.append(" ");
            }
            sb.append(splits[0]);
            return sb.toString();
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        private final Text myKey = new Text();
        private final Text myValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, List<Double>> slotXMiValues = new HashMap<>();
            HashMap<String, List<Double>> slotYMiValues = new HashMap<>();
            String[] keySplits = key.toString().split("\t");
            String path1 = keySplits[1];
            String path2 = keySplits[2];
            boolean isRevP1 = isReversed(path1);//checks if path is in shape: Y verb X
            boolean isRevP2 = isReversed(path2);
            double numeratorSim = 0;
            double denominatorSim = 0;
            double simSlotX = 0;
            double simSlotY = 0;
            boolean reverse = isRevP1; //for first iteration
            for(Text table:values) {
                String[] slotX;
                String[] slotY;
                if (reverse) {
                    slotX = table.toString().split(";")[1].split(",");
                    slotY = table.toString().split(";")[0].split(",");
                } else {
                    slotX = table.toString().split(";")[0].split(",");
                    slotY = table.toString().split(";")[1].split(",");
                }
                for (String x : slotX) {
                    String[] xVals = x.split(" ");
                    slotXMiValues.computeIfAbsent(xVals[0], k -> new ArrayList<>());
                    slotXMiValues.get(xVals[0]).add(Double.parseDouble(xVals[1]));
                }
                for (String y : slotY) {
                    String[] yVals = y.split(" ");
                    slotYMiValues.computeIfAbsent(yVals[0], k -> new ArrayList<>());
                    slotYMiValues.get(yVals[0]).add(Double.parseDouble(yVals[1]));
                }
                reverse = isRevP2; //reverse value should be isRevP2 for next iteration
            }

            for (List<Double> xMiVals : slotXMiValues.values()) {
                if (xMiVals.size() == 2) {
                    numeratorSim += xMiVals.get(0) + xMiVals.get(1);
                    denominatorSim += xMiVals.get(0) + xMiVals.get(1);
                } else denominatorSim += xMiVals.get(0);
            }
            if (denominatorSim != 0)
                simSlotX = numeratorSim / denominatorSim;
            numeratorSim = 0;
            denominatorSim = 0;
            for (List<Double> yMiVals : slotYMiValues.values()) {
                if (yMiVals.size() == 2) {
                    numeratorSim += yMiVals.get(0) + yMiVals.get(1);
                    denominatorSim += yMiVals.get(0) + yMiVals.get(1);
                } else denominatorSim += yMiVals.get(0);
            }
            if (denominatorSim != 0)
                simSlotY = numeratorSim / denominatorSim;
            if (simSlotX * simSlotY > 0) {
                myKey.set(key.toString());
                myValue.set(String.format("%f", Math.sqrt(simSlotX * simSlotY)));
                context.write(myKey, myValue);
            }
        }

        private boolean isReversed(String path){
            return path.split(" ")[0].equals("Y");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Round3");
        job.setJarByClass(Round3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

