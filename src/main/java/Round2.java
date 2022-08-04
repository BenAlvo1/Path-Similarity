import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Round2 {

    public static String output = "s3://path-similarity-output/round2-output;";
    //public static String output = "round2-output";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Text myKey = new Text();
        private final Text myValue = new Text();

        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            // value is <path>
            String[] splits = value.toString().split("\t");
            String path = splits[0];
            String originalValue = splits[1];
            myValue.set(originalValue);
            if (originalValue.contains(";")) { // value is table
                myKey.set(String.format("%s,1", path));
            } else { // value is <w, slot, cw>
                myKey.set(String.format("%s,2", path));
            }
            context.write(myKey, myValue); // Emit(<path, n> <table>)
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keySplits = key.toString().split(",");
            if (keySplits.length > 1) { // key is <path, n>
                key = new Text(keySplits[0]); // partition by <path> only
            }
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    // aggregate all occurrences of word in decade
    // output is <w1, w2, decade> <total occurrences>
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text myKey = new Text();
        private final Text myValue = new Text();


        private Map<String, Integer> countXSlots;
        private Map<String, Integer> countYSlots;
        private Map<String, Double> miXSlots;
        private Map<String, Double> miYSlots;
        private int occursPerSlot;
        private String lastPath;
        private long slotCount;

        @Override
        public void setup(Reducer<Text, Text, Text, Text>.Context context) {
            lastPath = "";
            occursPerSlot = 0;
            countXSlots = new HashMap<>();
            countYSlots = new HashMap<>();
            miXSlots = new HashMap<>();
            miYSlots = new HashMap<>();
            slotCount = context.getConfiguration().getLong(MyCounter.N_COUNTERS.SLOT.name(), 0);
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key is <path, 1> or <path, 2>
            String[] keySplits = key.toString().split(",");
            String path = keySplits[0];
            String valueType = keySplits[1];
            if (valueType.equals("1")) { // value is <table>
                lastPath = path;
                Text value = values.iterator().next();
                loadTable(value.toString());
            } else if (valueType.equals("2") && path.equals(lastPath)) {
                miXSlots.clear();
                miYSlots.clear();
                for (Text value : values) { // value is <w  slot  cw>
                    String[] valueSplits = value.toString().split(" ");
                    String word = valueSplits[0];
                    String slot = valueSplits[1];
                    int wordCount = Integer.parseInt(valueSplits[2]);
                    calcMI(slot, word, wordCount);
                }
                String table = buildTable();
                myValue.set(table);
                myKey.set(path);
                //System.out.println(path + "         " + table);
                context.write(myKey, myValue); // Emit(<path>, <table>)
            }
        }

        // Table format: <XSlots>;<YSlots>
        // <XSlots> = comma seperated pair of [<w> <c>]
        // <YSlots> = comma seperated pair of [<w> <c>]
        private String buildTable() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Double> entry : miXSlots.entrySet()) {
                sb.append(String.format("%s %f,", entry.getKey(), entry.getValue()));
            }
            if(sb.length()>0)
                sb.deleteCharAt(sb.length() - 1); // remove last comma ','
            sb.append(";");

            for (Map.Entry<String, Double> entry : miYSlots.entrySet()) {
                sb.append(String.format("%s %f,", entry.getKey(), entry.getValue()));
            }
            if(sb.length()>0)
                sb.deleteCharAt(sb.length() - 1); // remove last comma ','
            return sb.toString();
        }

        private void calcMI(String slot, String word, int wordCount) {
            Map<String, Double> miSlots = slot.equals("X") ? miXSlots : miYSlots;
            Map<String, Integer> cSlots = slot.equals("X") ? countXSlots : countYSlots;
            int count_w_in_path = (cSlots.get(word)!=null)?cSlots.get(word):0; // |p,Slot,w|
            long total_slot_count = this.slotCount; // |*,Slot,*|
            int all_words_in_path = this.occursPerSlot; // |p, Slot, *|
            int total_words_all_paths = wordCount; // |*, Slot, w|

            double numerator = count_w_in_path * total_slot_count;
            double denominator = all_words_in_path * total_words_all_paths;
            double mi = Math.log(numerator / denominator);
            if(mi>0)
                miSlots.put(word, mi);
            else{
                mi = 0;
                miSlots.put(word, mi);
            }
        }

        /*
        Table format: <n>;<XSlots>;<YSlots>
        <n> = number of total words in slot in path |p, slot, *|
        <XSlots> = comma seperated pairs of word to count (seperated by space) <w> <c>
        <YSlots> = comma seperated pairs of word to count (seperated by space) <w> <c>
         */
        private void loadTable(String table) {
            // Table format: <n>;<XSlots>;<YSlots>
            countXSlots.clear();
            countYSlots.clear();
            String[] tableParts = table.split(";");
            this.occursPerSlot = Integer.parseInt(tableParts[0]);
            String[] xWords = tableParts[1].split(",");
            String[] yWords = tableParts[2].split(",");
            for (String wordToCount : xWords) {
                String[] split = wordToCount.split(" ");
                String w = split[0];
                int count = Integer.parseInt(split[1]);
                countXSlots.put(w, count);
            }
            for (String wordToCount : yWords) {
                String[] split = wordToCount.split(" ");
                String w = split[0];
                int count = Integer.parseInt(split[1]);
                countYSlots.put(w, count);
            }
        }

    }


    public static void main(String[] args) throws Exception {
    }
}