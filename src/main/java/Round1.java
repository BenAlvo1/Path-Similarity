import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.Pair;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;


public class Round1 {

    public static String output = "s3://path-similarity-output/round1-output";
    //public static String output = "round1-output";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Text myValue = new Text();
        private final Text myKey = new Text();
        List<String> auxVerbs = Arrays.asList("am", "are", "is", "was", "were",
                "been","be","being", "can", "could", "do", "did", "does", "doing", "may",
                "might", "must", "shall", "should", "will", "would");

        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            try {
                String head_word = splits[0];
                String syntactic_ngram = splits[1];
                String total_count = splits[2];
                String[] tokens = syntactic_ngram.split(" ");
                if (tokens.length < 3) { // min length path is X verb Y
                    return;
                }
                int isHeadVerb = isHeadVerb(head_word, tokens);
                if (isHeadVerb == -1) {
                    return;
                }
                Pair<String, Integer> wSlotX = getSlotX(tokens, isHeadVerb);
                Pair<String, Integer> wSlotY = getSlotY(tokens, isHeadVerb);

                if (wSlotY == null || wSlotX == null || !isValidTree(tokens, isHeadVerb)) {
                    return;
                }
                // at this part path is legal
                // converting the path to present simple
                String path = buildPath(wSlotX.second, wSlotY.second, tokens);
                path = convertToPresentSimple(path);

                myKey.set(path);
                myValue.set(String.format("%s %s %s", wSlotX.first, wSlotY.first, total_count));
                context.write(myKey, myValue); // Emit (<path> <w1 w2 c>)


                myValue.set(total_count);
                myKey.set(String.format("%s,%s,%s", wSlotX.first, "X", "1"));
                context.write(myKey, myValue); // Emit (<w1, x, 1> <c>)
                myKey.set(String.format("%s,%s,%s", wSlotY.first, "Y", "1"));
                context.write(myKey, myValue); // Emit (<w2, y, 1> <c>)

                myValue.set(""); // value doesn't matter. just need to know to which paths send w count.
                // adding path to key and ignoring values results each path will just one info about a count of word in its table
                myKey.set(String.format("%s,%s,%s,%s", wSlotX.first, "X", "2", path));
                context.write(myKey, myValue); // Emit (<w1, x, 2, path> <>)
                myKey.set(String.format("%s,%s,%s,%s", wSlotY.first, "Y", "2", path));
                context.write(myKey, myValue); // Emit (<w2, y, 2, path> <>)

                //increment the total slot counter by <c> times
                context.getCounter(MyCounter.N_COUNTERS.SLOT).increment(Long.parseLong(total_count));
            }
            catch (Exception e){
                System.out.println("Error in map");
                e.printStackTrace();
                System.out.println(Arrays.toString(splits));
            }
        }

        private int isHeadVerb(String head_word, String[] tokens) {
            int verbIndex = 0;
            if(auxVerbs.contains(head_word))
                return -1;
            for (String token : tokens) {
                String[] splits = token.split("/");
                if (splits[0].equals(head_word) && splits[1].contains("VB")) {
                    return verbIndex;
                }
                verbIndex++;
            }
            return -1;
        }

        private String convertToPresentSimple(String path) {
            List<String> tokenLemma = new Sentence(path.substring(1, path.length() - 1)).lemmas();
            StringBuilder lemma = new StringBuilder("X ");
            for (String s : tokenLemma) {
                lemma.append(s);
                lemma.append(" ");
            }
            lemma.append("Y");
            return lemma.toString();
        }

        private Pair<String,Integer> getSlotX(String[] tokens,int verbIdx) {
            int index = 0;
            for(String xToken:tokens) {
                String[] splits = xToken.split("/");
                if(splits[1].contains("NN")&&splits[2].contains("subj")&&index<verbIdx) {
                    return new Pair<>(cleanEnglishWord(splits[0]),index);// xSlot must be noun
                }
                index++;
            }
            return null;
        }

        private boolean isValidTree(String[] tokens,int verbIdx){
            int pointToVerb = 0;
            for(String token:tokens) {
                String[] splits = token.split("/");
                if(Integer.parseInt(splits[3])==verbIdx+1) {
                    pointToVerb++;
                }
            }
            return pointToVerb >= 2;
        }

        private Pair<String,Integer> getSlotY(String[] tokens,int verbIdx) {
            int index = 0;
            for(String yToken:tokens) {
                String[] splits = yToken.split("/");
                if(splits[1].contains("NN")&&splits[2].contains("obj")&&index>verbIdx) {
                    return new Pair<>(cleanEnglishWord(splits[0]),index);// ySlot must be noun
                }
                index++;
            }
            return null;
        }

        private String cleanEnglishWord(String word) {
            return word.replaceAll("[^A-Za-z]","").toLowerCase();
        }

        private String buildPath(int slotXIdx,int slotYIdx,String[] tokens) {
            StringBuilder sb = new StringBuilder("X ");
            String[] splits;
            int startIdx = slotXIdx+1;
            int endIdx = slotYIdx;
            for (int i = startIdx; i < endIdx; i++) { // go pass all tokens except for first and last
                splits = tokens[i].split("/");
                sb.append(splits[0]);
                sb.append(" ");
            }
            sb.append("Y");
            return sb.toString();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keySplits = key.toString().split(",");
            if (keySplits.length > 2) { // key is <w, slot, n>
                key = new Text(keySplits[0] + " " + keySplits[1]); // partition by <w, slot> only
            }
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        private final static Text myValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                int total_w = 0;
                int new_w1w2_count = 0;
                Map<String, Integer> words = new HashMap<>();
                String[] keySplits = key.toString().split(",");
                if (keySplits.length == 1) {
                    for (Text val : values) {
                        String[] valSplits = val.toString().split(" ");
                        int c = Integer.parseInt(valSplits[2]);
                        new_w1w2_count = words.getOrDefault(valSplits[0] + " " + valSplits[1], 0) + c;
                        words.put(valSplits[0] + " " + valSplits[1], new_w1w2_count);
                    }
                    for (Map.Entry<String, Integer> entry : words.entrySet()) {
                        myValue.set(entry.getKey() + " " + entry.getValue());
                        context.write(key, myValue);
                    }
                } else if (keySplits.length == 3 && keySplits[2].equals("1")) {
                    //values: <c1>,<c2>....<cn>
                    for (Text count : values) {
                        total_w += Integer.parseInt(String.valueOf(count));
                    }
                    myValue.set(String.valueOf(total_w));
                    context.write(key, myValue);
                } else if (keySplits.length == 4 && keySplits[2].equals("2")) { // key is <w, slot, 2>, values are <path, c>
                    myValue.set("");
                    context.write(key, myValue);
                }
            }
            catch (Exception e){
                System.out.println("Error in reduce");
                e.printStackTrace();
            }

        }
    }

    // aggregate all occurrences of word in decade
    // output is <w1, w2, decade> <total occurrences>
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text myKey = new Text();
        private final Text myValue = new Text();
        private int total_w_all_paths;
        private String lastWord;

        @Override
        public void setup(Reducer<Text, Text, Text, Text>.Context context) {
            total_w_all_paths = 0;
            lastWord = "";
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key is <path> or <w, slot, n>
            try {
                String[] keySplits = key.toString().split(",");
                if (keySplits.length == 1) { // key is <path>, values are <w1  w2  c>
                    Map<String, Integer> xSlots = new HashMap<>();
                    Map<String, Integer> ySlots = new HashMap<>();
                    int occursPerSlot = 0;
                    for (Text value : values) {
                        String[] valueSplits = value.toString().split(" ");
                        String w1 = valueSplits[0];
                        String w2 = valueSplits[1];
                        int c = Integer.parseInt(valueSplits[2]);
                        occursPerSlot += c;
                        Integer w1NewCount = xSlots.getOrDefault(w1, 0) + c;
                        Integer w2NewCount = ySlots.getOrDefault(w2, 0) + c;
                        xSlots.put(w1, w1NewCount);
                        ySlots.put(w2, w2NewCount);
                    }

                    String table = buildTable(occursPerSlot, xSlots, ySlots);
                    myValue.set(table);
                    context.write(key, myValue); // Emit(<path>, <table>)
                } else if (keySplits.length == 3 && keySplits[2].equals("1")) { // key is <w, slot, 1>, values are <path, c>
                    // count sum of w occurs in specific slot
                    this.total_w_all_paths = 0;
                    lastWord = keySplits[0];
                    for (Text value : values) {
                        int c = Integer.parseInt(value.toString());
                        total_w_all_paths += c;
                    }
                } else if (keySplits.length == 4 && keySplits[2].equals("2") && keySplits[0].equals(lastWord)) { // key is <w, slot, 2>, values are <path, c>
                    String path = keySplits[3];
                    myKey.set(path);
                    myValue.set(String.format("%s %s %d", lastWord, keySplits[1], total_w_all_paths));
                    context.write(myKey, myValue);
                }
            }
            catch (Exception e){
                System.out.println("Error in reduce");
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }

        // Table format: <SlotCount>;<XSlots>;<YSlots>
        // <SlotCount> = String representing a number
        // <XSlots> = comma seperated pair of [<w> <c>]
        // <YSlots> = comma seperated pair of [<w> <c>]
        private String buildTable(int occursPerSlot, Map<String, Integer> xSlots, Map<String, Integer> ySlots) {
            StringBuilder sb = new StringBuilder(String.format("%d;", occursPerSlot));
            for (Map.Entry<String, Integer> entry : xSlots.entrySet()) {
                sb.append(String.format("%s %d,", entry.getKey().replace(",","").
                        replace(";",""), entry.getValue()));
            }
            sb.deleteCharAt(sb.length() - 1); // remove last comma ','
            sb.append(";");

            for (Map.Entry<String, Integer> entry : ySlots.entrySet()) {
                sb.append(String.format("%s %d,", entry.getKey().replace(",","").
                        replace(";",""), entry.getValue()));
            }
            sb.deleteCharAt(sb.length() - 1); // remove last comma ','
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {
    }
}