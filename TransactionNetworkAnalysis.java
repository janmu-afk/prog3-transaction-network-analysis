import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TransactionNetworkAnalysis {

    static HashSet<String> makeBAYC (String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = "";
        boolean skipHeader = true;  // skipping the header descriptor
        HashSet<String> bayc = new HashSet<>();
        while ((line = reader.readLine()) != null) {
            if (skipHeader) { skipHeader = false; continue; }
            String[] data = line.split(","); // csv
            bayc.add(data[4].trim() + "-" + data[5].trim()); // the from-to column
        }
        reader.close();
        return bayc;
    }

    static HashSet<String> makeBlacklist(String file) throws IOException {
        HashSet<String> blklist = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = "";
        while ((line = reader.readLine()) != null) {
            line = line.trim().replace("\"", ""); // make it easier to parse, like a comma list
            if (!line.isEmpty() && !line.equals("[") && !line.equals("]")) {
                blklist.add(line.replace(",", "")); // json
            }
        }
        reader.close();
        return blklist;
    }

    public static void main(String[] args) throws IOException {
        // init the hashsets
        HashSet<String> bayc = makeBAYC("boredapeyachtclub.csv");
        HashSet<String> blacklist = new HashSet<>();

        // several files in the folder
        File folder = new File("blacklist");
        // add them to an array
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));

        // combine them into a hashset
        if (files != null) {
            for (File file : files) {
                blacklist.addAll(makeBlacklist(file.getAbsolutePath()));
            }
        }

        // main part
        // init the reader and writer
        BufferedReader reader = new BufferedReader(new FileReader("prog3ETNsample.csv"));
        String line = "";

        // the linkability network
        Map<String, Map<String, Integer>> linkabilityNetwork = new HashMap<>();
        // map of weight frequencies
        Map<Integer, Integer> frequencies = new HashMap<>();

        // iterate through the ETN chunk
        while ((line = reader.readLine()) != null) {
            String[] columns = line.split(","); // split the columns
            String from = columns[5].trim();
            String to = columns[6].trim();

            // is either of the columns in the blacklist?
            if (blacklist.contains(from) || blacklist.contains(to)) continue;

            // is the pair contained within the BAYC set?
            if (!bayc.contains(from + "-" + to)) continue;

            // if both checks pass, we can add it to the network (only if it's unique)
            linkabilityNetwork.putIfAbsent(from, new HashMap<>());
            linkabilityNetwork.get(from).put(to, 1); // the weight is initially 1 since it's a direct path

            // retroactively applying weight increases to previous entries (checking if the current from is among the to's)
            // also converting it into an entry set because iteration
            for (Map.Entry<String, Map<String, Integer>> entry : linkabilityNetwork.entrySet()) {
                
                // instantiate the entry's values
                String previousFrom = entry.getKey();
                Map<String, Integer> connections = entry.getValue();
                
                // does any of the previous nodes point to the current?
                if (connections.containsKey(from)) {
                    int updatedWeight = connections.get(from) + 1;
                    
                    // fetch the appropriate connection and update its weight
                    linkabilityNetwork.get(previousFrom).put(to, updatedWeight);
                }
            }

        }

        reader.close();

        // I know writing separately takes more time but this is the easiest solution
        BufferedWriter writer = new BufferedWriter(new FileWriter("output.csv"));
        // write the header
        writer.write("from,to,weight");
        writer.newLine();

        for (Map.Entry<String, Map<String, Integer>> entry : linkabilityNetwork.entrySet()) {
            String from = entry.getKey();
            Map<String, Integer> connections = entry.getValue();
            
            // have to iterate through the inner maps as well
            // every entry will (hopefully) be an instance of the from, and one of its inner maps
            for (Map.Entry<String, Integer> connection : connections.entrySet()) {
                String to = connection.getKey();
                int weight = connection.getValue();

                // write entry to file
                writer.write(from + "," + to + "," + weight);
                writer.newLine();

                // update the frequency count for the weight
                frequencies.put(weight, frequencies.getOrDefault(weight, 0) + 1);
            }
        }

        writer.flush();
        writer.close();

        // printing the weights categorized by frequency
        for (Map.Entry<Integer, Integer> entry : frequencies.entrySet()) {
            System.out.println("weight " + entry.getKey() + " appears " + entry.getValue() + " times");
        }





    }


}
