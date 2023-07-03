package step1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class WordFrequencyCount {

    private String textFilename;
    
    public WordFrequencyCount(String textFilename) {
        this.textFilename = textFilename;
    }
    
    public HashMap<String, Integer> countWordFrequencies() {
        HashMap<String, Integer> wordFrequencies = new HashMap<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(textFilename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    wordFrequencies.put(word, wordFrequencies.getOrDefault(word, 0) + 1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return wordFrequencies;
    }

    public static void main(String[] args) {
        String filename = "step1/sante_publique.txt";
        WordFrequencyCount wordFrequencyCount = new WordFrequencyCount(filename);

        long startTimeCount = System.currentTimeMillis();
        HashMap<String, Integer> wordFrequencies = wordFrequencyCount.countWordFrequencies();
        long endTimeCount = System.currentTimeMillis();
        ArrayList<HashMap.Entry<String, Integer>> sortedEntries = new ArrayList<>(wordFrequencies.entrySet());
        sortedEntries.sort(HashMap.Entry.<String, Integer>comparingByValue().reversed().thenComparing(HashMap.Entry.comparingByKey()).reversed());
        long endTimeSort = System.currentTimeMillis();


        
        for (HashMap.Entry<String, Integer> entry : sortedEntries) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("Counting time taken: " + (endTimeCount - startTimeCount) + "ms.");
        System.out.println("Sorting time taken: " + (endTimeSort - endTimeCount) + "ms.");
    }
}
