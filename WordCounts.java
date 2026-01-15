package com.udacity.webcrawler;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toMap;

/**
 * Utility class that sorts the map of word counts.
 *
 * <p>TODO: Reimplement the sort() method using only the Stream API and lambdas and/or method
 *          references.
 */
final class WordCounts {

    //TODO: Reimplement this method using only the Stream API and lambdas and/or method references.

    static Map<String, Integer> sort(Map<String, Integer> wordCounts, int popularWordCount) {

        // TODO: Reimplement this method using only the Stream API and lambdas and/or method references.

        return wordCounts.entrySet().stream()
                .sorted(new WordCountComparator())
                .limit(min(popularWordCount, wordCounts.size()))
                .collect(toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (k, v) -> k,
                        LinkedHashMap::new));
    }
    /*
     * Comparator used to compare two word entries.
     */
    private static final class WordCountComparator implements Comparator<Map.Entry<String, Integer>> {
        @Override
        public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
            if (!a.getValue().equals(b.getValue())) {
                return b.getValue() - a.getValue();
            }
            if (a.getKey().length() != b.getKey().length()) {
                return b.getKey().length() - a.getKey().length();
            }
            return a.getKey().compareTo(b.getKey());
        }
    }
    /*
     * Private constructor to prevent object creation.
     */
    private WordCounts() {
        // This class cannot be instantiated
    }
}