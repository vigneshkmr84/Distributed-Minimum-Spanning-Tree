package utils;

import java.util.List;

public class MSTUtils {
    public static List<Integer> compareWeights(List<Integer> firstWeight, List<Integer> secondWeight) {
        //Compare based on edge weights first, the smaller one wins.
        if (firstWeight.get(0).compareTo(secondWeight.get(0)) != 0) {
            return firstWeight.get(0).compareTo(secondWeight.get(0)) < 0 ? firstWeight: secondWeight;
        } else {
            if (firstWeight.get(1).compareTo(secondWeight.get(1)) != 0) {
                return firstWeight.get(1).compareTo(secondWeight.get(1)) < 0 ? firstWeight : secondWeight;
            } else {
                return firstWeight.get(2).compareTo(secondWeight.get(2)) < 0 ? firstWeight : secondWeight;
            }
        }
    }
}
