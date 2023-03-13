package main.java.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ConfigParser {

    String fileName;

    List<String> fileLines;

    // key = uid, value = hostname:port
    Map<Integer, String> neighbourNodeHostDetails = new HashMap<>();

    // key = uid, value = edge weight
    Map<Integer, Integer> neighbourNodeEdgeWeights = new HashMap<>();

    int nodePort;

    public ConfigParser(String fileName) {
        this.fileName = fileName;
        this.fileLines = new ArrayList<>(Collections.emptyList());
    }

    public Map<Integer, Integer> getNeighbourNodeEdgeWeights() {
        return neighbourNodeEdgeWeights;
    }

    public Map<Integer, String> getNeighbourNodeHostDetails() {
        return neighbourNodeHostDetails;
    }

    public int getNodePort() {
        return nodePort;
    }

    public void parseConfig(int uid) throws Exception {

        if (!new File(fileName).exists()) {
            System.out.println("Config file doesn't exist.");
            System.exit(-1);
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                if ( !line.isEmpty() && !line.startsWith("#"))
                    fileLines.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int totalNodes = Integer.parseInt(fileLines.get(0));

        Map<Integer, String> nodeHostDetails = new HashMap<>();

        // get all node connection
        for (String s : fileLines.subList(1, 1 + totalNodes)) {
            String[] split = s.split(" ");
            nodeHostDetails.put(Integer.parseInt(split[0]), split[1] + ":" + split[2]);
        }

        // get all neighbouring nodes and edge-weights
        for (String s : fileLines.subList(1 + totalNodes, fileLines.size())) {
            String[] split = s.split(" ");
            int weight = Integer.parseInt(split[1]);
            String node = split[0];
            node = node.substring(1, node.length() - 1);
            String[] n = node.split(",");
            if (Integer.parseInt(n[0]) == uid) {
                neighbourNodeEdgeWeights.put(Integer.parseInt(n[1]), weight);
            } else if (Integer.parseInt(n[1]) == uid) {
                neighbourNodeEdgeWeights.put(Integer.parseInt(n[0]), weight);
            }
        }

        // extract only the neighbours
        for (int n : neighbourNodeEdgeWeights.keySet()) {
            neighbourNodeHostDetails.put(n, nodeHostDetails.get(n));
        }

        nodePort = Integer.parseInt(nodeHostDetails.get(uid).split(":")[1]);
    }

}
