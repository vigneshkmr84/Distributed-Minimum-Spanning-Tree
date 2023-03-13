import javafx.util.Pair;

import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {

        int nodeUID = Integer.parseInt(args[0]);
        String configFile = args[1];

        ConfigParser configParser = new ConfigParser(configFile);

        configParser.parseConfig(nodeUID);

        System.out.println("Neighbouring node connection");
        Map<Integer, String> hostDetailsMap = configParser.getNeighbourNodeHostDetails();
        for(Map.Entry<Integer, String> m: hostDetailsMap.entrySet()){
            System.out.println("uid : " + m.getKey() + ", connection : " + m.getValue());
        }

        System.out.println("Neighbouring node Edge Weights");
        for(Map.Entry<Integer, Integer> m: configParser.getNeighbourNodeEdgeWeights().entrySet()){
            System.out.println("Neigh Node : " + m.getKey() + ", Weight : " + m.getValue());
        }

        System.out.println("Node Port : " + configParser.getNodePort());


    }
}