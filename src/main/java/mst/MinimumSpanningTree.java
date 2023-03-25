package mst;

import utils.ConfigParser;
import utils.MSTUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MinimumSpanningTree {

    public ConfigParser config;
    public BlockingQueue<MSTMessage> messageQueue;
    Integer currentComponentId;
    private Map<Integer, ObjectOutputStream> objectOutputStreams;
    Set<Integer> neighborUIDs;

    public MinimumSpanningTree(ConfigParser configParser) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.currentRound = 1;
        this.currentComponentId = configParser.getUID();
        this.config = configParser;
        this.objectOutputStreams = new HashMap<>();
        this.neighborUIDs = Collections.unmodifiableSet(config.getNeighbourNodeHostDetails().keySet());
    }

    public void start() {
        setupClientAndServerSockets();
        runMST();
    }

    public void setupClientAndServerSockets() {
        //We need a server to lisen to incoming messages.
        final SocketListener socketListener = new SocketListener(messageQueue, config.getNodePort());
        final Thread socketListenerThread = new Thread(socketListener);
        socketListenerThread.start();

        System.out.println("Initiating connection to neighbouring nodes...");
        //We need to create clients for each neighbor to send messages to each of them.
        for (Map.Entry<Integer, String> neighborDetails : config.getNeighbourNodeHostDetails().entrySet()) {
            Socket socketToNeighbor = null;

            String connString = neighborDetails.getValue();
            // The neighbors server might not have started yet, so we perform a Retry storm to create the connection.
            while (true) {
                try {
                    socketToNeighbor = new Socket(connString.split(":")[0], Integer.parseInt(connString.split(":")[1]));
                    break;
                } catch (Exception e) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            // from the config.txt file, get the neighbors, and it's corresponding output streams
            try {
                final ObjectOutputStream neighborOutputStream = new ObjectOutputStream(socketToNeighbor.getOutputStream());
                objectOutputStreams.put(neighborDetails.getKey(), neighborOutputStream);
                System.out.println("Added node " + neighborDetails.getKey() + " to socket connection.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    Integer currentRound = 1;
    Integer currentPhase = 1;

    Integer mstParent = null;
    Set<Integer> mstChildren = new HashSet<>();

    boolean broadcastTestForCurrentPhase = false;
    Set<Integer> neighborsAvailableForTesting = config.getNeighbourNodeHostDetails().keySet();
    Integer numberOfTestingRepliesReceived = 0;
    Map<Integer, List<MSTMessage>> nextRoundMessageBuffers = new HashMap<>();
    Set<Integer> outgoingNeighborsInCurrentRound = new HashSet<>();
    Set<Integer> incomingNeighborsInCurrentRound = new HashSet<>();
    
    Set<Integer> neighborsRequiringTestingReplies = new HashSet<>();
    List<Integer> outgoingNeighborsMaxWeight = null;

    public void runMST() {
        // if uid == component_id, first initiate a broadcast message to all neighbours (at the start, all nodes will do this)

        System.out.println("Initiating MST Algorithm.");

        while (true) {
            if (config.getUID() == currentComponentId & !broadcastTestForCurrentPhase) {
                MSTMessage mstMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_TESTING);

                if (mstChildren.size() > 0) {
                    sendMessageToSomeNeighbors(mstMessage, mstChildren);
                } else {
                    // This "sends" a message to ourself, so that we can avoid writing separate conditions to handle phase 0.
                    messageQueue.offer(mstMessage);
                }

                broadcastTestForCurrentPhase = true;
            }

            MSTMessage message = messageQueue.poll();

            boolean isMessageBuffered = checkMessageRound(message);
            if (isMessageBuffered) {
                continue;
            }

            handleMessage(message, neighborsAvailableForTesting);


            //Handling Round
            if (incomingNeighborsInCurrentRound.size() == neighborUIDs.size()) {
                currentRound++;

                if (neighborsRequiringTestingReplies.size() > 0) {
                    MSTMessage replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.TEST_MESSAGE_REPLY);
                    sendMessageToSomeNeighbors(replyMessage, neighborUIDs);
                }
            }
        }
    }

    public void handleMessage(MSTMessage message, Set<Integer> neighborsAvailableForTesting) {
        MSTMessage replyMessage;
        
        switch (message.messageType) {
            // First, we need to send the broadcast testing message to the children in the MST.
            // Then, we need to test the rest of our incident edges in case we have not rejected them earlier.
            case BROADCAST_TESTING:
                if (mstChildren.size() > 0) {
                    replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_TESTING);
                    sendMessageToSomeNeighbors(replyMessage, neighborsAvailableForTesting);
                    outgoingNeighborsInCurrentRound.addAll(mstChildren);
                }

                if (neighborsAvailableForTesting.size() > 0) {
                    replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.TESTING_NEIGHBORS);
                    sendMessageToSomeNeighbors(replyMessage, neighborsAvailableForTesting);
                    outgoingNeighborsInCurrentRound.addAll(neighborsAvailableForTesting);
                } 

                Set<Integer> remainingNodes = new HashSet<>(neighborUIDs);
                remainingNodes.removeAll(outgoingNeighborsInCurrentRound);

                if (remainingNodes.size() > 0) {
                    replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.UPDATE_ROUND);
                    sendMessageToSomeNeighbors(replyMessage, remainingNodes);
                }

                outgoingNeighborsInCurrentRound.addAll(remainingNodes);
                break;
            case TEST_MESSAGE_REPLY:
                incomingNeighborsInCurrentRound.add(message.uid);
                
                List<Integer> messageWeight = Arrays.asList
                    (config.getNeighbourNodeEdgeWeights().get(message.uid), config.getUID(), message.uid);
                outgoingNeighborsMaxWeight = outgoingNeighborsMaxWeight==null ? 
                    messageWeight : MSTUtils.compareWeights(messageWeight, outgoingNeighborsMaxWeight);
                break;
            case BROADCAST_COMPLETION:
                break;
            case BROADCAST_NEW_LEADER:
                break;
            case CONVERGECAST_LEADER:
                break;
            case MERGE_COMPONENT:
                break;
            case TESTING_NEIGHBORS:
                incomingNeighborsInCurrentRound.add(message.uid);
                neighborsRequiringTestingReplies.add(message.uid);
                break;
            case UPDATE_ROUND:
                incomingNeighborsInCurrentRound.add(message.uid);
                break;
            default:
                break;
        }
    }


    // Check the message round and return if the message is buffered or not.
    public boolean checkMessageRound(MSTMessage message) {
        if (message.round < currentRound) {
            throw new Error("A message from previous round is received. Please check the algorithm!");
        } else if (message.round > currentRound) {
            if (message.messageType == MSTMessageType.UPDATE_ROUND) {
                currentRound++;
                return true;
            }

            List<MSTMessage> buffer;

            if (nextRoundMessageBuffers.containsKey(message.round)) {
                buffer = nextRoundMessageBuffers.get(message.round);
            } else {
                buffer = new ArrayList<>();
            }

            buffer.add(message);
            nextRoundMessageBuffers.put(message.round, buffer);

            return true;
        } else {
            return false;
        }
    }

    // broadcast message to all neighbours
    public void broadCastMessageToAllNeighbors(MSTMessage m){
        try {
            for (ObjectOutputStream outputStream: objectOutputStreams.values()) {
                outputStream.writeObject(m);
                outputStream.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //send message to a specific neighbors
    public void sendMessageToSomeNeighbors(MSTMessage m, Set<Integer> neighborUIDSet) {
        ObjectOutputStream objectOutputStream;

        try {
            for (Integer neighborUID: neighborUIDSet) {
                objectOutputStream = objectOutputStreams.get(neighborUID);
                objectOutputStream.writeObject(m);
                objectOutputStream.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class SocketListener implements Runnable {
    BlockingQueue<MSTMessage> queue;

    Integer port;

    SocketListener(BlockingQueue<MSTMessage> queue, Integer port) {
        this.queue = queue;
        this.port = port;
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Socket socket = serverSocket.accept();
                ClientHandler clientHandler = new ClientHandler(queue, socket);
                Thread clientHandlerThread = new Thread(clientHandler);
                clientHandlerThread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

class ClientHandler implements Runnable {
    BlockingQueue<MSTMessage> queue;
    Socket socket;
    Boolean bfsMessageSeen;

    ClientHandler(BlockingQueue<MSTMessage> queue, Socket socket) {
        this.queue = queue;
        this.socket = socket;
        this.bfsMessageSeen = false;
    }

    @Override
    public void run() {
        ObjectInputStream inputStream = null;

        try {
            inputStream = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Keep accepting message and add it to the Blocking Queue
        while (true) {
            try {
                Object inputObject = inputStream.readObject();

                if (!bfsMessageSeen) {
                    try {
                        MSTMessage message = (MSTMessage) inputObject;
                        queue.offer(message);
                    } catch (ClassCastException e) {
                        bfsMessageSeen = true;
                    }
                } else {
                    MSTMessage message = (MSTMessage) inputObject;
                    queue.offer(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
