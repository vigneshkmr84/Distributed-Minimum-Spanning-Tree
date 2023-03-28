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
    Set<Integer> neighborsAvailableForTesting;

    public MinimumSpanningTree(ConfigParser configParser) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.currentRound = 1;
        this.currentComponentId = configParser.getUID();
        this.config = configParser;
        this.objectOutputStreams = new HashMap<>();
        this.neighborUIDs = Collections.unmodifiableSet(config.getNeighbourNodeHostDetails().keySet());
        this.neighborsAvailableForTesting = config.getNeighbourNodeHostDetails().keySet();
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

    Set<Integer> mstNeighbors = new HashSet<>();

    boolean broadcastedTestForCurrentPhase = false;
    boolean newRoundStarted = false;
    Integer numberOfTestingRepliesReceived = 0;
    Map<Integer, List<MSTMessage>> nextRoundMessageBuffers = new HashMap<>();
    Set<Integer> incomingNeighborsInCurrentRound = new HashSet<>();
    
    Integer mstTestParentMessageUID = null;
    Set<Integer> neighborsRequiringTestingReplies = new HashSet<>();
    Set<Integer> neighborsAcknowledgingTestingMessages = new HashSet<>();
    Set<Integer> mstNeighborsRequestingConvergcastToLeader = new HashSet<>();
    Set<Integer> acknowledgeMergeToComponent = new HashSet<>();
    List<Integer> outgoingNeighborsMaxWeight = null;

    Integer countRoundsToEndPhase = 0;
    Integer countRoundsToBroadcastNewLeader = 0;
    Boolean startCounterToBroadcastNewLeader = false;
    Boolean startCounterToEndPhase = false;

    Boolean broadcastNewLeaderToNeighbors = false;
    Integer parentNeighborForBroadcastNewLeader = null;

    Boolean broadcastMergeToAllNeighbors = false;
    Integer parentNeighborForBroadcastMerge = null;

    public void runMST() {
        // if uid == component_id, first initiate a broadcast message to all neighbours (at the start, all nodes will do this)

        System.out.println("Initiating MST Algorithm.");

        while (true) {

            if (newRoundStarted) {
                handleRoundBeginning();
                newRoundStarted = false;
            }

            MSTMessage message;
            if (nextRoundMessageBuffers.containsKey(currentRound) && nextRoundMessageBuffers.get(currentRound).size() > 0) {
                message = nextRoundMessageBuffers.get(currentRound).remove(0);
            } else {
                message = messageQueue.poll();
            }

            if (message == null) {
                continue;
            }
  
            boolean isMessageBuffered = checkMessageRound(message);
            if (isMessageBuffered) {
                continue;
            }

            handleMessage(message, neighborsAvailableForTesting);

            //Handling round end
            if (incomingNeighborsInCurrentRound.size() == neighborUIDs.size()) {
                currentRound++;

                newRoundStarted = true;
                incomingNeighborsInCurrentRound.clear();

                countRoundsToBroadcastNewLeader += startCounterToBroadcastNewLeader ? 1 : 0;
                countRoundsToEndPhase += startCounterToEndPhase ? 1 : 0;
            }
        }
    }

    public void handleRoundBeginning() {
        Set<Integer> testingMessagesToLeader = null;
        Set<Integer> nodesNotCommunicatedInThisRound = new HashSet<>(neighborUIDs);

        if (neighborsAcknowledgingTestingMessages.size() > 0) {
            testingMessagesToLeader = new HashSet<>(neighborsAcknowledgingTestingMessages);
            testingMessagesToLeader.addAll(mstNeighborsRequestingConvergcastToLeader);
        }

        if (!broadcastedTestForCurrentPhase && (config.getUID()==currentComponentId || mstTestParentMessageUID != null)) {
            System.out.println("Sending mst children to broadcast testing message and tesing the edge weights of others");
            Set<Integer> outgoingNeighbors = handleBroadcastTesting();
            broadcastedTestForCurrentPhase = true;
            nodesNotCommunicatedInThisRound.removeAll(outgoingNeighbors);
        }
        
        if (neighborsRequiringTestingReplies.size() > 0) {
            MSTMessage replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.TEST_MESSAGE_REPLY);
            sendMessageToSomeNeighbors(replyMessage, neighborsRequiringTestingReplies);
            nodesNotCommunicatedInThisRound.removeAll(neighborsRequiringTestingReplies);
            neighborsRequiringTestingReplies.clear();
        } 
        
        
        // If the leader node has no children, i.e. the MST is empty.
        if (config.getUID() == currentComponentId && mstNeighbors.size() == 0 && neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            currentComponentId = Math.max(currentComponentId, outgoingNeighborsMaxWeight.get(2));
            
            Integer newNode = handleMergeMessage();
            nodesNotCommunicatedInThisRound.remove(newNode);

            startCounterToBroadcastNewLeader = true;
            neighborsAcknowledgingTestingMessages.clear();
        } //The leader is part of a MST and all the children and his other neighbors have acknowledged the testing message.
        else if (config.getUID() == currentComponentId && mstNeighbors.size() > 0 && mstNeighborsRequestingConvergcastToLeader.equals(mstNeighbors) && neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            currentComponentId = Math.max(currentComponentId, outgoingNeighborsMaxWeight.get(2));

            if (neighborsAcknowledgingTestingMessages.contains(outgoingNeighborsMaxWeight.get(1)) || neighborsAcknowledgingTestingMessages.contains(outgoingNeighborsMaxWeight.get(2))) {
                Integer newNode = handleMergeMessage();
                nodesNotCommunicatedInThisRound.remove(newNode);
            } else {
                MSTMessage broadcastMergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_MERGE);
                sendMessageToSomeNeighbors(broadcastMergeMessage, mstNeighbors);
                nodesNotCommunicatedInThisRound.removeAll(mstNeighbors);
            }

            startCounterToBroadcastNewLeader = true;
            mstNeighborsRequestingConvergcastToLeader.clear();
            neighborsAcknowledgingTestingMessages.clear();
        } // If I am a leaf node who has received a reply from all my outgoing edges or a intermediate node in the MST who has received a reply, I perform the convergecast.
        else if (((mstNeighbors.size() > 0 && mstNeighborsRequestingConvergcastToLeader.equals(mstNeighbors)) || mstNeighbors.size() == 0)  && 
        neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            MSTMessage convergecastMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.CONVERGECAST_MAX_TO_LEADER, outgoingNeighborsMaxWeight);
            sendMessageToSomeNeighbors(convergecastMessage, Set.of(mstTestParentMessageUID));

            nodesNotCommunicatedInThisRound.remove(mstTestParentMessageUID);

            mstTestParentMessageUID = null;
            mstNeighborsRequestingConvergcastToLeader.clear();
            neighborsAcknowledgingTestingMessages.clear();
        }

        if (broadcastMergeToAllNeighbors) {
            Set<Integer> neighborsToSendMessage = new HashSet<>(mstNeighbors);
            neighborsToSendMessage.remove(parentNeighborForBroadcastMerge);

            // This means that I am a leaf node and I should send merge message to outgoing neighbor of the message.
            if (neighborsToSendMessage.size() == 0) {
                handleMergeMessage();
            } else {
                MSTMessage broadcastMergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_MERGE);
                sendMessageToSomeNeighbors(broadcastMergeMessage, neighborsToSendMessage);
                nodesNotCommunicatedInThisRound.removeAll(neighborsToSendMessage);
            }

            startCounterToBroadcastNewLeader = true;
            broadcastMergeToAllNeighbors = false;
        }

        if (broadcastNewLeaderToNeighbors) {
            Set<Integer> neighborsToSendMessage = new HashSet<>(mstNeighbors);
            neighborsToSendMessage.remove(parentNeighborForBroadcastNewLeader);

            if (neighborsToSendMessage.size() != 0) {
                MSTMessage broadcastLeaderMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_NEW_LEADER);
                sendMessageToSomeNeighbors(broadcastLeaderMessage, neighborsToSendMessage);
                nodesNotCommunicatedInThisRound.removeAll(neighborsToSendMessage);
            } 

            startCounterToEndPhase = true;
            broadcastNewLeaderToNeighbors = false;
        }

        if (countRoundsToBroadcastNewLeader == (config.getTotalNodes() + 1) && config.getUID() == currentComponentId) {
            MSTMessage broadcastMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_NEW_LEADER);
            sendMessageToSomeNeighbors(broadcastMessage, mstNeighbors);
            nodesNotCommunicatedInThisRound.removeAll(mstNeighbors);

            startCounterToEndPhase = true;
            countRoundsToBroadcastNewLeader = 0;
            startCounterToBroadcastNewLeader = false;
        }

        if (countRoundsToEndPhase == (config.getTotalNodes() + 1)) {
            startCounterToEndPhase = false;
            broadcastedTestForCurrentPhase = false;
            System.out.print("The Phase has ended, we are moving to the new phase.");
            currentPhase += 1;
        }

        //Temp code to test how the MST algorithm works.
        if (currentPhase == 3) {
            System.exit(0);
        }


        if (nodesNotCommunicatedInThisRound.size() > 0) {
            MSTMessage updateMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.UPDATE_ROUND);
            sendMessageToSomeNeighbors(updateMessage, nodesNotCommunicatedInThisRound);
        }
    }

    public Integer handleMergeMessage() {
        MSTMessage mergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.MERGE_COMPONENT);

        Integer nodeToSendMergeMessage = config.getUID() != outgoingNeighborsMaxWeight.get(1) ? outgoingNeighborsMaxWeight.get(1) : outgoingNeighborsMaxWeight.get(2);
        sendMessageToSomeNeighbors(mergeMessage, Set.of(nodeToSendMergeMessage));

        mstNeighbors.add(nodeToSendMergeMessage);
        neighborsAvailableForTesting.remove(nodeToSendMergeMessage);

        return nodeToSendMergeMessage;
    }

    /** This function handles the message sending portion for the broadcast testing round.
     * This is the round where the leader node or any other node informs its neighbors that they need to test of MWOE.
     */
    public Set<Integer> handleBroadcastTesting() {
        Set<Integer> outgoingNeighborsInCurrentRound = new HashSet<>();
        MSTMessage replyMessage;
        if (mstNeighbors.size() > 0) {
            replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_TESTING);
            sendMessageToSomeNeighbors(replyMessage, neighborsAvailableForTesting);
            outgoingNeighborsInCurrentRound.addAll(mstNeighbors);
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

        return outgoingNeighborsInCurrentRound;
    }

    public void handleMessage(MSTMessage message, Set<Integer> neighborsAvailableForTesting) {
        System.out.println("Processing message from " + message.uid + " " + message.round + " " + message.messageType);
        List<Integer> messageWeight;
        incomingNeighborsInCurrentRound.add(message.uid);

        switch (message.messageType) {
            // First, we need to send the broadcast testing message to the children in the MST.
            // Then, we need to test the rest of our incident edges in case we have not rejected them earlier.
            case BROADCAST_TESTING:
                mstTestParentMessageUID = message.uid;
                neighborsAvailableForTesting.remove(message.uid);
                break;
            case TEST_MESSAGE_REPLY:
                neighborsAcknowledgingTestingMessages.add(message.uid);
                
                messageWeight = Arrays.asList
                    (config.getNeighbourNodeEdgeWeights().get(message.uid), Math.min(config.getUID(), message.uid), Math.max(config.getUID(), message.uid));
                outgoingNeighborsMaxWeight = outgoingNeighborsMaxWeight==null ? 
                    messageWeight : MSTUtils.compareWeights(messageWeight, outgoingNeighborsMaxWeight);
                break;
            case BROADCAST_NEW_LEADER:
                currentComponentId = Math.max(currentComponentId, message.componentId);
                broadcastNewLeaderToNeighbors = true;
                parentNeighborForBroadcastNewLeader = message.uid;
                break;
            case BROADCAST_MERGE:
                currentComponentId = Math.max(currentComponentId, message.componentId);
                outgoingNeighborsMaxWeight = message.maxWeight;
                broadcastMergeToAllNeighbors = true;
                parentNeighborForBroadcastMerge = message.uid;
                break;
            case CONVERGECAST_MAX_TO_LEADER:
                mstNeighborsRequestingConvergcastToLeader.add(message.uid);
                
                messageWeight = message.maxWeight;
                outgoingNeighborsMaxWeight = outgoingNeighborsMaxWeight==null ? 
                    messageWeight : MSTUtils.compareWeights(messageWeight, outgoingNeighborsMaxWeight);
                break;
            case MERGE_COMPONENT:
                currentComponentId = Math.max(currentComponentId, message.componentId);
                startCounterToBroadcastNewLeader = true;
                break;
            case TESTING_NEIGHBORS:
                neighborsRequiringTestingReplies.add(message.uid);
                break;
            case UPDATE_ROUND:
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

    ClientHandler(BlockingQueue<MSTMessage> queue, Socket socket) {
        this.queue = queue;
        this.socket = socket;
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
                MSTMessage message = (MSTMessage) inputObject;
                queue.offer(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
