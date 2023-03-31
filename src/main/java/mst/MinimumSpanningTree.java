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

//TODO: Ensure that the mstNeighbor is removed from the neighborsAvailableForTesting set.
//TODO: Merge component adds the new node to mstNeighbors.

public class MinimumSpanningTree {

    public ConfigParser config;
    public BlockingQueue<MSTMessage> messageQueue;
    Integer currentComponentId;
    private Map<Integer, ObjectOutputStream> objectOutputStreams;
    Set<Integer> neighborUIDs;
    Set<Integer> neighborsAvailableForTesting;
    public int uid;

    public MinimumSpanningTree(ConfigParser configParser) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.currentRound = 1;
        this.currentComponentId = configParser.getUID();
        this.uid = configParser.getUID();
        this.config = configParser;
        this.objectOutputStreams = new HashMap<>();
        Set<Integer> tempSet = Collections.unmodifiableSet(config.getNeighbourNodeHostDetails().keySet());
        this.neighborUIDs = new HashSet<>(tempSet);
        this.neighborsAvailableForTesting = new HashSet<>(tempSet);
        System.out.println("UID: " + uid);
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
    boolean newRoundStarted = true;
    Integer numberOfTestingRepliesReceived = 0;
    Map<Integer, List<MSTMessage>> nextRoundMessageBuffers = new HashMap<>();
    Set<Integer> incomingNeighborsInCurrentRound = new HashSet<>();
    
    Integer mstTestParentMessageUID = null;
    Set<Integer> neighborsRequiringTestingReplies = new HashSet<>();
    Set<Integer> neighborsAcknowledgingTestingMessages = new HashSet<>();
    Set<Integer> mstNeighborsRequestingConvergcastToLeader = new HashSet<>();
    Set<Integer> acknowledgeMergeToComponent = new HashSet<>();
    Set<Integer> sameComponent = new HashSet<>();
    List<Integer> outgoingNeighborsMinWeight = null;

    Integer mergeMessageRecipient = null; //Node that receives our merge component message if we send any.
    Set<Integer> mergeMessageSenders = new HashSet<>(); //Node that has sent us a merge component message. Note that multiple nodes can send us this message.

    Boolean expectingBroadcastMerge = false;
    Boolean broadcastNewLeaderToNeighbors = false;
    Integer parentForBroadcastNewLeader = null;

    Boolean broadcastMergeRequest = false;
    Integer parentForBroadcastMerge = null;

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
            
            System.out.println("Processing message from " + message.uid + " " + message.round + " " + message.messageType);
            boolean isMessageBuffered = checkMessageRound(message);
            if (isMessageBuffered) {
                continue;
            }

            handleMessage(message, neighborsAvailableForTesting);

            //Handling round end
            if (incomingNeighborsInCurrentRound.size() == neighborUIDs.size()) {
                currentRound++;
                System.out.println("Starting round "+ currentRound);

                newRoundStarted = true;
                incomingNeighborsInCurrentRound.clear();

            }
        }
    }

    public void handleRoundBeginning() {
        Set<Integer> nodesCommunicatedInThisRound = new HashSet<>();

        if (!broadcastedTestForCurrentPhase && (config.getUID()==currentComponentId || mstTestParentMessageUID != null)) {
            System.out.println("Sending mst children to broadcast testing message and tesing the edge weights of others");
            Set<Integer> outgoingNeighbors = handleBroadcastTesting();
            broadcastedTestForCurrentPhase = true;
            nodesCommunicatedInThisRound.addAll(outgoingNeighbors);
        } else if (neighborsRequiringTestingReplies.size() > 0) {
            MSTMessage replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.TEST_MESSAGE_REPLY);
            sendMessageToSomeNeighbors(replyMessage, neighborsRequiringTestingReplies);
            nodesCommunicatedInThisRound.addAll(neighborsRequiringTestingReplies);
            neighborsRequiringTestingReplies.clear();
        }

//        if(sameComponent.size() > 0){
//            MSTMessage replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.SAME_COMPONENT);
//            sendMessageToSomeNeighbors(replyMessage, sameComponent);
//            nodesCommunicatedInThisRound.addAll(sameComponent);
//            sameComponent.clear();
//        }
        
        // If the leader node has no children, i.e. the MST is empty.
        if (config.getUID() == currentComponentId && mstNeighbors.size() == 0 && neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            
            Integer newNode = handleMergeMessage();
            nodesCommunicatedInThisRound.add(newNode);

            neighborsAcknowledgingTestingMessages.clear();
        } //The leader is part of a MST and all the children and his other neighbors have acknowledged the testing message.
        else if (config.getUID() == currentComponentId && mstNeighbors.size() > 0 && mstNeighborsRequestingConvergcastToLeader.equals(mstNeighbors) && neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            System.out.println("The minimum weight edge inside is" + outgoingNeighborsMinWeight);
            if (outgoingNeighborsMinWeight == null){
                // TODO: TERMINATION
            }
            else if (neighborsAcknowledgingTestingMessages.contains(outgoingNeighborsMinWeight.get(1)) || neighborsAcknowledgingTestingMessages.contains(outgoingNeighborsMinWeight.get(2))) {
                Integer newNode = handleMergeMessage();
                nodesCommunicatedInThisRound.add(newNode);
            } else {
                MSTMessage broadcastMergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_MERGE, outgoingNeighborsMinWeight);
                sendMessageToSomeNeighbors(broadcastMergeMessage, mstNeighbors);
                nodesCommunicatedInThisRound.addAll(mstNeighbors);
            }

            mstNeighborsRequestingConvergcastToLeader.clear();
            neighborsAcknowledgingTestingMessages.clear();
        } // If I am a leaf node who has received a reply from all my outgoing edges or a intermediate node in the MST who has received a reply, I perform the convergecast.
        else if (((mstNeighbors.size() > 0 && mstNeighborsRequestingConvergcastToLeader.equals(mstNeighbors)) || mstNeighbors.size() == 0)  && 
        neighborsAcknowledgingTestingMessages.equals(neighborsAvailableForTesting)) {
            MSTMessage convergecastMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.CONVERGECAST_MIN_TO_LEADER, outgoingNeighborsMinWeight);
            sendMessageToSomeNeighbors(convergecastMessage, new HashSet<Integer>(Arrays.asList(mstTestParentMessageUID)));

            if(mstTestParentMessageUID != null && mstTestParentMessageUID != uid) {
                nodesCommunicatedInThisRound.add(mstTestParentMessageUID);
                mstNeighbors.add(mstTestParentMessageUID);
            }
            expectingBroadcastMerge = true;
            mstTestParentMessageUID = null;
            mstNeighborsRequestingConvergcastToLeader.clear();
            neighborsAcknowledgingTestingMessages.clear();
        }

        else if (broadcastMergeRequest) {
            if (mstNeighbors.size() > 0) {
                MSTMessage broadcastMergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_MERGE, outgoingNeighborsMinWeight);
                sendMessageToSomeNeighbors(broadcastMergeMessage, mstNeighbors);
                nodesCommunicatedInThisRound.addAll(mstNeighbors);
            }

            Boolean neighborContainsMWOE = (neighborsAvailableForTesting.contains(outgoingNeighborsMinWeight.get(1)) || neighborsAvailableForTesting.contains(outgoingNeighborsMinWeight.get(2)));
            if (neighborContainsMWOE) {
                Integer newNode = handleMergeMessage();
                nodesCommunicatedInThisRound.add(newNode);
            }

            // We want to add neighbors who contacted us before the broadcast merge to our component only after it is completed.
            if (mergeMessageSenders.size() > 0) {
                mstNeighbors.addAll(mergeMessageSenders);
                mstNeighbors.remove(uid);
                neighborsAvailableForTesting.removeAll(mergeMessageSenders);
                expectingBroadcastMerge = false;
            }

            if(parentForBroadcastMerge != uid)
                mstNeighbors.add(parentForBroadcastMerge);

            parentForBroadcastMerge = null;
            broadcastMergeRequest = false;
        } else if (mergeMessageRecipient!=null && mergeMessageSenders.size()>0 && mergeMessageSenders.contains(mergeMessageRecipient)) {
            //If my uid is the maximum, I start the broadcast.
            if (config.getUID() == Math.max(mergeMessageRecipient, config.getUID())) {
                System.out.println("I am the leader of this new component");
                currentComponentId = config.getUID();
                MSTMessage broadcastLeaderMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_NEW_LEADER);
                sendMessageToSomeNeighbors(broadcastLeaderMessage, mstNeighbors);
                nodesCommunicatedInThisRound.addAll(mstNeighbors);
            }

            mergeMessageRecipient = null;
            mergeMessageSenders.clear();
        }

        else if (broadcastNewLeaderToNeighbors) {
            MSTMessage broadcastLeaderMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.BROADCAST_NEW_LEADER);
            sendMessageToSomeNeighbors(broadcastLeaderMessage, mstNeighbors);
            nodesCommunicatedInThisRound.addAll(mstNeighbors);
            if(parentForBroadcastNewLeader != uid)
                mstNeighbors.add(parentForBroadcastNewLeader);
            parentForBroadcastNewLeader = null;
            broadcastNewLeaderToNeighbors = false;   
        }

        if ((currentRound % (6 * config.getTotalNodes())) == 0) {
            System.out.println("The current phase has ended. Beginning phase " + currentPhase);
            System.out.println("The MST neighbors for this phase are " + mstNeighbors);
            System.out.println("My component ID is " + currentComponentId);
            broadcastedTestForCurrentPhase = false;
            outgoingNeighborsMinWeight = null;
            currentPhase += 1;
            System.out.println("Neighbors Available for Testing "+ neighborsAvailableForTesting);
        }

         //Temp code to test how the MST algorithm works. (Currently Runs forever)
         if (currentPhase == 6) {
             System.exit(0);
         }

        Set<Integer> nodesNotCommunicatedInThisRound = new HashSet<>(neighborUIDs);
        nodesNotCommunicatedInThisRound.removeAll(nodesCommunicatedInThisRound);


        if (nodesNotCommunicatedInThisRound.size() > 0) {
            MSTMessage updateMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.UPDATE_ROUND);
            sendMessageToSomeNeighbors(updateMessage, nodesNotCommunicatedInThisRound);
        }
    }

    public Integer handleMergeMessage() {
        MSTMessage mergeMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.MERGE_COMPONENT);

        Integer nodeToSendMergeMessage = neighborsAvailableForTesting.contains(outgoingNeighborsMinWeight.get(1)) 
                        ? outgoingNeighborsMinWeight.get(1) : outgoingNeighborsMinWeight.get(2);
        if(nodeToSendMergeMessage != uid){
            System.out.println("Sending merge component message to " + nodeToSendMergeMessage);
            mergeMessageRecipient = nodeToSendMergeMessage;
            sendMessageToSomeNeighbors(mergeMessage, new HashSet<Integer>(Arrays.asList(nodeToSendMergeMessage)));
            if(nodeToSendMergeMessage != uid)
                mstNeighbors.add(nodeToSendMergeMessage);
        }
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
            sendMessageToSomeNeighbors(replyMessage, mstNeighbors);
            outgoingNeighborsInCurrentRound.addAll(new HashSet<>(mstNeighbors));
        }

        if (neighborsAvailableForTesting.size() > 0) {
            replyMessage = new MSTMessage(currentRound, config.getUID(), currentComponentId, MSTMessageType.TESTING_NEIGHBORS);
            sendMessageToSomeNeighbors(replyMessage, neighborsAvailableForTesting);
            outgoingNeighborsInCurrentRound.addAll(new HashSet<>(neighborsAvailableForTesting));
        }

        return outgoingNeighborsInCurrentRound;
    }

    public void handleMessage(MSTMessage message, Set<Integer> neighborsAvailableForTesting) {
        List<Integer> messageWeight;
        incomingNeighborsInCurrentRound.add(message.uid);

        switch (message.messageType) {
            // First, we need to send the broadcast testing message to the children in the MST.
            // Then, we need to test the rest of our incident edges in case we have not rejected them earlier.
            case BROADCAST_TESTING:
                mstTestParentMessageUID = message.uid;
                mstNeighbors.remove(mstTestParentMessageUID);
                break;
            case TEST_MESSAGE_REPLY:
                neighborsAcknowledgingTestingMessages.add(message.uid);
                
                messageWeight = Arrays.asList
                    (config.getNeighbourNodeEdgeWeights().get(message.uid), Math.min(config.getUID(), message.uid), Math.max(config.getUID(), message.uid));
                outgoingNeighborsMinWeight = outgoingNeighborsMinWeight==null ? 
                    messageWeight : MSTUtils.compareWeights(messageWeight, outgoingNeighborsMinWeight);
                break;
            case BROADCAST_NEW_LEADER:
                currentComponentId = message.componentId;
                parentForBroadcastNewLeader = message.uid;
                mstNeighbors.remove(parentForBroadcastNewLeader);
                broadcastNewLeaderToNeighbors = true;
                break;
            case BROADCAST_MERGE:
                parentForBroadcastMerge = message.uid;
                outgoingNeighborsMinWeight = message.minWeight;
                mstNeighbors.remove(parentForBroadcastMerge);

                broadcastMergeRequest = true;
                break;
            case CONVERGECAST_MIN_TO_LEADER:
                mstNeighborsRequestingConvergcastToLeader.add(message.uid);
                
                messageWeight = message.minWeight;
                outgoingNeighborsMinWeight = outgoingNeighborsMinWeight==null ? 
                    messageWeight : MSTUtils.compareWeights(messageWeight, outgoingNeighborsMinWeight);
                break;
            case MERGE_COMPONENT:
                if (!expectingBroadcastMerge) {
                    if(message.uid != uid)
                        mstNeighbors.add(message.uid);
                    neighborsAvailableForTesting.remove(message.uid);
                }
                mergeMessageSenders.add(message.uid);
                break;
            case SAME_COMPONENT:

                System.out.println("~~~SAME COMPONENT~~~~");
                System.out.println(mstNeighbors);
                System.out.println(neighborsRequiringTestingReplies);
                System.out.println(neighborsAcknowledgingTestingMessages);
                neighborsRequiringTestingReplies.remove(message.uid);
                neighborsAcknowledgingTestingMessages.remove(message.uid);
                System.out.println("~~~AFTER~~~~");
                System.out.println(mstNeighbors);
                System.out.println(neighborsRequiringTestingReplies);
                System.out.println(neighborsAcknowledgingTestingMessages);
                break;
            case TESTING_NEIGHBORS:
                //if(message.componentId == currentComponentId){
                //   sameComponent.add(message.uid);
                //}
                //else{
                    neighborsRequiringTestingReplies.add(message.uid);
                //}
                break;
            case UPDATE_ROUND:
                break;
            case TERMINATE:
                System.out.println("Node Terminated, MST Finished: " + mstNeighbors);
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
                if(neighborUID != null){
                    objectOutputStream = objectOutputStreams.get(neighborUID);
                    objectOutputStream.writeObject(m);
                    objectOutputStream.flush();
                }
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
