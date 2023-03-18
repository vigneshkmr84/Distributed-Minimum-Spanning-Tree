package mst;

import mst.MSTMessage;
import mst.MSTMessageType;
import utils.ConfigParser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MinimumSpanningTree {

    public ConfigParser config;
    public BlockingQueue<MSTMessage> messageQueue;
    public int currentRound;
    int currentComponentId;
    public Set<Integer> mstChildren;
    private Map<Integer, ObjectOutputStream> objectOutputStreams;

    public MinimumSpanningTree(ConfigParser configParser) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.currentRound = 1;
        this.currentComponentId = configParser.UID;
        this.config = configParser;
        this.objectOutputStreams = new HashMap<>();
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

    public void runMST() {
        // if uid == component_id, first initiate a broadcast message to all neighbours (at the start, all nodes will do this)

        System.out.println("Initiating MST Algorithm.");

        broadcastTestForCurrentPhase = false;

        while (true) {
            if (config.UID == currentComponentId & !broadcastTestForCurrentPhase) {
                MSTMessage mstMessage = new MSTMessage(currentRound, config.UID, currentComponentId, MSTMessageType.BROADCAST_TESTING);

                if (mstChildren.size() > 0) {
                    broadCastMessageToSomeNeighbors(mstMessage, mstChildren);
                } else {
                    messageQueue.offer(mstMessage);
                }

                broadcastTestForCurrentPhase = true;
            }

            MSTMessage message = messageQueue.poll();

            switch (message) {
                case BROADCAST_TESTING:
                    // Dummy thing will be changed
                    broadCastMessageToSomeNeighbors(message, mstChildren);
                    break;
            }

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

    public void broadCastMessageToSomeNeighbors(MSTMessage m, Set<Integer> neighborUIDSet) {
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
