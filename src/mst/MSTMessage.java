package mst;

import java.io.Serializable;

public class MSTMessage implements Serializable {

    int round;

    int uid;

    // leader of that component can be used as component id
    int componentId;

    // search-reply, test, leader //
    // leader: is used when a new leader is elected to broadcast.
    // search: used by leader to check for neighbouring components.
    // reply: used as reply for search message.
    String messageType;

    public MSTMessage(int round, int uid, int componentId) {
        this.round = round;
        this.uid = uid;
        this.componentId = componentId;
    }
}
