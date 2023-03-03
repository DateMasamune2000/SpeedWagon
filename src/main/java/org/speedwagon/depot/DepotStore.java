package org.speedwagon.depot;

import redis.clients.jedis.Jedis;
// The depot store uses the Redis Client for java, Jedis.
/**
SpeedWagon broker's storage class. DepotStore is a simple wrapper
 that supports all the message queuing features of SpeedWagon
 */
public class DepotStore {

        private final String address;
        private final int port;

        private final Jedis client;
        public DepotStore(String address,int port){
            this.address = address;
            this.port = port;
            this.client = new Jedis(this.address,this.port);

        }

        public void connect(){
            client.connect();
        }

        public void close(){
            client.close();
        }


}
