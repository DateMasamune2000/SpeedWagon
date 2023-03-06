package org.speedwagon.depot;

import redis.clients.jedis.*;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// The depot store uses the Redis Client for java, Jedis.
/**
SpeedWagon broker's storage class. DepotStore is a simple wrapper
 that supports all the message queuing features of SpeedWagon
 */
public class DepotStore {

        private final String address;
        private final int port;

        private JedisPoolConfig workerPoolConfig;
        private JedisPool workerPool;


        public DepotStore(String address,int port){
            this.address = address;
            this.port = port;
            this.workerPoolConfig = new JedisPoolConfig();

        }
        public DepotStore(String address){
        this.address = address;
        this.port = 6379;
        this.workerPoolConfig = new JedisPoolConfig();

    }

    /**
     * Function to change default configuration
     * @param maxTotal max number of jedis instances in the pool
     * @param maxIdle max number of idle jedis instances in the pool
     */
        public void setConfig(int maxTotal,int maxIdle){
            this.workerPoolConfig = new JedisPoolConfig();
            workerPoolConfig.setMaxTotal(maxTotal);
            workerPoolConfig.setMaxIdle(maxIdle);

        }

        public void instantiateWorkerPool(){
            this.workerPool = new JedisPool(workerPoolConfig,this.address,this.port);
        }

        public void load(String key , String message){
            try(Jedis worker = this.workerPool.getResource()){
                Map<String,String> key_val = new HashMap<String,String>();
                key_val.put(key,message);
                System.out.println(worker.xadd("stream", (StreamEntryID) null,key_val));

            }
        }
    public void load(byte[] key , byte[] message){
        try(Jedis worker = this.workerPool.getResource()){
            Map<byte[],byte[]> key_val = new HashMap<byte[],byte[]>();
            key_val.put(key,message);
            XAddParams params = new XAddParams();
            params.maxLen(1000);
            params.noMkStream();
            System.out.println(worker.xadd("stream".getBytes(),key_val,params));

        }
    }

        public void destroyWorkerPool(){
            this.workerPool.close();
        }

    public StreamEntry unload(Map<String,StreamEntryID>streams){
        try(Jedis worker = this.workerPool.getResource()){
            XReadParams params = new XReadParams();
            List<Map.Entry<String, List<StreamEntry>>> data = worker.xread(params, streams);
            StreamEntry message = data.get(0).getValue().get(0);
            worker.xdel("stream",message.getID());
            return message;
        }
    }
    public List<StreamEntry> unloadN(Map<String,StreamEntryID>streams , int n){
        try(Jedis worker = this.workerPool.getResource()){
            XReadParams params = new XReadParams();
            List<Map.Entry<String, List<StreamEntry>>> data = worker.xread(params, streams);
            List<StreamEntry> messages  = new ArrayList<>();
            System.out.println(data);
            for(int i =0;i<n;i++) {
                try {
                    StreamEntry message = data.get(0).getValue().get(i);
                    messages.add(message);
                }
                catch (Exception ignored){
                }
            }
            for(StreamEntry message: messages){
                worker.xdel("stream",message.getID());
            }
            return messages;
        }
    }

    public static void main(String[] args) {
        DepotStore store = new DepotStore("localhost");
        store.instantiateWorkerPool();
//        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        byte[] data = "message".getBytes();
//        store.load("actual".getBytes(),data);


// create a Map object representing the name of the stream and its corresponding ID
        Map<String, StreamEntryID> streams = new HashMap<String, StreamEntryID>();
        streams.put("stream",new StreamEntryID());

// read one entry from the stream
        List<StreamEntry> message = store.unloadN(streams,5);


// print the entry
        System.out.println(message);

        store.destroyWorkerPool();

    }


}
