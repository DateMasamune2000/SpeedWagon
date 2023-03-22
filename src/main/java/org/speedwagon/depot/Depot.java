package org.speedwagon.depot;

// Redis
import redis.clients.jedis.*;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

// Misc. utilities
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;

// HTTP stuff
import com.sun.net.httpserver.*;

public class Depot{

    private final String address;
    private final int redisPort;
    private ForkLiftPool forkLiftPool;
    public Depot(String address, int port){
        this.address = address;
        this.redisPort = port;
    }
    public Depot(String address){
        this.address = address;
        this.redisPort = 6379;
    }

    public void instantiateWorkerPool(){
        this.forkLiftPool = new ForkLiftPool(this.address,this.redisPort);
    }

    public void destroyWorkerPool(){
        this.forkLiftPool.close();
    }

    //Currently implementing HTTP interface
    public static void main(String[] args) throws Exception {
        Depot depot = new Depot("127.0.0.1");
        depot.instantiateWorkerPool();

//        byte[] data = "message".getBytes();
//        ForkLift forklift = depot.forkLiftPool.getForkLift();
//        StreamEntryID id = forklift.load("stream".getBytes(),"actual".getBytes());
//
//        String stream = "stream";
//        Map<String, StreamEntryID> streamMap = new HashMap<String, StreamEntryID>();
//        streamMap.put("stream",new StreamEntryID());
//
//        System.out.println(forklift.unload(streamMap));

        // TODO: Make server part multithreaded
        HttpServer server = HttpServer.create(new InetSocketAddress(6969), 0);
        server.createContext("/producer", new ProducerApiHandler());

        server.start();

        depot.destroyWorkerPool();
    }

    static class ProducerApiHandler implements HttpHandler {
        public static ForkLiftPool forkLiftPool;
        @Override
        public void handle(HttpExchange t) throws IOException {
            OutputStream outputStream  = t.getResponseBody();
            String response = "Speed Wagon v0.0 pre-alpha";
            t.sendResponseHeaders(200, response.length());
            outputStream.write(response.getBytes());
            outputStream.flush();
            outputStream.close();
        }
    }
}


/*
* Decorator for jedis instance
* provides ability to load/unload from redis stream
 */
class ForkLift implements AutoCloseable {

    private Jedis forkLift;
    private String keyString = "message";
    private byte[] keyBytes = keyString.getBytes();
    public ForkLift(Jedis jedis){
        this.forkLift = jedis;
    }
    public void load(String key, String message) {
        Map<String, String> key_val = new HashMap<String, String>();
        key_val.put(key, message);
        System.out.println(this.forkLift.xadd("stream", (StreamEntryID) null, key_val));

    }

    public StreamEntry unload(Map<String, StreamEntryID> streams) {
        String stream = streams.keySet().iterator().next();
        XReadParams params = new XReadParams();
        List<Map.Entry<String, List<StreamEntry>>> data = this.forkLift.xread(params, streams);
        System.out.println(data);
        StreamEntry message = data.get(0).getValue().get(0);
        this.forkLift.xdel(stream, message.getID());
        System.out.println("Message unloaded: " + message);
        return message;
    }

    public List<StreamEntry> unloadN(Map<String, StreamEntryID> streams, int n) {
        String stream = streams.keySet().iterator().next();
        XReadParams params = new XReadParams();
        List<Map.Entry<String, List<StreamEntry>>> data = this.forkLift.xread(params, streams);
        List<StreamEntry> messages = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            try {
                StreamEntry message = data.get(0).getValue().get(i);
                messages.add(message);
            } catch (Exception ignored) {
            }
        }
        for (StreamEntry message : messages) {
            this.forkLift.xdel(stream, message.getID());
        }
        System.out.println("Messages unloaded:" + messages);
        return messages;

    }
    public StreamEntryID load(byte[] stream,byte[] message){

        Map<byte[],byte[]> data= new HashMap<>();
        data.put(this.keyBytes,message);
        XAddParams params = new XAddParams();

        // add message and create StreamEntryId from returned id byte[]
        return new StreamEntryID(this.forkLift.xadd(stream,data,params));
    }
    public byte[] unload(byte[] stream){
        Map<String,StreamEntryID> entry = new HashMap<>();
        entry.put(new String(stream),new StreamEntryID(0));
        XReadParams params = new XReadParams();
        params.count(1);

        List<Map.Entry<String, List<StreamEntry>>> data = this.forkLift.xread(params,entry);
        String message = data.get(0).getValue().get(0).getFields().get(this.keyString);
        return message.getBytes();
    }
    @Override
    public void close(){
        this.forkLift.close();
    }
}


/*
* Thread-Safe pool of ForkLifts
 */
class ForkLiftPool extends JedisPool{

    public ForkLiftPool(String host,int port){
        super(host,port);

    }
    public ForkLift getForkLift(){
        try(ForkLift forkLift = new ForkLift(this.getResource())){
            return forkLift;
        }
    }
}


