package org.speedwagon.depot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import redis.clients.jedis.*;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;


public class Depot{

    private final String address;
    private final int port;
    private ForkLiftPool forkLiftPool;
    public Depot(String address, int port){
        this.address = address;
        this.port = port;
    }
    public Depot(String address){
        this.address = address;
        this.port = 6379;
    }

    public void instantiateWorkerPool(){
        this.forkLiftPool = new ForkLiftPool(this.address,this.port);
    }

    public void destroyWorkerPool(){
        this.forkLiftPool.close();
    }

    public static void main(String[] args) throws IOException {
        Depot depot = new Depot("127.0.0.1");
        depot.instantiateWorkerPool();
//        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        ForkLift forklift = depot.forkLiftPool.getForkLift();

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/producer", new ProducerServer(forklift));
        server.createContext("/consumer", new ConsumerServer(forklift));
        server.start();

        // Put later in DepotServer
        // depot.destroyWorkerPool();
    }

}

class ConsumerServer implements HttpHandler {
    private ForkLift forkLift;

    public ConsumerServer(ForkLift fl) {
        forkLift = fl;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        InputStream is = httpExchange.getRequestBody();
        String stream = new String(is.readAllBytes());
        is.close();

        Map<String, StreamEntryID> streamMap = new HashMap<String, StreamEntryID>();
        streamMap.put(stream, new StreamEntryID());
        StreamEntry se = forkLift.unload(streamMap);

        String s = se.toString();
        OutputStream os = httpExchange.getResponseBody();
        httpExchange.sendResponseHeaders(200, s.length());
        os.write(s.getBytes());
        os.close();
    }
}

class ProducerServer implements HttpHandler {
    private ForkLift forkLift;

    public ProducerServer(ForkLift fl) {
        forkLift = fl;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        InputStream is = httpExchange.getRequestBody();
        String request = new String(is.readAllBytes());
        String[] temp = request.split(";");
        String stream = temp[0], data = temp[1];
        is.close();

        StreamEntryID id = forkLift.load(stream.getBytes(), data.getBytes());

        String s = "Done\r\n";
        OutputStream os = httpExchange.getResponseBody();
        httpExchange.sendResponseHeaders(200, s.length());
        os.write(s.getBytes());
        os.close();
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


