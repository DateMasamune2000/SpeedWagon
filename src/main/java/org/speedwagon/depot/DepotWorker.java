package org.speedwagon.depot;



import redis.clients.jedis.*;

/**
 * Support class for DepotStore.Each DepotWorker manages a redis stream
 */
public class DepotWorker {


    public Jedis worker;

    public DepotWorker(Jedis worker) {

        this.worker = worker;
    }
}
