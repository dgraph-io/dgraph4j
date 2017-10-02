package io.dgraph.client;

import java.time.ZonedDateTime;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import io.dgraph.entity.DgraphRequest;
import io.dgraph.entity.Edge;
import io.dgraph.entity.Node;

public class DgraphMain {

    public static void main(final String[] args) {
        final DgraphClient dgraphClient = GrpcDgraphClient.newInstance("127.0.0.1", 9080);

        Gson gson = new Gson();
        
        DgraphRequest req = new DgraphRequest();
        Map<String,Object> obj = Maps.newHashMap()   ;    
        
        obj.put("_uid_", 6007);
        
        obj.put("key1",4.2);
        obj.put("key2",3.1);
        
        req.deleteObject(obj);
        
        DgraphResult resp = dgraphClient.query(req);

        System.out.println("Response  "+gson.toJson(resp));
        
        final DgraphResult result2 = dgraphClient.query("{me(func:uid(6007)) { uid,key1,key2}}");

        // final DgraphResult result = dgraphClient.query("{me(uid: 0xa}");
       System.out.println("uh" + result2.toJsonObject());
    }
}