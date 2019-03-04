package com.dena.hadoopLogMon.mrhist;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Date;
import java.util.HashMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AnalyzeSparkFile {

    private Conf conf;
    private Client client = Client.create();
    private Gson parser = new Gson();
    private Log log = LogFactory.getLog("root");

    public AnalyzeSparkFile(Conf conf){
        this.conf = conf;
    }

    public List<ResultSpark> exec() throws Exception{
        List<ResultSpark> list = new LinkedList<ResultSpark>();

        String sparkUrl = conf.getSparkUrls();
        WebResource job = client.resource(sparkUrl + "api/v1/applications");
        String json = job.get(String.class);
        JsonArray d = parser.fromJson(json, JsonArray.class);
        List<ResultSpark> l = analyze(sparkUrl, d);
        for(ResultSpark r: l){
            list.add(r);
        }

        return list;
    }

    private List<ResultSpark> analyze(String sparkUrl, JsonArray d) throws Exception{
        List<ResultSpark> result = new LinkedList<ResultSpark>();
        for(int i = 0; i < d.size(); i++){
            JsonObject obj = d.get(i).getAsJsonObject();
            String id = obj.get("id").getAsString();
            String name = obj.get("name").getAsString();
            JsonArray attempts = obj.get("attempts").getAsJsonArray();
            JsonObject attempt = attempts.get(0).getAsJsonObject();
            Date start = null, end = null;
            if(attempt.get("startTimeEpoch") == null){
                SimpleDateFormat format = new SimpleDateFormat();
                format.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");

                String sstart = attempt.get("startTime").getAsString();
                start = format.parse(sstart);

                String send = attempt.get("endTime").getAsString();
                end = format.parse(send);
            }else {
                log.info("id:" + id + "    name:" + name + "     start:" + attempt.get("startTimeEpoch").getAsString() + "    end:" + attempt.get("endTimeEpoch").getAsString());
                start = new Date(attempt.get("startTimeEpoch").getAsLong());
                end = new Date(attempt.get("endTimeEpoch").getAsLong());
            }
            if(conf.getStartTime().after(start) || conf.getEndTime().before(start)) continue;

            ResultSpark rs = new ResultSpark(id, name, start, end);
            WebResource exec = client.resource(sparkUrl + "api/v1/applications/" + id + "/executors");
            String json = exec.get(String.class);
            List<ResultSparkExecutor> lr = analyzeExecutor(parser.fromJson(json, JsonArray.class));
            rs.addExecutors(lr);
            result.add(rs);
        }

        return result;
    }

    private List<ResultSparkExecutor> analyzeExecutor(JsonArray d) throws Exception{
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        for(int i = 0; i < d.size(); i++){
            JsonObject obj = d.get(i).getAsJsonObject();

            if(obj.get("id").getAsString().equals("driver")) continue;

            String hostport = obj.get("hostPort").getAsString();
            String hostname = hostport.split(":")[0];
            if(map.containsKey(hostname)){
                map.put(hostname, map.get(hostname) + 1);
            }else{
                map.put(hostname, 1);
            }
        }

        List<ResultSparkExecutor> lr = new LinkedList<ResultSparkExecutor>();
        Iterator it = map.keySet().iterator();
        while(it.hasNext()){
            String hostname = (String)it.next();
            int count = map.get(hostname);
            ResultSparkExecutor rse = new ResultSparkExecutor();
            rse.addHostname(hostname);
            rse.addExecNum(count);
            lr.add(rse);
        }

        return lr;
    }
}
