package com.dena.hadoopLogMon.mrhist;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Calendar;

public class MrHist {

    public static void main(String args[]) throws Exception{
        Conf conf = makeConfig(args);
        AnalyzeFile af = new AnalyzeFile(conf);
        Result result = af.exec();

        HashMap<String, Integer>  taskCounts = new HashMap<>();

        Result newResult = result.filter(conf);
        if(conf.getJson()){
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(newResult);
            System.out.println(json);
        }else {
            newResult.output();
        }
        newResult.count(taskCounts);

        AnalyzeSparkFile asf = new AnalyzeSparkFile(conf);
        List<ResultSpark> lrs = asf.exec();
        Iterator it = lrs.iterator();
        while(it.hasNext()){
            ResultSpark rs = (ResultSpark)it.next();
            ResultSpark newrs = rs.filter(conf);
            if(newrs == null) continue;
            if(conf.getJson()) {
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(newrs);
                System.out.println(json);
            }else {
                newrs.output();
            }
        }
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH");
        lrs.stream().forEach(v -> {
                Date jobTime = v.getStartTime();
                calendar.setTime(jobTime);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                Date hour = calendar.getTime();
                String key = sdFormat.format(hour);
                if(taskCounts.containsKey(key)){
                    taskCounts.put(key, taskCounts.get(key) + 1);
                }else{
                    taskCounts.put(key, 1);
                }
            });

        if(conf.getCount()){
            countOutput(taskCounts);
        }


    }

    public static void countOutput(HashMap<String, Integer> map){
        System.out.println("");
        map.keySet().stream().forEach(v -> {
            Integer count = map.get(v);
            System.out.println("time: " + v + "  jobs: " + count);
        });
    }

    public static Conf makeConfig(String args[]) throws ParseException, java.text.ParseException{
        Options options = new Options();
        options.addOption("s", "start", true, "start time (mandatory)");
        options.addOption("e", "end", true, "end time (mandatory)");
        options.addOption("p", "path", true, "HDFS path (default:/mr-history/done)");
        options.addOption("m", "mapnum", true, "output jobs larger than mapnum(default:0)");
        options.addOption("r", "reducenum", true, "output jobs larger than reducenum(default:0)");
        options.addOption("d", "direct", false, "this option means path option specifies hdfs path directory");
        options.addOption("h", "hosts", true, "output hosts(default: output all hosts)");
        options.addOption("d", "detail", false, "output task details");
        options.addOption("c", "count", false, "output job counts for each hour");
        options.addOption("j", "json", false, "output as json format");
        options.addOption("u", "sparkurl", true, "spark url");
        CommandLineParser parser = new GnuParser();
        CommandLine cmdLine = parser.parse(options, args);

        Date start = null, end = null;
        String path = null;
        String sparkurl = null;
        List<String> hosts = new LinkedList<String>();
        int mapnum = 0, reducenum = 0;
        boolean direct = false, detail = false, count = false, json = false;
        SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        if (cmdLine.hasOption("start")) {
            String sstart = cmdLine.getOptionValue("start");
            start = sdFormat.parse(sstart);
        }else{
            System.err.println("start time is required");
            System.exit(1);
        }
        if (cmdLine.hasOption("end")) {
            String send = cmdLine.getOptionValue("end");
            end = sdFormat.parse(send);
        }else{
            System.err.println("end time is required");
            System.exit(1);
        }
        if (cmdLine.hasOption("path")) {
            path = cmdLine.getOptionValue("path");
        }else{
            path = "/mr-history/done";
        }

        if (cmdLine.hasOption("mapnum")) {
            mapnum = Integer.parseInt(cmdLine.getOptionValue("mapnum"));
        }

        if (cmdLine.hasOption("reducenum")) {
            reducenum = Integer.parseInt(cmdLine.getOptionValue("reducenum"));
        }

        if (cmdLine.hasOption("direct")){
            direct = true;
        }

        if (cmdLine.hasOption("hosts")){
            String[] shosts = cmdLine.getOptionValue("hosts").split(":", 0);
            for(String s: shosts){
                hosts.add(s);
            }
        }

        if (cmdLine.hasOption("detail")){
            detail = true;
        }

        if(cmdLine.hasOption("count")){
            count = true;
        }

        if(cmdLine.hasOption("json")){
            json = true;
        }
        if (cmdLine.hasOption("sparkurl")) {
            sparkurl = cmdLine.getOptionValue("sparkurl");
        }else{
            System.err.println("sparkurl is required");
            System.exit(1);
        }
        

        return new Conf.Builder(start, end, path, sparkurl)
                .mapnum(mapnum)
                .reducenum(reducenum)
                .direct(direct)
                .hosts(hosts)
                .detail(detail)
                .count(count)
                .json(json)
                .build();

    }
}
