package com.dena.hadoopLogMon.mrhist;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

public class ResultSpark {

    public String appid;
    public String appname;
    public Date start;
    public Date end;
    public long duration;
    public List<ResultSparkExecutor> rse;

    public ResultSpark(String appid, String appname, Date start, Date end){
        this.appid = appid;
        this.appname = appname;
        this.start = start;
        this.end = end;
        this.duration = end.getTime() - start.getTime();
    }

    public String getAppid(){
        return appid;
    }

    public String getAppname(){
        return appname;
    }

    public Date getStartTime(){
        return start;
    }

    public Date getEndTime(){
        return end;
    }

    public void addExecutors(List<ResultSparkExecutor> r){
        rse = r;
    }

    public ResultSpark filter(Conf conf){
        List<ResultSparkExecutor> newlist = new LinkedList<ResultSparkExecutor>();

        Iterator it = rse.iterator();
        while(it.hasNext()){
            ResultSparkExecutor rse = (ResultSparkExecutor)it.next();
            ResultSparkExecutor newrse = rse.filter(conf);
            if(newrse != null){
                newlist.add(newrse);
            }
        }
        if(newlist.size() == 0 ) return null;
        rse = newlist;

        return this;
    }

    public void output(){
        System.out.println("appid: " + appid);
        System.out.println("appname: " + appname);
        System.out.println("Start Time: " + start);
        System.out.println("End Time: " + end);
        System.out.println("Duration: " + duration);
        Iterator it = rse.iterator();
        while(it.hasNext()){
            ResultSparkExecutor rse = (ResultSparkExecutor)it.next();
            rse.output();
        }
        System.out.println("");
    }



}
