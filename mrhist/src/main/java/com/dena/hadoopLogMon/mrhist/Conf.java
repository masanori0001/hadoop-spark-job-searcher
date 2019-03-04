package com.dena.hadoopLogMon.mrhist;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;

public class Conf {

    private Date startTime;
    private Date endTime;
    private String path;
    private int mapnum;
    private int reducenum;
    private boolean direct;
    private List<String> hosts;
    private boolean detail;
    private String sparkUrl;
    private boolean count;
    private boolean json;

    public static class Builder {
        private Date startTime;
        private Date endTime;
        private String path;
        private int mapnum;
        private int reducenum;
        private boolean direct;
        private List<String> hosts;
        private boolean detail;
        private String sparkUrl;
        private boolean count;
        private boolean json;

        Builder(Date startTime, Date endTime, String path, String sparkUrl){
            this.startTime = startTime;
            this.endTime = endTime;
            this.path = path;
            this.sparkUrl = sparkUrl;
        }

        Builder mapnum(int mapnum){
            this.mapnum = mapnum;
            return this;
        }

        Builder reducenum(int reducenum){
            this.reducenum = reducenum;
            return this;
        }

        Builder direct(boolean direct){
            this.direct = direct;
            return this;
        }

        Builder hosts(List<String> hosts){
            this.hosts = hosts;
            return this;
        }

        Builder detail(boolean detail){
            this.detail = detail;
            return this;
        }

        Builder count(boolean count){
            this.count = count;
            return this;
        }

        Builder json(boolean json){
            this.json = json;
            return this;
        }

        Conf build(){
            return new Conf(this);
        }


    }

    private Conf(Builder builder){
        startTime = builder.startTime;
        endTime = builder.endTime;
        path = builder.path;
        mapnum = builder.mapnum;
        reducenum = builder.reducenum;
        direct = builder.direct;
        hosts = builder.hosts;
        detail = builder.detail;
        count = builder.count;
        json = builder.json;
        sparkUrl = builder.sparkUrl;
    }

    public Date getStartTime(){
        return startTime;
    }

    public Date getEndTime(){
        return endTime;
    }

    public String getPath(){
        return path;
    }

    public int getMapNum(){
        return mapnum;
    }

    public int getReduceNum(){
        return reducenum;
    }

    public boolean getDirect(){
        return direct;
    }
 
    public List<String> getHosts(){
        return hosts;
    }

    public void setPath(String path){
        this.path = path;
    }

    public boolean getDetail(){ return detail; }

    public String getSparkUrls(){ return sparkUrl; }

    public boolean getCount(){return count;}

    public boolean getJson(){ return json; }

}
