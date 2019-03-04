package com.dena.hadoopLogMon.mrhist;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AnalyzeFile {


    private Conf conf;
    private Result result = new Result();
    private Gson parser = new Gson();
    FileSystem fs;
    private Log log = LogFactory.getLog("root");
    private Pattern p = Pattern.compile("job_[0-9]+_[0-9]+-([0-9]+)-");

    public AnalyzeFile(Conf conf) throws Exception{
        this.conf = conf;
        Configuration hconf = new Configuration();
        hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hconf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        hconf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        fs = FileSystem.get(hconf);
    }

    public Result exec() throws Exception{
        List<String> directories = getDirectories();
        List<String> files = getFileNames(directories);

        return analyze(files);
    }

    private List<String> getDirectories(){
        String path = null;
        if(conf.getDirect()){
           path = conf.getPath();
        }else{
           String syear = new SimpleDateFormat("yyyy").format(conf.getStartTime());
           String smonth = new SimpleDateFormat("MM").format(conf.getStartTime());
           String sday = new SimpleDateFormat("dd").format(conf.getStartTime());

           path = conf.getPath() + "/" + syear + "/" + smonth + "/" + sday;
        }
        log.info("search path:" + path);
        List<String> directories = new LinkedList<String>();
        directories.add(path);

        return directories;
    }

    private List<String> getFileNames(List<String> directories) throws Exception{
        List<String> files = new LinkedList<String>();
        try {
            Iterator it = directories.iterator();
            while (it.hasNext()) {
                String directory = (String) it.next();
                String glob = directory + "/*/*.jhist";
                Path path = new Path(glob);
                FileStatus[] status = fs.globStatus(path);
                Path[] listedPaths = FileUtil.stat2Paths(status);
                for (Path p : listedPaths) {
                    log.info("file:" + p.toString());
                    if(isWithinTime(p.toString())){
                        files.add(p.toString());
                    }
                }

            }
        }catch(Exception e){
            System.err.println("can not access hadoop");
            e.printStackTrace();

            throw e;
        }
        return files;
    }

    private boolean isWithinTime(String path){
        Matcher m = p.matcher(path);
        if(m.find() == false) return false;
        String stime = m.group(1);
        Date time = new Date(Long.parseLong(stime));
        if( time.compareTo(conf.getStartTime()) == -1 || time.compareTo(conf.getEndTime()) == 1) return false;
        return true;
    }    

    private Result analyze(List<String> paths) throws Exception{ 
        Iterator it = paths.iterator();
        while(it.hasNext()){
            String path = (String)it.next();
            BufferedReader br = null;
            try{
                FSDataInputStream fss = fs.open(new Path(path));
                br = new BufferedReader(new InputStreamReader(fss));
                ResultRow row = analyzeRow(br);
                if(row != null) result.addResult(row);
            }catch(Exception e){
                e.printStackTrace();
                throw e;
            }finally{
               if(br != null)  br.close();
            }
        }

        return result;
    }

    private ResultRow analyzeRow(BufferedReader br) throws Exception{

        String line = null;
        ResultRow rr = new ResultRow();
        HashMap<String, ResultHost> mmap = new HashMap<String, ResultHost>();
        HashMap<String, ResultHost> rmap = new HashMap<String, ResultHost>();
        while((line = br.readLine()) != null) {
            if (line.equals("")) continue;
            try {
                JsonObject d = parser.fromJson(line, JsonObject.class);
                String type = d.get("type").getAsString();
                log.info("type = " + type);
                switch (getJsonType(type)) {
                    case JOB_SUBMITTED:
                        ResultJob job = analyzeJob(d);
                        if(job == null){
                            log.info("job not matched");
                            return null;
                        }
                        rr.addJob(job);
                        break;
                    case MAP_ATTEMPT_STARTED:
                        Task mt = analyzeMapReduce(d);
                        ResultTaskInfo mrti = new ResultTaskInfo();
                        mrti.addStartTime(mt.time);
                        mrti.addTaskId(mt.taskId);
                        if(mmap.containsKey(mt.hostname)){
                            ResultHost host = mmap.get(mt.hostname);
                            host.addTask(mrti);
                        }else{
                            ResultHost host = new ResultHost(mt.hostname);
                            host.addTask(mrti);
                            mmap.put(mt.hostname, host);
                        }
                        break;

                    case REDUCE_ATTEMPT_STARTED:
                        Task rt = analyzeMapReduce(d);
                        ResultTaskInfo rrti = new ResultTaskInfo();
                        rrti.addStartTime(rt.time);
                        rrti.addTaskId(rt.taskId);
                        if(rmap.containsKey(rt.hostname)){
                            ResultHost host = rmap.get(rt.hostname);
                            host.addTask(rrti);
                        }else{
                            ResultHost host = new ResultHost(rt.hostname);
                            host.addTask(rrti);
                            rmap.put(rt.hostname, host);
                        }
                        break;

                    case MAP_ATTEMPT_FINISHED:
                        Task mtask = analyzeMapFnished(d);
                        ResultHost mhost = mmap.get(mtask.hostname);
                        mhost.addTaskFinished(mtask);
                        break;

                    case REDUCE_ATTEMPT_FINISHED:
                        Task rtask = analyzeReduceFnished(d);
                        ResultHost rhost = mmap.get(rtask.hostname);
                        rhost.addTaskFinished(rtask);
                        break;

                    case JOB_FINISHED:
                        Date date = analyzeJobFinished(d);
                        rr.getJob().addEndTime(date);
                        break;

                }
            }catch(Exception e){
                //nothing to do
                log.info("not json format: string = " + line);
            }
        }

        rr.addMHosts(mmap);
        rr.addRHosts(rmap);

        return rr;
    }

    private JsonType getJsonType(String type){
        for(JsonType val: JsonType.values()){
            if(type.equals(val.name())) return val;
        }
        return JsonType.OTHERS;
    }

    private Task analyzeMapReduce(JsonObject obj){
        JsonObject jobInfo = obj.get("event").getAsJsonObject().get("org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted").getAsJsonObject();
        String hostname = jobInfo.get("trackerName").getAsString();
        String taskId = jobInfo.get("taskid").getAsString();
        String stime = jobInfo.get("startTime").getAsString();
        Date time = new Date(Long.parseLong(stime));
        Task task = new Task(taskId, time, hostname);
        log.info("mapreduce hostname " + hostname);

        return task;
    }

    private Task analyzeMapFnished(JsonObject obj){
        JsonObject jobInfo = obj.get("event").getAsJsonObject().get("org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished").getAsJsonObject();
        String hostname = jobInfo.get("hostname").getAsString();
        String taskId = jobInfo.get("taskid").getAsString();
        String etime = jobInfo.get("finishTime").getAsString();
        Date time = new Date(Long.parseLong(etime));
        Task task = new Task(taskId, time, hostname);
        log.info("hostname = " + hostname + "  task id =  " + taskId + "  finish time = " + time);

        return task;
    }

    private Task analyzeReduceFnished(JsonObject obj){
        JsonObject jobInfo = obj.get("event").getAsJsonObject().get("org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinished").getAsJsonObject();
        String hostname = jobInfo.get("hostname").getAsString();
        String taskId = jobInfo.get("taskid").getAsString();
        String etime = jobInfo.get("finishTime").getAsString();
        Date time = new Date(Long.parseLong(etime));
        Task task = new Task(taskId, time, hostname);
        log.info("hostname = " + hostname + "  task id =  " + taskId + "  finish time = " + time);

        return task;
    }


    private ResultJob analyzeJob(JsonObject obj){
        JsonObject jobInfo = obj.get("event").getAsJsonObject().get("org.apache.hadoop.mapreduce.jobhistory.JobSubmitted").getAsJsonObject();
        String jobId = jobInfo.get("jobid").getAsString();
        String jobName = jobInfo.get("jobName").getAsString();
        String sst = jobInfo.get("submitTime").getAsString();
        Date st = new Date(Long.parseLong(sst));
	    if( st.compareTo(conf.getStartTime()) == -1 || st.compareTo(conf.getEndTime()) == 1){
            return null;
        }

        log.info("job info jobId = " + jobId + "  jobName = " + jobName + "  start time = " + sst);
        ResultJob rj = new ResultJob();
        rj.addJobName(jobName);
        rj.addJonId(jobId);
        rj.addStartTime(st);

        return rj;
    }

    private Date analyzeJobFinished(JsonObject obj){
        JsonObject jobInfo = obj.get("event").getAsJsonObject().get("org.apache.hadoop.mapreduce.jobhistory.JobFinished").getAsJsonObject();
        String sdate = jobInfo.get("finishTime").getAsString();
        Date st = new Date(Long.parseLong(sdate));

        return st;
    }
}
