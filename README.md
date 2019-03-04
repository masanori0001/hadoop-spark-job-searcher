# hadoop-spark-job-searcher

MapReduce/sparkのジョブの過去の実行状況を表示するコマンドです

## ビルド方法

cd mrhist/bin  
bash deploy.sh  

## 実行方法  
hadoopユーザで実行する必要があります

cd mrhist/bin  
bash mrhist.sh [オプション]

### オプション
- -s, --start "YYYY/MM/DD HH:mm"  この時間以降に開始されたジョブを表示する  
- -e, --end "YYYY/MM/DD HH:mm"    この時間より前に開始されたジョブを表示する  
- -p, --path "HDFS path"          MapReduceのHistory Logが格納されているパスを指定します(デフォルト /mr-history/done)
- -m, --mapnum  数値                 指定した数以上のmap taskを持つジョブのみを表示します
- -r, --reducenum 数値               指定した数以上のreduce taskを持つジョブのみを表示します
- -h, --hosts "hostnames"         指定したホストでmap, reduceを実行したジョブのみ表示します。また各ジョブのMapHosts, ReduceHostsには指定したホストだけ表示します。ホスト名は:区切りで複数指定することが可能です
- -u, --sparkurl "Spark URL"      Spark History ServerのURLを指定します。

## 出力形式
```
JobName : PigLatin:test.pig
JobID : job_1507188234442_1838082
Start Time : Sun Nov 05 05:08:41 JST 2017
End TIme   : Sun Nov 05 05:10:41 JST 2017
  MapHosts:  tasks  25
     hostname : gh2074   task num : 10
     hostname : gh2283   task num : 2
     hostname : gh2098   task num : 10
     hostname : gh2142   task num : 2
     hostname : gh2171   task num : 1
  ReduceHosts:  tasks  16
     hostname : gh2176   task num : 15
     hostname : gh2329   task num : 1
```
- JobName      ジョブ名
- JobID        ジョブID
- Start Time   ジョブの起動時間
- End Time     ジョブの終了時間
- MapHosts     mapの情報。mapの全タスク数とホストごとのタスク数
- ReduceHosts  reduceの情報。reduceの全タスク数とホストごとのタスク数
