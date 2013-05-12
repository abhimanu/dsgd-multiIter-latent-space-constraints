DSGD
====

Implementation of DSGD

Hadoop 0.20.1

Include in your bashrc:

```bash
export PATH=$PATH:/hadoop/hadoop-current/bin
export PATH=$PATH:/hadoop/jdk-current/bin

alias hls='hadoop fs -ls '
alias hput='hadoop fs -put '
alias hmv='hadoop fs -mv '
alias hmkdir='hadoop fs -mkdir '
alias hcat='hadoop fs -cat '
```

Run with the following command:
```bash
hadoop jar DSGD.jar DSGD -D dsgd.N=[INT] -D dsgd.M=[INT] -D dsgd.rank=[INT] [data source] [output] [UV data]
```

For exmaple:
```bash
hadoop jar DSGD.jar DSGD -D dsgd.N=279936 -D dsgd.M=78125 -D dsgd.rank=5 -D dsgd.d=5 /user/abeutel/synthetic-data /user/abeutel/output /user/abeutel/UV
```

or
```bash
hadoop jar DSGD.jar DSGD -D dsgd.N=1392873 -D dsgd.M=4710 -D dsgd.rank=10 -D mapred.reduce.tasks=5 5 /user/abeutel/tencent /user/abeutel/tencent_output
```



```bash
hadoop jar Norm.jar FrobeniusNorm -D dsgd.N=279936 -D dsgd.M=78125 -D dsgd.rank=5 /user/abeutel/synthetic-data /user/abeutel/output_nothing  /user/abeutel/output_all/iter4
```


N:1392873
M:4710
