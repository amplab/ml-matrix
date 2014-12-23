### Instructions on running ml-matrix on EC2

1. Launch a cluster using [Spark's EC2 scripts](http://spark.apache.org/docs/latest/ec2-scripts.html)
2. After the cluster launches, ssh to the master and clone the [matrix-bench](https://github.com/shivaram/matrix-bench)
repository.
3. Run `cd matrix-bench; bash setup-ec2.sh` and wait for OpenBLAS to build.
4. After OpenBLAS build completes, copy `/root/openblas-install` to all the machines with
`/root/spark-ec2/copy-dir /root/openblas-install`
5. Clone and build ml-matrix. `git clone https://github.com/amplab/ml-matrix.git; cd ml-matrix; sbt/sbt clean assembly`
   After the build finishes, copy ml-matrix to all the slaves with `/root/spark-ec2/copy-dir /root/ml-matrix`.
   NOTE: This is required for the extraClassPath setup we have below

6. Configure the BLAS using instructions below

### Native BLAS / LAPACK for netlib, using spark-submit

Since ATLAS is pre-installed in the Spark AMI, do the following so we can link to OpenBlas:
```
~/spark/sbin/slaves.sh rm /etc/ld.so.conf.d/atlas-x86_64.conf
~/spark/sbin/slaves.sh ldconfig
```

After each executor receives a built OpenBlas install, do these:
```
~/spark/sbin/slaves.sh ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/liblapack.so.3
~/spark/sbin/slaves.sh ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/libblas.so.3
```

Before running an application, `copy-dir` the application jar, and make sure the application jar is
prepended to `spark.executor.extraClassPath` in `conf/spark-defaults.conf`:

```
spark.executor.extraClassPath
/root/ml-matrix/target/scala-2.10/mlmatrix-assembly-0.1.jar:/root/ephemeral-hdfs/conf
```

Lastly, make sure `export OMP_NUM_THREADS=1` is in `spark-env.sh`.
