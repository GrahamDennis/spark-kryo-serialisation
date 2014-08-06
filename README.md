# Spark Kryo Serialisation bug

Using Spark with Kryo serialisation and a custom Kryo registrator (to register custom classes with Kryo) can cause problems in Spark.  These problems appear as a failure during Kryo deserialisation and I have variously seen the following failures:

    java.lang.ClassCastException: scala.Tuple1 cannot be cast to scala.Product2
    java.lang.ClassNotFoundException
    java.lang.IndexOutOfBoundsException

I've verified this problem in Spark 1.0.0 and Spark 1.0.2.  My current hypothesis is described below.

To build this repository and demonstrate the problem, run:

    # Build the .jar file
    ./gradlew build -i
    # copy the built jar file in build/libs/spark-kryo-serialisation-0.0.1.jar to your spark master, and submit:
    spark-submit --class org.example.SparkDriver --master spark://YOUR-SPARK-MASTER:7077 spark-kryo-serialisation-0.0.1.jar
    # Note that this mustn't be run locally; You need to have at least two nodes to force Kryo serialisation / deserialisation.
    # I have tested this with a 4-worker Spark 1.0.0 cluster.

The problem appears in the master node logs as:

    14/08/06 17:42:32 WARN scheduler.TaskSetManager: Loss was due to java.lang.ClassCastException
    java.lang.ClassCastException: scala.Tuple1 cannot be cast to scala.Product2
    	at org.apache.spark.scheduler.ShuffleMapTask$$anonfun$runTask$1.apply(ShuffleMapTask.scala:159)
    	at org.apache.spark.scheduler.ShuffleMapTask$$anonfun$runTask$1.apply(ShuffleMapTask.scala:158)
    	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
    	at org.apache.spark.InterruptibleIterator.foreach(InterruptibleIterator.scala:28)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:158)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
    	at org.apache.spark.scheduler.Task.run(Task.scala:51)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:187)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
    	at java.lang.Thread.run(Thread.java:745)

Taking a look at the worker node stderr logs reveals the root cause (this may not appear on all worker nodes):

    14/08/06 17:42:31 ERROR serializer.KryoSerializer: Failed to run spark.kryo.registrator
    java.lang.ClassNotFoundException: org.example.MyRegistrator

The `org.example.MyRegistrator` class is included in the `spark-kryo-serialisation-0.0.1.jar` file, which *is* distributed to the workers, and *is* accessible by them.  This is demonstrated by other lines in the same log file of the form:

    ################# MyRegistrator called

This output is produced when the `org.example.MyRegistrator` class is called by Spark's Kryo serialisation code.

## Root cause hypothesis

1. Kryo doesn't serialise class names, but instead serialises ID numbers for classes.  These ID numbers are generated when classes are registered, and are assigned in order.  Thus for serialisation & deserialisation to work, the classes must be registered in the same order every time, on all nodes.
2. The first-order problem I was seeing was "ClassNotFoundException" or "ClassCastException" or "IndexOutOfBoundsException".  These are all because one or more nodes have different ID number —> class mappings, and thus Kryo tries to deserialise data using the wrong class.  This will cause different errors depending on exactly what data is in the buffer when the class ID numbers mismatch.
3. The cause of this problem is that classes aren't being registered in the same order.  To register custom classes with Kryo, Spark provides a hook via the config option "spark.kryo.registrator" with which you can register custom classes with Kryo.  Sometimes Spark fails to find my class, which appears as a "Failed to run spark.kryo.registrator" error message.  In this situation, Spark really should bail, but instead it catches the error and then continues to register classes.  Thus some nodes will have different Class ID —> class mappings.  Note that the "Failed to run spark.kryo.registrator" does not appear in the console output on master, or in the Web UI for the various stages, *only* in the worker stderr logs.
4. The cause of this problem is that some threads in the Executor process have different class loaders.  The threads that run Spark jobs are modified after they are launched to only have your application jar on the class path (and anything else added via SparkContext.addJar or SparkConfig.setJars).  This happens in `org.apache.spark.executor.Executor.TaskRunner.run`.  Other threads in the Executor process do not have your application jar available to them.  In particular, there are a number of threads associated with the `ConnectionManager` which send & receive data between spark nodes.  These threads may serialise and/or deserialise using Kryo, but they do not have the application jar available from their class loader, and thus looking up the "spark.kryo.registrator" will fail!  This causes the "Failed to run spark.kryo.registrator" message, and thus the kryo deserialisation errors later on.
