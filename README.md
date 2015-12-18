# Computational Geometry on GeoSpark

### Dataset
Under the data folder, you can find sample data file, points.csv of size 98 MB and number of points 4920533

### Jars
Two external jars are required to run GeoSpark code. We have added them to the jars/ folder.

### Prerequisites
1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7

### Compilation Steps
1. git clone https://github.com/jayapriya90/GeoSpark.git
2. cd GeoSpark
3. mvn compile
4. mvn package

This will create a GeoSpark-0.1.jar file in the target folder.

### Steps to deploy
1. Create your own Apache Spark project
2. Add GeoSpark jar and other jars from the jars/ into your Apache Spark build environment
3. You can now use GeoSpark spatial RDDs in your Apache Spark program to store spatial data and call needed functions!

### Running Computational Geometry on GeoSpark
#### Launch Spark command line using
1. Launching locally: ./spark-shell --jars <path to GeoSpark-0.1.jar>,<path to guava-18.0.jar>,<path to jts-1.13.jar>
2. Launching on a cluster: ./spark-shell --jars <path to GeoSpark-0.1.jar>,<path to guava-18.0.jar>,<path to jts-1.13.jar> --master <name of the master>

#### import spatialRDD
import org.datasyslab.geospark.spatialRDD._

####declare points RDD
val points = new PointRDD(sc, "<Path to points.csv>", 0, ",", number of partitions)

#### Closest Pair
points.closestPair()

#### Skyline
points.skyline()

#### Convex Hull
points.convexhull()

