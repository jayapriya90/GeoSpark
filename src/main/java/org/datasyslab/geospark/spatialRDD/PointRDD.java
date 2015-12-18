/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.gemotryObjects.Rectangle;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PointXComparator;
import org.datasyslab.geospark.utils.PointYComparator;
import org.datasyslab.geospark.partition.PartitionAssignGridPoint;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Iterables;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.AbstractNode;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

;

// TODO: Auto-generated Javadoc
class PointFormatMapper implements Serializable, Function<String, Point> {
  Integer offset = 0;
  String splitter = "csv";

  public PointFormatMapper(Integer Offset, String Splitter) {
    this.offset = Offset;
    this.splitter = Splitter;
  }

  public Point call(String s) {
    String seperater = ",";
    if (this.splitter.contains("tsv")) {
      seperater = "\t";
    } else {
      seperater = ",";
    }
    GeometryFactory fact = new GeometryFactory();
    List<String> input = Arrays.asList(s.split(seperater));
    Coordinate coordinate = new Coordinate(Double.parseDouble(input.get(0 + this.offset)),
        Double.parseDouble(input.get(1 + this.offset)));
    Point point = fact.createPoint(coordinate);
    return point;
  }
}

/**
 * The Class PointRDD.
 */
public class PointRDD implements Serializable {


  private static final int DEFAULT_NUM_PARTITIONS = 100;
  private static final int DEFAULT_LEAF_SIZE = 100;
  private static final int MAX_BOUND_RECTANGLE = 1000000;
  private static final int DEFAULT_GRID_NUMBER_HORIZONTAL = 10;
  private static final int DEFAULT_GRID_NUMBER_VERTICAL = 10;

  /**
   * The point rdd.
   */
  private JavaRDD<Point> pointRDD;
  private int numSplits;

  /**
   * Instantiates a new point rdd.
   *
   * @param pointRDD the point rdd
   */
  public PointRDD(JavaRDD<Point> pointRDD) {
    this.setPointRDD(pointRDD.cache());
  }

  /**
   * Instantiates a new point rdd.
   *
   * @param spark         the spark
   * @param InputLocation the input location
   * @param Offset        the offset
   * @param Splitter      the splitter
   * @param partitions    the partitions
   */
  public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter,
      Integer partitions, boolean cache) {
    if (cache) {
      this.setPointRDD(
          spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter))
              .cache());
    } else {
      this.setPointRDD(
          spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter)));
    }
    this.numSplits = partitions;
  }

  public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter,
      Integer partitions) {
    this(spark, InputLocation, Offset, Splitter, partitions, true);
  }

  public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {
    this(spark, InputLocation, Offset, Splitter, true);
  }

  /**
   * Instantiates a new point rdd.
   *
   * @param spark         the spark
   * @param InputLocation the input location
   * @param Offset        the offset
   * @param Splitter      the splitter
   */
  public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter,
      boolean cache) {
    this(spark, InputLocation, Offset, Splitter, DEFAULT_NUM_PARTITIONS, cache);
  }

  public PointRDD(JavaSparkContext sc, List<Point> points) {
    this.pointRDD = sc.parallelize(points);
  }

  /**
   * Gets the point rdd.
   *
   * @return the point rdd
   */
  public JavaRDD<Point> getPointRDD() {
    return pointRDD;
  }

  /**
   * Sets the point rdd.
   *
   * @param pointRDD the new point rdd
   */
  public void setPointRDD(JavaRDD<Point> pointRDD) {
    this.pointRDD = pointRDD;
  }

  /**
   * Re partition.
   *
   * @param partitions the partitions
   * @return the java rdd
   */
  public JavaRDD<Point> rePartition(Integer partitions) {
    return this.pointRDD.repartition(partitions);
  }


  /**
   * Boundary.
   *
   * @return the envelope
   */
  public Envelope boundary() {
    Double[] boundary = new Double[4];

    Double minLongitude = this.pointRDD
        .min((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
    Double maxLongitude = this.pointRDD
        .max((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
    Double minLatitude = this.pointRDD
        .min((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
    Double maxLatitude = this.pointRDD
        .max((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
    boundary[0] = minLongitude;
    boundary[1] = minLatitude;
    boundary[2] = maxLongitude;
    boundary[3] = maxLatitude;
    return new Envelope(minLongitude, maxLongitude, minLatitude, maxLatitude);
  }

  /*************************************** SKYLINE *****************************************/

  public Point[] skylineLocal(boolean rtreeIndex) {
    return skylineLocal(rtreeIndex, 10);
  }

  public Point[] skylineLocal(boolean rtreeIndex, int leafSize) {
    if (rtreeIndex) {
      PointIndexRDD pointIndexRDD = new PointIndexRDD(pointRDD, leafSize);
      // convert JavaRDD<STRTree> to JavaRDD<Point>
      JavaRDD<Point> prunedPoints = pointIndexRDD.getPointIndexRDDrtree()
          .flatMap(new FlatMapFunction<STRtree, Point>() {
            @Override
            public Iterable<Point> call(final STRtree stRtree) throws Exception {
              return getPrunedPoints(stRtree);
            }
          });
      return skylineLocal(prunedPoints);
    } else {
      return skylineLocal(this.pointRDD);
    }
  }

  public Point[] skylineLocal(JavaRDD<Point> pRDD) {
    long start = System.currentTimeMillis();
    Point[] pointsArray = pRDD.toArray().toArray(new Point[]{});
    Arrays.sort(pointsArray, new PointXComparator());
    Point[] result = skyline(pointsArray, 0, pointsArray.length);
    long end = System.currentTimeMillis();
    System.out.println("Total time: " + (end - start) + "ms");
    return result;
  }

  public Point[] skyline() {
    return skyline(false);
  }

  public Point[] skyline(boolean useIndex) {
    return skyline(DEFAULT_NUM_PARTITIONS, DEFAULT_LEAF_SIZE, useIndex);
  }

  public Point[] skyline(int numPartitions, int rtreeLeafSize, boolean useIndex) {
    System.out.println("Initial number of partitions (input splits): " + numSplits);
    System.out.println("Requested number of partitions (for RDD): " + numPartitions);

    if (useIndex) {
      System.out.println("Leaf node size in RTree: " + rtreeLeafSize);
      PointIndexRDD pointIndexRDD = new PointIndexRDD(pointRDD, rtreeLeafSize);

      // convert JavaRDD<STRTree> to JavaRDD<Point>
      JavaRDD<Point> prunedPoints = pointIndexRDD.getPointIndexRDDrtree()
          .flatMap(new FlatMapFunction<STRtree, Point>() {
        @Override
        public Iterable<Point> call(final STRtree stRtree) throws Exception {
          return getPrunedPoints(stRtree);
        }
      });

      return skylineImpl(prunedPoints.repartition(numPartitions), numPartitions);
    } else {
      return skylineImpl(pointRDD.repartition(numPartitions), numPartitions);
    }
  }

  private Point[] skylineImpl(JavaRDD<Point> points, int numPartitions) {
    long start = System.currentTimeMillis();
    final int dividerValue = (int) ((MAX_BOUND_RECTANGLE - 0) / numPartitions);
    JavaPairRDD<Integer, Point> keyToPointsData =
        points.mapToPair(new PairFunction<Point, Integer, Point>() {

          private static final long serialVersionUID = -433072613673987883L;

          public Tuple2<Integer, Point> call(Point t) throws Exception {
            return new Tuple2<Integer, Point>((int) (t.getX() / dividerValue), t);
          }
        });
    System.out.println("Mapped points to key in: " + (System.currentTimeMillis() - start) + " ms");

    long start2 = System.currentTimeMillis();
    JavaPairRDD<Integer, Iterable<Point>> partitionedPointsRDD =
        keyToPointsData.groupByKey(numPartitions);
    System.out.println("Created partitions in: " + (System.currentTimeMillis() - start2) + " ms");

		/*
     * Calculate individual skylines
     */
    start2 = System.currentTimeMillis();
    partitionedPointsRDD =
        partitionedPointsRDD
            .mapValues(new Function<Iterable<Point>, Iterable<Point>>() {

              private static final long serialVersionUID = 4592384070663695223L;

              public Iterable<Point> call(Iterable<Point> v1) throws Exception {
                Point[] pointsArray = Iterables.toArray(v1, Point.class);
                // calculate skyline.
                Arrays.sort(pointsArray, new PointXComparator());
                Point[] skyline = skyline(pointsArray, 0, pointsArray.length);
                return Arrays.asList(skyline);
              }
            });
    // partitionedPointsRDD = partitionedPointsRDD.cache();
    System.out.println("Calculated skylines for individual partitions in: " + (System.currentTimeMillis() - start2) + " ms");

    /*
     * Merge all the skylines
     */
    start2 = System.currentTimeMillis();
    partitionedPointsRDD = partitionedPointsRDD.sortByKey(true);
    List<Tuple2<Integer, Iterable<Point>>> skylineTuples =
        partitionedPointsRDD.collect();
    Point[] skyline = Iterables.toArray(skylineTuples.get(0)._2(), Point.class);
    List<Point> result = new ArrayList<Point>();
    result.addAll(Arrays.asList(skyline));
    for (int i = 1; i < skylineTuples.size(); ++i) {
      Point[] resultArray = result.toArray(new Point[]{});
      Point[] newArray = Iterables.toArray(skylineTuples.get(i)._2(), Point.class);
      Point[] mergeSkylines = mergeSkylines(resultArray, newArray);
      result.clear();
      result.addAll(Arrays.asList(mergeSkylines));
    }

    System.out.println("Merged individual skylines in: " + (System.currentTimeMillis() - start2) + " ms");
    System.out.println("Total skyline points: " + result.size());
    System.out.println("\nTotal time: " + (System.currentTimeMillis() - start) + " ms");
    return result.toArray(new Point[]{});
  }

  private Point[] mergeSkylines(Point[] skyline1, Point[] skyline2) {
    int cutPointForSkyline1 = 0;
    while (cutPointForSkyline1 < skyline1.length
        && !skylineDominate(skyline2[0], skyline1[cutPointForSkyline1])) {
      cutPointForSkyline1++;
    }
    Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
    System.arraycopy(skyline2, 0, result, cutPointForSkyline1, skyline2.length);
    return result;
  }

  private static boolean skylineDominate(Point p1, Point p2) {
    return p1.getX() >= p2.getX() && p1.getY() >= p2.getY();
  }

  private static Point[] skyline(Point[] points, int start, int end) {
    if (end - start == 1) {
      // Return the one input point as the skyline
      return new Point[]{points[start]};
    }
    int mid = (start + end) / 2;
    // Find the skyline of each half
    Point[] skyline1 = skyline(points, start, mid);
    Point[] skyline2 = skyline(points, mid, end);
    // Merge the two skylines
    int cutPointForSkyline1 = 0;
    while (cutPointForSkyline1 < skyline1.length
        && !skylineDominate(skyline2[0], skyline1[cutPointForSkyline1])) {
      cutPointForSkyline1++;
    }
    Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
    System.arraycopy(skyline2, 0, result, cutPointForSkyline1, skyline2.length);
    return result;
  }

  private List<Point> getPrunedPoints(final STRtree stRtree) {
    List<Envelope> cells = getAllCells(stRtree);
    System.out.println("Total cells before pruning: " + cells.size());
    long start = System.currentTimeMillis();
    Collection<Envelope> prunedCells = pruneCells(cells);
    long end = System.currentTimeMillis();
    System.out.println("Total cells after pruning: " + prunedCells.size());
    System.out.println("Time taken for pruning cells: " + (end - start) + " ms");
    final List<Point> result = Lists.newArrayList();
    for (Envelope envelope : prunedCells) {
      result.addAll(stRtree.query(envelope));
    }
    return result;
  }

  private Set<Envelope> pruneCells(final List<Envelope> cells) {
    Set<Envelope> result = new HashSet<>();
    for (int i = 0; i < cells.size(); i++) {
      Envelope ci = cells.get(i);
      int nonDominatedCount = 0;
      for (int j = 0; j < cells.size(); j++) {
        Envelope cj = cells.get(j);
        if (skylineDominateEnvelope(ci, cj)) {
          continue;
        } else {
          nonDominatedCount++;
        }
      }
      if (nonDominatedCount == cells.size()) {
        result.add(ci);
      }
    }
    return result;
  }

  private List<Envelope> getAllCells(final STRtree stRtree) {
    List<Envelope> result = Lists.newArrayList();
    getAllCellsImpl(stRtree.getRoot(), result);
    return result;
  }

  private void getAllCellsImpl(final AbstractNode root,
      final List<Envelope> result) {
    if (root.getLevel() == 0) {
      result.add((Envelope) root.getBounds());
    } else {
      for (Object childBound : root.getChildBoundables()) {
        getAllCellsImpl((AbstractNode) childBound, result);
      }
    }
  }

  private boolean skylineDominateEnvelope(final Envelope child1,
      final Envelope child2) {
    Point child1TopRight = getTopRight(child1);

    Point child2BottomLeft = getBottomLeft(child2);
    if (skylineDominate(child2BottomLeft, child1TopRight)) {
      return true;
    }

    Point child2TopLeft = getTopLeft((child2));
    if (skylineDominate(child2TopLeft, child1TopRight)) {
      return true;
    }

    Point child2BottomRight = getBottomRight(child2);
    if (skylineDominate(child2BottomRight, child1TopRight)) {
      return true;
    }

    return false;
  }

  private Point getTopRight(Envelope envelope) {
    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPoint(new Coordinate(envelope.getMaxX(), envelope.getMaxY()));
  }

  private Point getBottomRight(Envelope envelope) {
    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPoint(new Coordinate(envelope.getMaxX(), envelope.getMinY()));
  }

  private Point getBottomLeft(Envelope envelope) {
    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPoint(new Coordinate(envelope.getMinX(), envelope.getMinY()));
  }

  private Point getTopLeft(Envelope envelope) {
    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPoint(new Coordinate(envelope.getMinX(), envelope.getMaxY()));
  }

  /****************************** CLOSEST PAIR ***********************************/
  public Point[] closestPair() {
    return closestPair(DEFAULT_GRID_NUMBER_HORIZONTAL, DEFAULT_GRID_NUMBER_VERTICAL, false);
  }

  public Point[] closestPair(int GridNumberHorizontal, int GridNumberVertical) {
    return closestPair(GridNumberHorizontal, GridNumberVertical, false);
  }

  public Point[] closestPair(boolean debug) {
    return closestPair(DEFAULT_GRID_NUMBER_HORIZONTAL, DEFAULT_GRID_NUMBER_VERTICAL, debug);
  }

  public Point[] closestPair(int GridNumberHorizontal, int GridNumberVertical, boolean debug) {
    long start = System.currentTimeMillis();
    if (debug) {
      System.out.println("Mapping points");
    }

    int numPartitions = GridNumberHorizontal*GridNumberVertical;
    final Envelope boundary = this.boundary();
    final Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
    final Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
    final double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
    final double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
    for (int i = 0; i < GridNumberHorizontal + 1; i++) {
        gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
    }
    for (int i = 0; i < GridNumberVertical + 1; i++) {
       gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
    }

    JavaPairRDD<Integer, Point> keyToPointsData =
        this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,
                                          GridNumberVertical, gridHorizontalBorder, gridVerticalBorder));

    if (debug) {
      System.out.println("DONE Mapping points no=" + keyToPointsData.count()
          + " in " + (System.currentTimeMillis() - start));
    }

    long start2 = System.currentTimeMillis();
    if (debug) {
      System.out.println("Creating partitions from mapped points");
    }
    JavaPairRDD<Integer, Iterable<Point>> partitionedPointsRDD =
        keyToPointsData.groupByKey(numPartitions);
    if (debug) {
      System.out.println("DONE Creating partitions from mapped points: "
          + partitionedPointsRDD.count() + " in "
          + (System.currentTimeMillis() - start2) + "ms");
    }
    /*
     * Filter out candidates for the the final in-memory closest pair
     */
    start2 = System.currentTimeMillis();
    if (debug) {
      System.out.println("Calculating closestpairs individual partitions.");
    }
    partitionedPointsRDD =
        partitionedPointsRDD
            .mapValues(new Function<Iterable<Point>, Iterable<Point>>() {

              private static final long serialVersionUID = 4592384070663695223L;

              public Iterable<Point> call(Iterable<Point> v1) throws Exception {

                Point[] pointsArray = Iterables.toArray(v1, Point.class);
                if (pointsArray == null) {
                  return new ArrayList<Point>();
                }
                Arrays.sort(pointsArray, new PointXComparator());
                // calculate skyline.
                DistancePointPair closestPair =
                    closestPair(pointsArray, new Point[pointsArray.length], 0,
                        pointsArray.length - 1);

                List<Point> candidates = new ArrayList<Point>();
                if (closestPair == null || pointsArray.length == 1) {
                  candidates.add(pointsArray[0]);
                  return candidates;
                }
                candidates.add(closestPair.first);
                candidates.add(closestPair.second);
                for (Point p : pointsArray) {
                  if (p.equals(closestPair.first)
                      || p.equals(closestPair.second)) {
                    continue;
                  }
                  double l = ((int) (p.getX() - boundary.getMinX()) / LongitudeIncrement) * LongitudeIncrement;
                  double r = l + LongitudeIncrement;
                  double b = ((int) (p.getY() - boundary.getMinY()) / LatitudeIncrement) * LatitudeIncrement;
                  double t = b + LatitudeIncrement;
                  double distance = closestPair.distance;
                  if (p.getX() - l <= distance || r - p.getX() <= distance) {
                    candidates.add(p);
                    continue;
                  }
                  if (p.getY() - b <= distance || t - p.getY() <= distance) {
                    candidates.add(p);
                  }
                }

                return candidates;
              }
            });
    // partitionedPointsRDD = partitionedPointsRDD.cache();
    if (debug) {
      System.out
          .println("DONE Calculating closest pairs for partitions. Number of partitions: "
              + partitionedPointsRDD.count()
              + " in "
              + (System.currentTimeMillis() - start2) + "ms");
    }

    /*
     * Calculate closest pairs from filtered candidates
     */
    start2 = System.currentTimeMillis();
    if (debug) {
      System.out.println("Calculating closest pairs from candidates.");
    }
    JavaRDD<Iterable<Point>> values = partitionedPointsRDD.values();
    List<Iterable<Point>> array = values.toArray();
    List<Point> finalCandidates = new ArrayList<Point>();
    for (Iterable<Point> points : array) {
      for (Point point : points) {
        finalCandidates.add(point);
      }
    }
    Collections.sort(finalCandidates, new PointXComparator());
    if (debug) {
      System.out.println("Final candidates size: " + finalCandidates.size());
    }
    Point[] listToArray = finalCandidates.toArray(new Point[]{});
    DistancePointPair closestPair =
        closestPair(listToArray, new Point[listToArray.length], 0,
            listToArray.length - 1);
    if (debug) {
      System.out.println("DONE Calculating closest pairs from candidates in "
          + (System.currentTimeMillis() - start2) + "ms");
      System.out.println("Closest pair: ");
      System.out.println("Point 1: " + closestPair.first);
      System.out.println("Point 2: " + closestPair.second);
      System.out.println("Distance: " + closestPair.distance);
      System.out.println("Total time taken: "
          + (System.currentTimeMillis() - start) + "ms");
    }
    Coordinate point1 = new Coordinate(closestPair.first.getX(), closestPair.first.getY());
    Coordinate point2 = new Coordinate(closestPair.second.getX(), closestPair.second.getY());
    GeometryFactory geometryFactory = new GeometryFactory();
    return new Point[]{geometryFactory.createPoint(point1), geometryFactory.createPoint(point2)};
  }

  public Point[] closestPairLocal() {
    Point[] pointsArray = this.pointRDD.toArray().toArray(new Point[]{});
    Arrays.sort(pointsArray, new PointXComparator());
    DistancePointPair closestPair = closestPair(pointsArray, new Point[pointsArray.length], 0,
        pointsArray.length - 1);
    Coordinate point1 = new Coordinate(closestPair.first.getX(), closestPair.first.getY());
    Coordinate point2 = new Coordinate(closestPair.second.getX(), closestPair.second.getY());
    GeometryFactory geometryFactory = new GeometryFactory();
    return new Point[]{geometryFactory.createPoint(point1), geometryFactory.createPoint(point2)};
  }

  public static class DistancePointPair {
    public Point first;
    public Point second;
    public double distance;

    public DistancePointPair(Point first, Point second, double distance) {
      this.first = first;
      this.second = second;
      this.distance = distance;
    }

    public DistancePointPair() {
    }
  }


  private DistancePointPair closestPair(Point[] input, Point[] tmp, int l,
      int r) {
    if (l >= r) {
      return null;
    }

    int mid = (l + r) >> 1;
    double medianX = input[mid].getX();
    DistancePointPair delta1 = closestPair(input, tmp, l, mid);
    DistancePointPair delta2 = closestPair(input, tmp, mid + 1, r);
    DistancePointPair delta;
    if (delta1 == null || delta2 == null) {
      delta = delta1 == null ? delta2 : delta1;
    } else {
      delta = delta1.distance < delta2.distance ? delta1 : delta2;
    }
    int i = l, j = mid + 1, k = l;

    while (i <= mid && j <= r) {
      if (input[i].getY() < input[j].getY()) {
        tmp[k++] = input[i++];
      } else {
        tmp[k++] = input[j++];
      }
    }
    while (i <= mid) {
      tmp[k++] = input[i++];
    }
    while (j <= r) {
      tmp[k++] = input[j++];
    }

    for (i = l; i <= r; i++) {
      input[i] = tmp[i];
    }

    k = l;
    for (i = l; i <= r; i++) {
      if (delta == null || Math.abs(tmp[i].getX() - medianX) <= delta.distance) {
        tmp[k++] = tmp[i];
      }
    }

    for (i = l; i < k; i++) {
      for (j = i + 1; j < k; j++) {
        if (delta != null && tmp[j].getY() - tmp[i].getY() >= delta.distance) {
          break;
        } else if (delta == null || tmp[i].distance(tmp[j]) < delta.distance) {
          if (delta == null) {
            delta = new DistancePointPair();
          }
          delta.distance = tmp[i].distance(tmp[j]);
          delta.first = tmp[i];
          delta.second = tmp[j];
        }
      }
    }
    return delta;
  }


  /********************************** CONVEX HULL ******************************************/


  public Point[] convexHull() {
    return convexHull(DEFAULT_NUM_PARTITIONS);
  }

  public Point[] convexHull(int numPartitions) {
    System.out.println("Initial number of partitions (input splits): " + numSplits);
    System.out.println("Requested number of partitions (for RDD): " + numPartitions);
    long start = System.currentTimeMillis();
    Point[] points = convexHullImpl(pointRDD.repartition(numPartitions));
    long end = System.currentTimeMillis();
    System.out.println("Time taken: " + (end - start) + " ms");
    return points;
  }


  private Point[] convexHullImpl(JavaRDD<Point> points) {
    JavaRDD<Point> localHulls = points.mapPartitions(new LocalHull()).repartition(1);
    JavaRDD<Point> globalHull = localHulls.mapPartitions(new GlobalHull());
    return globalHull.toArray().toArray(new Point[]{});
  }

  public static class LocalHull
      implements FlatMapFunction<Iterator<Point>, Point>, Serializable {
    private static final long serialVersionUID = 1L;

    //Iterate over the points to calcualte the convex hull
    public Iterable<Point> call(Iterator<Point> s) {
      List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
      GeometryFactory geom = new GeometryFactory();
      try {
        while (s.hasNext()) {
          //Read the points
          Point coord = s.next();

          //Add the point to the list of Active Coordinates
          ActiveCoords.add(coord.getCoordinate());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]),
          geom);
      Geometry g = ch.getConvexHull();
      Coordinate[] cs = g.getCoordinates();

      Set<Point> points = new HashSet<>();
      for (Coordinate c : cs) {
        points.add(geom.createPoint(c));
      }
      return points;
    }
  }

  //This calculates the global hull using the list of the local hulls.
  public static class GlobalHull
      implements FlatMapFunction<Iterator<Point>, Point>, Serializable {
    private static final long serialVersionUID = 1L;

    //Iterates over all the partitions and iteratively calcualtes the convex hull
    public Iterable<Point> call(Iterator<Point> givListIter) {
      List<Coordinate> polList = new ArrayList<Coordinate>();
      GeometryFactory geom = new GeometryFactory();
      while (givListIter.hasNext()) {
        Point tempPol = givListIter.next();
        polList.add(tempPol.getCoordinate());
      }
      ConvexHull ch = new ConvexHull(polList.toArray(new Coordinate[polList.size()]), geom);
      Geometry g = ch.getConvexHull();
      Coordinate[] cs = g.getCoordinates();

      Set<Point> points = new HashSet<>();
      for (Coordinate c : cs) {
        points.add(geom.createPoint(c));
      }
      return points;
    }
  }
}
