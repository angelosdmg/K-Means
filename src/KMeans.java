import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("Deprecation")
public class KMeans {

    //in - out
    public static String OUT = "outfile";
    public static String IN = "inputlarger";


    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-00000";
    public static String DATA_FILE_NAME = "/points.txt";
    public static String JOB_NAME = "KMeans";

    
    public static String SPLITTER = "\t| ";
    public static List<String> mCenters = new ArrayList<String>();


    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void configure(JobConf job) {
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            String [] temp = line.split(SPLITTER);
                            mCenters.add(temp[0]);
                        }
                    } finally {
                        cacheReader.close();
                    }

                }

            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException{

            String line = value.toString();
            double min1;
            double min2 = Double.MAX_VALUE;
            String nearest_center = mCenters.get(0);

            // Find the minimum center from a point
            for (String c : mCenters) {

                min1 = euclidean(c,line);
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = c;
                    min2 = min1;
                }
            }

            // Emit the nearest center and the point
            output.collect(new Text(nearest_center), new Text(line));

        }


    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            double sumX = 0;
            double sumY = 0;
            String points = "";

            int num_of_elements = 0;
            while (values.hasNext()) {
                String point = values.next().toString();
                String[] points_x_y = point.split(",");

                points += " " + points_x_y[0] + "," + points_x_y[1];
                double pointX = Double.parseDouble(points_x_y[0]);
                double pointY = Double.parseDouble(points_x_y[1]);
                sumX += pointX;
                sumY += pointY;
                ++num_of_elements;
            }

            double newX = sumX / num_of_elements;
            double newY = sumY / num_of_elements;
            String newCenter = Double.toString(newX) + "," + Double.toString(newY);

            output.collect(new Text(newCenter), new Text(points));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {

        IN = args[0]; //το folder που βρίσκονται τα input files
        OUT = args[1];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;

        // Reiterating till the convergence (convergence changed)
        int iteration = 0; // controlling the first iteration of the program
        boolean isdone = false; //true when convergence is reached
        while (!isdone) {
            JobConf conf = new JobConf(KMeans.class);
            if (iteration == 0) {
                Path hdfsPath = new Path(input + CENTROID_FILE_NAME);

                org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } else {
                Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);

                org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(hdfsPath.toUri(), conf);

            }

            conf.setJobName(JOB_NAME);
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(conf, new Path(output));

            JobClient.runJob(conf);

            Path ofile = new Path(output + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));

            List<Text> centers_next = new ArrayList<Text>();
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split(SPLITTER);
                Text center = new Text(sp[0]);
                centers_next.add(center);
                line = br.readLine();
            }
            br.close();

            String prev;
            if (iteration == 0) {
                prev = input + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(
                    fs1.open(prevfile)));
            List<Text> centers_prev = new ArrayList<Text>();
            String prev_lines = br1.readLine();
            while (prev_lines != null) {
                String[] sp1 = prev_lines.split(SPLITTER);
                Text center = new Text(sp1[0]);
                centers_prev.add(center);
                prev_lines = br1.readLine();
            }
            br1.close();

            Collections.sort(centers_next);
            Collections.sort(centers_prev);

            Iterator<Text> it = centers_prev.iterator();
            for (Text center_next : centers_next) {
                Text temp = it.next();
                double distance = euclidean(temp.toString(), center_next.toString());
                if (Math.abs(distance) <= 3) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            if(isdone) {
                System.out.println(centers_next.get(0).toString());
                System.out.println(centers_next.get(1).toString());
                System.out.println(centers_next.get(2).toString());
                System.out.println(iteration);
            }

            again_input = output;
            output = OUT + System.nanoTime();
        }
    }

    public static double euclidean(String centroid, String point) {
        String[] centroidArray = centroid.split(",");
        double centroidX = Double.parseDouble(centroidArray[0]);
        double centroidY = Double.parseDouble(centroidArray[1]);

        String[] pointArray = point.split(",");
        double pointX = Double.parseDouble(pointArray[0]);
        double pointY = Double.parseDouble(pointArray[1]);

        double xDistance = centroidX - pointX;
        double xDistanceSquare = Math.pow(xDistance, 2);

        double yDistance = centroidY - pointY;
        double yDistanceSquare = Math.pow(yDistance, 2);

        double distance = xDistanceSquare + yDistanceSquare;
        return distance;
    }

}
