package org.janelia.thickness.experiments;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.thickness.utility.SparkRender;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import loci.formats.FormatException;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class SparkRenderWave {

    public static void main(String[] args) throws IOException, FormatException {

        SparkConf sparkConf = new SparkConf().setAppName("Render");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final String sourceFormat = args[0];
        final String transformFormat = args[1];
        final String outputFormat = args[2];
        int radius = Integer.parseInt(args[3]);
        int step = Integer.parseInt(args[4]);
        int start = Integer.parseInt(args[5]);
        int stop = Integer.parseInt(args[6]);

        ImagePlus img0 = new ImagePlus(String.format(sourceFormat, start));

        final int width = img0.getWidth();
        final int height = img0.getHeight();
        final int size = stop - start;

        System.out.println( sourceFormat );
        System.out.println( transformFormat );
        System.out.println( outputFormat );
        System.out.println( String.format( "width=%d, height=%d, size=%d", width, height, size ) );

        final long[] dim = new long[]{width, height, size};

        ImagePlus firstTransform = new ImagePlus(String.format(transformFormat, 0) );
        final int transformWidth = firstTransform.getWidth();
        final int transformHeight = firstTransform.getHeight();
        System.out.println( String.format( "transform width=%d, transform height=%d", transformWidth, transformHeight ) );






        final double[] radiiDouble = new double[]{ radius, radius };
        final double[] stepsDouble = new double[]{ step, step };

        JavaRDD<Integer> indices = sc.parallelize(Utility.arange(size));
        JavaPairRDD<Integer, FloatProcessor> sections = indices
                .mapToPair(new Utility.Duplicate<>())
                .sortByKey()
                .map(new Utility.DropValue<>())
                .mapToPair(new Utility.LoadFileFromPattern(sourceFormat))
                .cache();
        System.out.println( "sections: " + sections.count() );

        JavaPairRDD<Integer, FloatProcessor> transforms = indices
                .mapToPair(new Utility.Duplicate<>())
                .sortByKey()
                .map(new Utility.DropValue<>())
                .mapToPair(new Utility.LoadFileFromPattern( transformFormat ) )
                .cache();

        System.out.println( "transforms: " + transforms.count() );

        JavaPairRDD<Integer, FloatProcessor> transformed = SparkRender.render(sc, sections, transforms, stepsDouble, radiiDouble, dim, 1.0 /* fix this */, start ).cache();
        System.out.println( "transformed: " + transformed.count() );


        List<Tuple2<Integer, Boolean>> successOnWrite = transformed
                .mapToPair(new Utility.WriteToFormatString<Integer>(outputFormat))
                .collect();
        int count = 0;
        for ( Tuple2<Integer, Boolean> s : successOnWrite )
        {
            if ( s._2().booleanValue() )
                continue;
            ++count;
            System.out.println( "Failed to write forward image " + s._1().intValue() );
        }
        System.out.println( "Successfully transformed and wrote " + (size-count) + "/" + size + " images." );

    }

}
