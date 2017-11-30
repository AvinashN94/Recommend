package Movie;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import au.com.bytecode.opencsv.CSVParser;

import scala.Tuple2;


public class Recommend {
	static String result = "";

	public static void main(final String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("CS185 Project").setMaster("local");
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> u1 = sc.textFile(args[0]);

		JavaRDD<String> i1 = sc.textFile(args[1]);
	
		JavaRDD<Rating> r1 = u1.map(new Function<String, Rating>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
			}
		});
		
		JavaPairRDD<Integer,String> i2 = i1.mapToPair(
				new PairFunction<String, Integer, String>() {
		
					private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(String t) throws Exception {
				CSVParser parser = new CSVParser();		
				String[] s = parser.parseLine(t);
				return new Tuple2<Integer,String>(Integer.parseInt(s[0]), s[1]);
			}
		});

		MatrixFactorizationModel m1 = ALS.trainImplicit(JavaRDD.toRDD(r1),10, 10);
		JavaRDD<Tuple2<Object, Object>> u2 = r1
				.map(new Function<Rating, Tuple2<Object, Object>>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Object, Object> call(Rating r) {
						return new Tuple2<Object, Object>(r.user(), r.product());
					}
				});
		JavaRDD<Integer> n1 = u2.filter(new Function<Tuple2<Object,Object>, Boolean>() {
		
		
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Object, Object> v1) throws Exception {
				if (((Integer) v1._1).intValue() != Integer.parseInt(args[2])) {
					return true;
				}
				//System.out.println(v1._1);
				return false;
			}
		}).map(new Function<Tuple2<Object,Object>, Integer>() {
		
			
			private static final long serialVersionUID = 1L;

			public Integer call(Tuple2<Object, Object> v1) throws Exception {
				return (Integer) v1._2;
			}
		});
	
		JavaRDD<Tuple2<Object, Object>> in1 = n1
				.map(new Function<Integer, Tuple2<Object, Object>>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Object, Object> call(Integer r) {
						return new Tuple2<Object, Object>(Integer.parseInt(args[2]), r);
					}
		});

		JavaRDD<Rating> rcm1 = m1.predict(in1.rdd()).toJavaRDD().distinct();
		rcm1 = rcm1.sortBy(new Function<Rating,Double>(){
	
		
			private static final long serialVersionUID = 1L;

			public Double call(Rating v1) throws Exception {
				return v1.rating();
			}
			
		}, false, 1);	

		JavaRDD<Rating> tR1 = sc.parallelize(rcm1.take(10));
		tR1.foreach(new VoidFunction<Rating>() {

			private static final long serialVersionUID = 1L;

			public void call(Rating arg0) throws Exception {
			 System.out.println(arg0.toString());
				
			}
		});

		JavaRDD<Tuple2<Rating, String>> rt = tR1.mapToPair(
				new PairFunction<Rating, Integer, Rating>() {
		
					private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Rating> call(Rating t) throws Exception {
				return new Tuple2<Integer,Rating>(t.product(),t);
			}
		}).join(i2).values();
		
		
		//Print the top recommendations for user 1.
		rt.foreach(new VoidFunction<Tuple2<Rating,String>>() {
			
			
			private static final long serialVersionUID = 1L;

			
			public void call(Tuple2<Rating, String> t) throws Exception {
				
				System.out.println(t._1.product() + "\t" + t._1.rating() + "\t" + t._2);
				result=result+t._1.product() + "," + t._1.rating() + "," + t._2+"\n";
			}
		});
		PrintWriter writer;
		try {
			writer = new PrintWriter("wow.txt", "UTF-8");
			writer.println(result);
			
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	Map<String, String> maa =  new HashMap<>();
	//	maa.put("es.nodes", "localhost");
	//	maa.put("es.port", "9200");
	//	JavaEsSpark.saveToEs(recommendedItems, "test/movie",maa);
	//	recommendedItems.saveAsTextFile("wow");
	
	
	sc.close();
		
	}

}