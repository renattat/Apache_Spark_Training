package project2;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
//		InferCSVSchema parser = new InferCSVSchema();
//		parser.printSchema();
		
//		DefineCSVSchema parser2 = new DefineCSVSchema();
//		parser2.printDefinedSchema();
//
		JSONLinesParser parser3 = new JSONLinesParser();
		parser3.parseJsonLines();

	}

}
