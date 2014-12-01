package de.unibonn.iai.eis.luzzu.io.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.qml.parser.LQMLParser;
import de.unibonn.iai.eis.luzzu.qml.parser.ParseException;


public class DeclerativeMetricCompiler {
	
	final static Logger logger = LoggerFactory.getLogger(DeclerativeMetricCompiler.class);

	private static DeclerativeMetricCompiler instance = null;

	private final StringBuilder javaClass = new StringBuilder();
	
	public DeclerativeMetricCompiler() throws URISyntaxException, IOException{
		this.loadDeclerativePattern(); 
	}

	public static DeclerativeMetricCompiler getInstance(){
		if (instance  == null) {
			logger.info("Initialising and verifying external metrics.");
			try {
				instance = new DeclerativeMetricCompiler();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return instance;
	}
	
	
	private static FileFilter lqmFilter = new FileFilter() {
		public boolean accept(File file) {
			if (file.getName().endsWith(".lqm")) {
				return true;
			}
			return false;
		}
	};
	
	@SuppressWarnings({ "unchecked", "resource" })
	public Map<String, Class<? extends QualityMetric>> compile() throws IOException, ParseException {
		Map<String, Class<? extends QualityMetric>> clazzes = new HashMap<String, Class<? extends QualityMetric>>();
		
		//get system compiler:
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
 
        // for compilation diagnostic message processing on compilation WARNING/ERROR
        MyDiagnosticListener c = new MyDiagnosticListener();
		
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(c, Locale.ENGLISH,null);
        
        File f = new File("classes/");
        if (!f.exists()) f.mkdir();
        
        //specify classes output folder
        List<String> options = Arrays.asList("-d", "classes/");
	    
		// parse and compile declerative functions
		Set<URI> lqiSet = this.loadMetrics();
		
		if (lqiSet.size() > 0){
		try {
			this.loadDeclerativePattern();
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}}
		
		
		for(URI lqiMetric : lqiSet){
			//parse
			Tuple parsed = this.parse(lqiMetric);
			
			JavaFileObject so = null;
			try
			{
				so = new InMemoryJavaFileObject(parsed.className, parsed.javaClass);
			} catch (Exception exception){
				exception.printStackTrace();
			}
			
			Iterable<? extends JavaFileObject> files = Arrays.asList(so);
		    
			//compile
			JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager,c, options, null, files);
	        
			task.call();

			//create classes
			File file = new File("classes/");
			URL uri = file.toURI().toURL();
			URL[] urls = new URL[] { uri };
			
			ClassLoader loader = new URLClassLoader(urls);
			
			Class<? extends QualityMetric> clazz = null;
			try{
				clazz = (Class<? extends QualityMetric>) loader.loadClass(parsed.className);
			} catch (ClassNotFoundException e) {
				logger.error("Class {} is not found. Skipped loading the class.", parsed.className );
				continue;
			}

			clazzes.put(parsed.className, clazz);
		}
		
	    fileManager.close();
	    
	    return clazzes;
	}
	
	
	private void loadDeclerativePattern() throws URISyntaxException, IOException{
		
		String nextLine = null;
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(
					new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("declerative_pattern.txt"), 
					Charset.defaultCharset()));
			
			while((nextLine = reader.readLine()) != null) {
				this.javaClass.append(nextLine);
			}
		} finally {
			if(reader != null) {
				reader.close();
			}
		}
	}
	
	private Tuple parse(URI lqmFile) throws IOException, ParseException{
		List<String> lines = Files.readAllLines(Paths.get(lqmFile), Charset.defaultCharset());
		StringBuilder sb = new StringBuilder();
		for (String s : lines)
			sb.append(s);
		
		Reader reader = new StringReader(sb.toString()) ;
	    LQMLParser parser = new LQMLParser(reader) ;
	    
	    Map<String, String> parse = parser.parse();
	    
	    String _jClass = this.javaClass.toString();
	    
	    _jClass = _jClass.replace("[%%packagename%%]", parse.get("[%%packagename%%]"));
	    _jClass = _jClass.replace("[%%metricuri%%]", parse.get("[%%metricuri%%]"));
	    _jClass = _jClass.replace("[%%imports%%]", parse.get("[%%imports%%]"));
	    _jClass = _jClass.replace("[%%author%%]", parse.get("[%%author%%]"));
	    _jClass = _jClass.replace("[%%label%%]", parse.get("[%%label%%]"));
	    _jClass = _jClass.replace("[%%description%%]", parse.get("[%%description%%]"));
	    _jClass = _jClass.replace("[%%classname%%]", parse.get("[%%classname%%]"));
	    _jClass = _jClass.replace("[%%variables%%]", parse.get("[%%variables%%]"));
	    _jClass = _jClass.replace("[%%computefunction%%]", parse.get("[%%computefunction%%]"));
	    _jClass = _jClass.replace("[%%metricvaluefuntion%%]", parse.get("[%%metricvaluefuntion%%]"));
	
	    Tuple t = new Tuple();
	    t.className = parse.get("[%%packagename%%]") + "." +parse.get("[%%classname%%]");
	    t.javaClass = _jClass;
	    
	    return t;
	}
	
	private Set<URI> loadMetrics() throws IOException, ParseException{
		Set<URI> files = new HashSet<URI>(); 
		File externalsFolder = new File("externals/");
		File[] listOfFiles = externalsFolder.listFiles();
		
		for(File metrics : listOfFiles){
			for(File declFile : metrics.listFiles(lqmFilter))
				files.add(declFile.toURI());
		}
		return files;
	}
	
	private class Tuple{
		protected String className;
		protected String javaClass;
	}
	
	
	
	// taken from: http://www.beyondlinux.com/2011/07/20/3-steps-to-dynamically-compile-instantiate-and-run-a-java-class/
	private static class MyDiagnosticListener implements DiagnosticListener<JavaFileObject>
    {
        public void report(Diagnostic<? extends JavaFileObject> diagnostic)
        {
            System.out.println("Line Number->" + diagnostic.getLineNumber());
            System.out.println("code->" + diagnostic.getCode());
            System.out.println("Message->"+ diagnostic.getMessage(Locale.ENGLISH));
            System.out.println("Source->" + diagnostic.getSource());
            System.out.println(" ");
        }
    }
	
	 /** java File Object represents an in-memory java source file <br>
     * so there is no need to put the source file on hard disk  **/
    private static class InMemoryJavaFileObject extends SimpleJavaFileObject
    {
        private String contents = null;
 
        public InMemoryJavaFileObject(String className, String contents) throws Exception
        {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.contents = contents;
        }
 
        public CharSequence getCharContent(boolean ignoreEncodingErrors)
                throws IOException
        {
            return contents;
        }
    }
}
