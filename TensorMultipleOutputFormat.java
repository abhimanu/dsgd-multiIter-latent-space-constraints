import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class TensorMultipleOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	protected String generateFileNameForKeyValue(Text key, Text value, String name) {
		char c = key.toString().charAt(0);
		return c + "/" + key.toString();
		//return name;
	}
}
