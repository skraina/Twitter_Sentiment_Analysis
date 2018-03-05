package com.github.skraina.FTSA.Preprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.fs.Path;

import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;


public class PreprocessMapper extends Mapper <LongWritable, Text, NullWritable, Text> 
{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		// short format of a tweet
		//		{"in_reply_to_status_id_str":null,"in_reply_to_status_id":null,"created_at":"Sun Aug 30 13:00:00 +0000 2015","in_reply_to_user_id_str":null,"source":"<a href=\"https://roundteam.co\" rel=\"nofollow\">RoundTeam<\/a>","retweeted_status":{"in_reply_to_status_id_str":null,"in_reply_to_status_id":null,"possibly_sensitive":false,"coordinates":null,"created_at":"Sun Aug 30 12:38:23 +0000 2015","truncated":false,"in_reply_to_user_id_str":null,"source":"<a href=\"http://www.mba-exchange.com\" rel=\"nofollow\">Mba Jobs in Pacific<\/a>",
		String txt_raw = null; // raw tweet as a String
		String txt_clean = null; // raw tweet preprocessed String
		JsonNode rootNode = null; // rootnode of the JSON structure
		try {
			rootNode = new JdomParser().parse(value.toString());
			txt_raw = rootNode.getStringValue("text"); // getting the value of the text field
			// Filtering out non-English tweets
			if(!rootNode.getStringValue("lang").equals("en"))
				return;
			// Removing RT (Retweet), Twitter handles from beginning, URLs, Twitter handles inside Text, hyphen from joined words, all special chars except _ and space, extra spaces from start/end/between. 
			txt_clean = txt_raw.replaceAll("RT\\s+@\\w+:", "").replaceAll("http\\S+", "").replaceAll("@\\S+", "").replaceAll("(\\w)-(\\w)", "$1 $2").replaceAll("[^a-zA-Z0-9_ ]", " ").replaceAll(" +", " ").trim().toLowerCase();
		} catch (InvalidSyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
/*		if(value.toString().startsWith(";") || value.toString().length() == 0 || value.toString().contains("+"))
		{
			return;
		}
		else
		{
			//String jsonData[] = value.toString().split(" ");
			String jsonData = value.toString();
			context.write(NullWritable.get(), new Text(jsonData));
			int cnt = 0;
			while(cnt <= jsonData.length-1)
			{
				String fPart = jsonData[cnt];
				context.write(NullWritable.get(), new Text(fPart));
				cnt = cnt + 1;
			}

		}
*/
		context.write(NullWritable.get(), new Text(txt_clean));
	}


}
