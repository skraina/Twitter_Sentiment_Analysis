package com.github.skraina.FTSA.Hive.UDF;

//import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;


import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.lang.Exception;


public class TweetClassifier extends UDF
{
	//private Integer TweetScore;
	private static List<String> PosWords; // Storage for positive words
	private static List<String> NegWords; // Storage for negative words
	
	// Evaluate method returns final score for each tweet after subtracting #of negative words from #of positive words.
	public Integer evaluate(String TweetText, String PosFilePath, String NegFilePath) throws Exception
	{
		int TweetScore = 0;
		if(PosWords == null)  // Build list only at first call of Evaluate method
		{
			PosWords = new ArrayList<String>();
			BuildWordList(PosWords, PosFilePath);
		}
		if(NegWords == null)
		{
			NegWords = new ArrayList<String>();
			BuildWordList(NegWords, NegFilePath);
		}
				
		TweetScore = EvalTweetScore(TweetText);
		return TweetScore;
	}
	
	public void BuildWordList(List<String> WordType, String WordFilePath) throws Exception
	{
		String currWord = null;
		try
		{
			// Reading positive/negative word files and build lists
			BufferedReader WordReader = new BufferedReader(new FileReader(new File(WordFilePath)));
			currWord = WordReader.readLine();

			// Word files have some lines beginning with ; and \n and contains +, thus need to be filtered out
			while((currWord = WordReader.readLine()) != null)
			{
				if(!currWord.startsWith(";") && currWord.length() != 0 && !currWord.contains("+"))
				{
					WordType.add(currWord);
				}
			}
			WordReader.close();
		}
		catch (FileNotFoundException ex)
		{
			throw new HiveException("Word File " + WordFilePath + " not found at path");
		}
		catch (IOException ex)
		{
			throw new HiveException();
		}
		
	}
	
	// Method compares each word for a tweet to positive as well as negative word lists and evaluates absolute score for each tweet.
	public Integer EvalTweetScore(String TweetText)
	{
		int posCnt = 0;  // counter for positive words
		int negCnt = 0;  // counter for negative words
		
		String[] TweetWords = TweetText.split(" ");
		
		// Check whether Tweet words exist in positive or negative word list
		for(int i = 0; i < TweetWords.length; i++)
		{
			if(PosWords.contains(TweetWords[i]))
			{
				posCnt++;
			}
			if(NegWords.contains(TweetWords[i]))
			{
				negCnt++;
			}
		}
		int absScore = posCnt - negCnt; // Absolute score is evaluated by subtracting negative count from positive count.  
		//return absScore;
		//Evaluate relative score
		if(absScore < 0)
		{
			return -1;
		}
		else if(absScore > 0)
		{
			return 1;
		}
		return 0;  // Either words do not exist or equal number of positive and negative words.
	}
	
}
