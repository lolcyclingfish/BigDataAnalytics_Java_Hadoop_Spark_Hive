package com.javamakeuse.hadoop.poc.Homework2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String temp = "Xyz";
        System.out.println(temp.substring(0,1));
    	if (temp.substring(0,1).matches("[d-fD-F]")) {
    	System.out.println("Yes, matched.");} else {
    		System.out.println( "No, not matched.");
    	}
    }
}
