package edu.thu.db;

import java.io.Serializable;

/**
 * Created by jugs on 12/27/16.
 */
public class MyMovie implements Serializable
{
    public String review;
    public String fname;
    public String title;

    public MyMovie()
    {

    }

    public MyMovie(String review, String fname, String title)
    {
        this.review = review;
        this.fname = fname;
        this.title = title;
    }

    public String getReview()
    {
        return review;
    }

    public void setReview(String review)
    {
        this.review = review;
    }

    public String getFname()
    {
        return fname;
    }

    public void setFname(String fname)
    {
        this.fname = fname;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }


}
