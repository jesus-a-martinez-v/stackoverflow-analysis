# stackoverflow-analysis
Implementation of distributed K-Means over a corpus of StackOverflow posts used to determine the popularity of certain programming languages.

The overall goal of this project is to implement a distributed k-means algorithm which clusters posts on the popular 
question-answer platform StackOverflow according to their score.

The motivation is as follows: StackOverflow is an important source of documentation. However, different user-provided answers 
may have very different ratings (based on user votes) based on their perceived value. Therefore, we would like to look at the 
distribution of questions and their answers. For example, how many highly-rated answers do StackOverflow users post, and 
how high are their scores? Are there big differences between higher-rated answers and lower-rated ones?

Also we are interested in comparing these distributions for different programming language communities. 
Differences in distributions could reflect differences in the availability of documentation. For example, StackOverflow 
could have better documentation for a certain library than that library's API documentation. However, to avoid invalid 
conclusions we will focus on the well-defined problem of clustering answers according to their scores.

## Getting the data

You can download the necessary data by clicking [here](https://drive.google.com/file/d/0B1SO9hJRt-hgSGt3eHRiOGx3dG8/view?usp=sharing). Place it inside `src/main/resources/stackoverflow`.

## Running the code

Just run `sbt run` on your console.

## TODO

 * Add more unit tests.
