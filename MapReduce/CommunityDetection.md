

# Community Detection in Online Social Networks


Community detection has drawn lots of attention these years in machine learning field. With the popularity of online social networks, such as Facebook, Twitter and Sina Weibo, we can get many valuable datasets to develop algorithms.

You'll implement a community detection algorithm for the online social networks. Basically, people belong to the same community when they have high similarity (i.e. sharing many common interests). There are two types of relationship in online social networks. One is symmetric, which means when Alice is Bob’s friend, Bob will also be Alice's friend; the other is asymmetric, which means Bob may not be Alice’s friend even Alice is Bob’s friend. In the second case, there are two roles in the relationship: follower and followee (like the case when using Twitter and Weibo). When Alice follows Bob, Alice is the follower and Bob is the followee.

In order to detect communities, we need to calculate the similarity between any pair of users. In this homework, similarity is measured by the number of common followees for the given pair of users. The following figure illustrates the process.

![](https://raw.githubusercontent.com/justinjiajia/img/master/MapReduce/mr-community_detection.PNG)



The set of followees of A is {B, C, E} and set of followees of B is {A, C, E}. There are 2 common followees between A and B (i.e., C and E). The similarity between A and B is therefore 2.


We will provide three datasets with different sizes, generated from Sina Weibo users' relationship. The smaller dataset contains 1000 users, and the medium one contains around 2.5 million users, and the large one contains 4.8 million users. Each user is represented by its unique ID number. The download links of three datasets are listed in the Ref [6] - [8]. The small dataset is provided to facilitate your initial debugging and testing. The design of your program should be scalable enough to handle all the datasets.


The format of the data file is as follows (different followers separated by space):

```text
followee_1_id:follower_1_id follower_2_id follower_3_id ….
followee_2_id:follower_1_id follower_6_id follower_7_id ….
...
```

For example, in the above figure, the data is represented as:

```text
A:B D
B:A
C:A B E
E:A B C
```


The output of the community detection is that for EVERY user, we want to know the TOP K most similar persons. The output format should be (different similar persons separated by space):

```text
User_1:Similiar_Person_1 Similiar_Person_2 … Similiar_Person_K
User_2:Similiar_Person_1 Similiar_Person_2 … Similiar_Person_K
…

```


Solve the above community detection problem using MapReduce. You are free to use any programming languages to implement the required MapReduce components, mapper(s), reducer(s), partitioner(s), etc. (e.g by leveraging the "Hadoop Streaming" capability).

Again, the design of your program should be scalable enough to handle all three of the datasets. In other words, you are expected to use the same program to solve the following problems:

- a) Get the TOP 10 (=K) most similar people of EVERY user in the medium-sized dataset in Ref[7].

- b) Get the TOP 10 (=K) most similar people of EVERY user in the large dataset in Ref [8]. (Hints: use [composite key](http://tutorials.techmytalk.com/2014/11/14/mapreduce-composite-key-operation-part2/) and [secondary sort](http://codingjunkie.net/secondary-sort/))


Warning: Please closely monitor the resource consumption, including memory and hard disk. The Amazon free tier may not provide enough hard disk. You need to apply for extra storage or use other platforms.


Hints:

- 1. Since the medium-sized dataset is also quite large already, to avoid excessive AWS charges, you are advised to you are advised to be careful when dealing with AWS.

- 2. As for the large dataset, you may want to set `mapreduce.map.output.compress=true` to compress the intermediate results, in case you don’t have enough local hard disk space (to hold the intermediate tuples).

- 3. The large dataset may take a long time (around 10 hours) to process even if everything is correct. You should start doing this homework as early as possible!


## Submission Requirements:

1. Submit the MapReduce code

2. As for a) and b), submit the community detection results of all those users whose IDs share the same last 5 digits with your DGUT student ID. For example, if your student ID is 115500054321, then you need to submit the community members for users with ID = 54321, 154321, 254321, 354321, ..., 1154321, ...



## References


[6] Small scale dataset
https://www.dropbox.com/s/ntzk80l5iiiuh50/Small%20Dataset.txt?dl=0
[7] Medium scale dataset
https://www.dropbox.com/s/6sxnnadhxbyk7ho/Medium%20Dataset.txt?dl=0
[8] Large scale dataset
https://www.dropbox.com/s/lrlgz50m88j6fpc/Large%20Dataset.txt?dl=0
