# Chinese N-Gram Language Model MapReduce
A distributed chinese n-gram language model implementation for train and test on large corpus , using [Hadoop MapReduce](https://hadoop.apache.org/). The language model treat every chinese character as a "word", thus a tri-gram contains three characters. Trained model can be used in web search and other application with no struggle.

## Compile
We use [Maven](https://maven.apache.org/) to build our project.

## Run
### Train
Suppose you have compiled the project, having a jar named lm.jar. Now run the jar with training data path `/input/train.dat`, output path `/output` and the gram window size is 3(aka tri-gram). Note your input and output path must be HDFS paths, and input text must be encoded in UTF-8.
```
$ hadoop jar lm.jar train 3 /output /input/train.dat
```
You can find the trained model in `/output/out/`.
### Test
You can also evaluate your language model with [Perplexity](https://en.wikipedia.org/wiki/Perplexity#Perplexity_per_word) metric. Suppose we want to evaluate our tri-gram model above with data in `/input/eval.dat`
```
$ hadoop jar lm.jar eval 3 /output /input/eval.dat
```
You can find the evaluate result in `/output/eval`.
### Run together
You can also run train and test all together.
```
$ hadoop jar lm.jar all 3 /output /input/train.dat /input/eval.dat
```