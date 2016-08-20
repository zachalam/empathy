# empathy
The goal of empathy is simple: Improve customer happiness by categorizing support tickets based on their emotion.

When a customer submits a support ticket through your site: run the text through empathy and save the emotion. Use this emotion to find and respond to unhappy customers (prioritize "sadness" over "happy" tickets) faster.

Installation
-----------------------------
In empathy.py - Ensure that the EMO_FILE constant is pointing to the "emotion.csv" location on disk.
```
EMO_FILE = "../empathy/emo.csv"
```

Usage
-----------------------------
Simply call spark-submit with empathy. Pass the text to be analyzed as a second parameter.
```
time ./bin/spark-submit --master local[4] /path/to/empathy/empathy.py "I love this company. You guys provide excellent support." > OUTPUT
```

Example output
-----------------------------
Output is returned as a JSON-parsable string.

Example Call: I love this company. You guys provide excellent support.
```
{"emotion": "love",
"message": "I love this company. You guys provide excellent support.",
"sentence": 1}
```
Emotion: love


Example Call: The support at this company isn't good. I'm very unhappy. I won't be ordering from you again.
```
{"emotion": "sadness",
"message": "The support at this company isn't good. I'm very unhappy. I won't be ordering from you again.",
"sentence": 3}
```
Emotion: sadness

* "emotion" is detected emotion.
* "message" is string empathy was called with.
* "sentence" is the sentence number giving off the strongest emotion.


Data Source
-----------------------------
[Sentiment Analysis: Emotion in Text
](https://www.crowdflower.com/wp-content/uploads/2016/07/text_emotion.csv) -
In a variation on the popular task of sentiment analysis, this dataset contains labels for the emotional content (such as happiness, sadness, and anger) of texts. Hundreds to thousands of examples across 13 labels. (crowdflower.com)
