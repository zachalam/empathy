# empathy
The goal of empathy is simple: Improve customer happiness by categorizing support tickets based on their emotion.

When a customer submits a support ticket through your site, run the text through empathy and save the emotion. Use this emotion to find and respond to unhappy customers (who are giving off "sadness" or "hate") faster. 

Installation
-----------------------------
In empathy.py - Ensure that the EMO_FILE constant is pointing to the "emotion.csv" location on disk.
```
EMO_FILE = "../empathy/emotion.csv"
```

Usage
-----------------------------
Simply call spark-submit with empathy. Pass the text to be analyzed as a second parameter.
```
time ./bin/spark-submit --master local[4] /Users/zachalam/Programming/empathy/empathy.py "I love this company. You guys provide excellent support." > OUTPUT
```

Example output
-----------------------------
Output is returned as a JSON-parsable string.

Called with text: I love this company. You guys provide excellent support.
```
{"emotion": "love",
"message": "I love this company. You guys provide excellent support.", "sentence": 1}
```
Emotion: love


Called with text: The support at this company isn't good. I'm very unhappy. I won't be ordering from you again.
```
{"emotion": "sadness",
"message": "The support at this company isn't good. I'm very unhappy. I won't be ordering from you again.", "sentence": 3}
```
Emotion: sadness

* "emotion" is detected emotion.
* "message" is string empathy was called with.
* "sentence" is the sentence number giving off the strongest emotion.
