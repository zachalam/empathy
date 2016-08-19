# empathy
Apache spark application that parses text for emotion.

Installation
-----------------------------
Ensure that EMO_FILE is pointing to the correct location on disk.
```
EMO_FILE = "../empathy/emotion.csv"
```

Usage
-----------------------------
Simply call spark-submit with empathy.
```
time ./bin/spark-submit --master local[4] /Users/zachalam/Programming/empathy/empathy.py "I love this company. You guys provide excellent support." > OUTPUT
```

Example output
-----------------------------
Output is returned as a JSON-parsable string.
```
{"emotion": "love",
"message": "I love this company. You guys provide excellent support.",
"sentence": 1
```
* "emotion" is detected emotion.
* "message" is string empathy was called with.
* "sentence" is the sentence number giving off the strongest emotion.
