# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""empathy.py"""
from pyspark import SparkContext
import sys, unicodedata, string, json

# init a few constants
# ----------------------------
# used with sys.argv to determine which element contains our message
PARAM_MSG = 1
# location of emotions.csv file on system
EMO_FILE = "../empathy/emotion.csv"
# column header positions for emotions.csv
EMO_TWEETID = 0
EMO_EMOTION = 1
EMO_AUTHOR = 2
EMO_MESSAGE = 3


# define function calls
# ----------------------------
# remove punctuation, extra whitespace from string
def clean_string(the_string):
    #print the_string
    # remove punctuation and special chars
    the_string = the_string.translate(string.maketrans("",""), string.punctuation)
    # remove whitespace
    the_string = " ".join(the_string.split())
    # lowercase string
    return the_string.lower()

def clean_unicode(the_string):
    the_string = unicodedata.normalize('NFKD', the_string).encode('ascii','ignore')
    return clean_string(the_string)

# clean up a line in the RDD
def clean_rdd_line(the_line):
    line = the_line.split(",")
    return line[EMO_TWEETID] + ',' + clean_unicode(line[EMO_EMOTION]) + ',' \
    + line[EMO_AUTHOR] + ',' + clean_unicode(line[EMO_MESSAGE])

# return the highest scoring emotion in array.
def highest_score(emotions,sentence_num):
    # keep track of the highest score.
    highest_score = 0
    highest_emotion = [u'unknown',0,0]
    # check each emotion score one at a time.
    for emotion in emotions:
        the_emotion = emotion[0]
        the_score = emotion[1]

        # if emotions array provided a sentence number use it
        if len(emotion) >= 3: the_sentence = emotion[2]
        else: the_sentence = sentence_num

        # we found a higher scoring emotion, save it!
        if the_score > highest_score:
            highest_score = the_score
            highest_emotion = [the_emotion,the_score,the_sentence]

    # return the highest scoring emotion
    return highest_emotion


# verify script called correctly
# ----------------------------
if len(sys.argv) != 2:
    print("Error! Usage: empathy <message>")
    exit(-1)


# init SparkContext & load emotions as RDD
# ----------------------------
sc = SparkContext("local", "empathy")
emo_rdd = sc.textFile(EMO_FILE).cache()


# remove quotes and clean up text in RDD
# ----------------------------
emo_rdd = emo_rdd.map(clean_rdd_line)


# begin message analysis
# ----------------------------
# obtain message from command
message = sys.argv[PARAM_MSG]
# break apart each sentence in message
sentences = message.split(".")

# how many words to group together
span = 3
# keep track of emotions in each sentence
emotion_scores = []
# keep track of each sentence
current_sentence = 1

for sentence in sentences:
    # clean setence of punctuation/extra chars.
    sentence = clean_string(sentence)
    # split apart message string into individual words
    words = sentence.split(" ")

    # only analyze sentence with at least 2 words
    if len(words) >= span:

        # combine words into groups of length "span".
        # given message = "how are you doing today"
        # mini_messages = ['how are you, 'are you doing', 'you doing today']
        mini_messages = [" ".join(words[i:i+span]) \
        for i in range(0,len(words)-span+1,1)]

        # build filtered rdd with these matching mini_messages
        messages_rdd = emo_rdd.filter(lambda line: \
        any(msg in line.split(",")[EMO_MESSAGE] for msg in mini_messages))
        #messages_rdd.saveAsTextFile(EMO_FILE + "2")

        # sum up scores in this sentence
        messages_rdd = messages_rdd.map(lambda line: line.split(",")[EMO_EMOTION])
        # get a score for each emotion
        emotions = messages_rdd.countByValue().items()

        # DEBUG Emotions var
        # print emotions

        # if no emotions do nothing this round
        if emotions:

            # save all emotion scores in array
            highest_emotion = highest_score(emotions,current_sentence)

            # save the highest scoring emotion in this sentence.
            emotion_scores.append(highest_emotion)
            #print highest_emotion

    # get ready for next sentence
    current_sentence += 1

# finalize strongest emotion
# ----------------------------
highscore = highest_score(emotion_scores,[])
emotion = clean_unicode(highscore[0])
sentence = highscore[2]

# output final result as JSON string
print json.dumps({'emotion': emotion, 'sentence': sentence, 'message': message})
