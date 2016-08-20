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
from pyspark import SparkConf, SparkContext
import sys, unicodedata, string, json

# init a few constants
# ----------------------------
# used with sys.argv to access incoming data params.
PARAM_MSG = 1
PARAM_EMO = 2
# number of parameters called to analyze
PARAM_COUNT_ANALYZE = 2

# location of emotions.csv file on system
EMO_FILE = "../empathy/emotions.csv"
# column header positions for emotions.csv
EMO_EMOTION = 0
EMO_MESSAGE = 1
# the number of words in a matching group
# higher (more specific, less results)
MATCH_SIZE = 3
# number of partions for RDD (hard storage)
# increase number for increased efficiency.
RDD_PARTITIONS = 1

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
    return clean_unicode(line[EMO_EMOTION]) + ',' \
    + clean_unicode(line[EMO_MESSAGE])

# return the highest scoring emotion in array.
def highest_score(emotions,sentence_num):
    # keep track of the highest score.
    highest_score = 0
    highest_emotion = [u'unknown',0,0]
    # check each emotion score one at a time.
    for emotion in emotions:
        the_emotion = emotion[0]
        the_score = emotion[1]

        # if emotions array provided a sentence number: resave it
        if len(emotion) >= 3: the_sentence = emotion[2]
        else: the_sentence = sentence_num

        # we found a higher scoring emotion, save it!
        if the_score > highest_score:
            highest_score = the_score
            highest_emotion = [the_emotion,the_score,the_sentence]

    # return the highest scoring emotion
    return highest_emotion

# main programming function - teach empathy new emotion
# by adding it to the emo.csv RDD
# emo_rdd - spark RDD (RDD)
# message - message from user, command line (string)
# emotion - emotion to teach it (string)
def empathy_teach(emo_rdd,message,emotion,sc):
    newline = sc.parallelize([emotion+","+message])
    new_rdd = newline.union(emo_rdd)
    new_rdd.coalesce(RDD_PARTITIONS).saveAsTextFile(EMO_FILE)

    #output empathy taught JSON response
    print json.dumps({'emotion': emotion, 'status': 'accepted'})


# main programming function - analyze text for emotion
# by comparing it to matching RDD strings
# emo_rdd - spark RDD (RDD)
# message - message from user, command line (string)
def empathy_analyze(emo_rdd,message):
    # break apart each sentence in message
    sentences = message.split(".")

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
        if len(words) >= MATCH_SIZE:

            # combine words into groups of length "MATCH_SIZE".
            # given message = "how are you doing today"
            # mini_messages = ['how are you, 'are you doing', 'you doing today']
            mini_messages = [" ".join(words[i:i+MATCH_SIZE]) \
            for i in range(0,len(words)-MATCH_SIZE+1,1)]

            # build filtered rdd with these matching mini_messages
            # split line (line.split) at most 1 time (second param)
            messages_rdd = emo_rdd.filter(lambda line: \
            any(msg in line.split(",",1)[EMO_MESSAGE] for msg in mini_messages))
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
    emotion = highscore[0]
    sentence = highscore[2]

    # output final result as JSON string
    print json.dumps({'emotion': emotion, 'sentence': sentence, 'message': message})


# ----------------------------
# ############################
# ############################
# ############################
# ----------------------------


# verify script called correctly
# ----------------------------
if len(sys.argv) < PARAM_COUNT_ANALYZE:
    print("Error! Usage: empathy <message> <emotion(optional)>")
    exit(-1)


# init SparkContext & load emotions as RDD
# ----------------------------
sparkConf = SparkConf() \
             .setMaster("local[4]") \
             .setAppName("empathy") \
             .set("spark.hadoop.validateOutputSpecs", "false")

sc = SparkContext(conf=sparkConf)
emo_rdd = sc.textFile(EMO_FILE).cache()


# remove quotes and clean up text in RDD
# ----------------------------
emo_rdd = emo_rdd.map(clean_rdd_line)


# obtain message from user.
# ----------------------------
# obtain message from command
message = sys.argv[PARAM_MSG]


# analyze message OR teach empathy new tricks
# depending on how many params were sent.
if len(sys.argv) == PARAM_COUNT_ANALYZE:
    # okay, analyze
    empathy_analyze(emo_rdd,message)
else:
    # okay, teach
    emotion = sys.argv[PARAM_EMO]
    empathy_teach(emo_rdd,message,emotion,sc)
