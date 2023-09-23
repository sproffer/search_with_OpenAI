#!/usr/local/bin/python3.11
import time, sys, traceback, json
from openaifuncs import get_embedded_dataframe, get_answer
from commonfuncs import log

# redirect error to a file
sys.stderr = open('stderr.txt', 'w')

userquestion = ""
while len(userquestion.strip()) < 8:
    userquestion = input("Enter your question : \n    ")

BOLDSTART="\033[1m"
BOLDSTOP="\033[m"

try:
    df = get_embedded_dataframe(searchphrase=userquestion)
    if df is None or df.empty:
        log(BOLDSTART + "Failed to load data frame, exit!     " + BOLDSTOP, endstr="\n")
        sys.exit(1)

    while userquestion.lower() != "stop":
        answerobj = get_answer(df, userquestion, top_n=12)
        print("\n\n")
        answerstr=answerobj["answer"]
        log(BOLDSTART+"Answer:\n" + BOLDSTOP + answerstr)
        print("\n")
        if len(answerobj["references"]) > 0:
            log(BOLDSTART + "References:\n" + BOLDSTOP + json.dumps(answerobj["references"], indent=2))
        userquestion = ""
        while len(userquestion.strip()) < 3:
            userquestion = input("\nEnter your next question, if you want to stop, type " + BOLDSTART + "stop" + BOLDSTOP + ": \n    ")
        print("\n")
    log(f"{BOLDSTART}The End.{BOLDSTOP}", endstr="\n")

except Exception as err:
    log(f"Unexpected {err=}, {type(err)=}")
    traceback.print_exc(limit=5, file=sys.stderr, chain=False)
    time.sleep(2)
    sys.exit(1)


