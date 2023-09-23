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

# TEST, use smaller web pages to avoid out of memory errors
webs = ['https://www.cdc.gov/bloodpressure/facts.htm',
        'https://www.cdc.gov/bloodpressure/risk_factors.htm',
        'https://www.cdc.gov/bloodpressure/prevent.htm',
        'https://www.cdc.gov/bloodpressure/manage.htm',
        'https://www.cdc.gov/bloodpressure/medicines.htm',
        'https://www.who.int/news-room/fact-sheets/detail/hypertension',
        'https://www.who.int/news-room/questions-and-answers/item/noncommunicable-diseases-hypertension',
        'https://www.who.int/news-room/questions-and-answers/item/noncommunicable-diseases-pulmonary-hypertension',
        'https://my.clevelandclinic.org/health/diseases/4314-hypertension-high-blood-pressure',
        'https://www.medicalnewstoday.com/articles/150109',
        'https://downloads.regulations.gov/NTIA-2023-0005-0108/attachment_1.pdf']

try:
    df = get_embedded_dataframe(webs, searchphrase=userquestion)
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


