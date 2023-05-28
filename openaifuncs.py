#!/usr/local/bin/python3
import tiktoken, time, openai, os, sys, random
import pandas as pd
import numpy as np
from openai.embeddings_utils import get_embedding, cosine_similarity
from pandarallel import pandarallel
from commonfuncs import log, getfilenamehash, getasyncwebresponses
from webpagedigest import extractcontents, getbingsearchlinks

maxsectionlength =  8000 # about 2000 tokens, given 4K token limit
mincontentoverlap = 800  # about 100 words, enough to not break up a logical continuous sentence in English
ignorelength =  30   # indexed content should have more than min content length

langmodel = "gpt-3.5-turbo"

# to create data frame file with embedding in it
# either provide a list of web pages to get contents,
# or a user question that would trigger Bing search to get a list of web pages.
def get_embedded_dataframe(webs, userquestion="", usecontent=False, filename=""):
    searchwebs = webs.copy()
    embeddingfilename = filename
    if len(filename) > 5:
        fparts = filename.split("/")
        hashstr = fparts[len(fparts)-1]
    else:
        hashstr = getfilenamehash(webs, userquestion)
        embeddingfilename = "/tmp/web-" + hashstr + ".csv"

    alreadyembedded = os.path.isfile(embeddingfilename)
    if alreadyembedded == False:
        if len(searchwebs) < 1:
            if  userquestion != None and len(userquestion) > 3:
                # do bing search to get webs
                searchwebs = getbingsearchlinks(userquestion)
            else:
                raise Exception("Cannot generate embedding, missing list of webs or user question.")

        log("Load " + str(len(searchwebs)) + " webpages, render and collect contents..." + (" " * 40), endstr="\r")
        results = getasyncwebresponses(searchwebs)
        log("Parse and break contents into data frames" + (" " * 30), endstr="\r")
        df = extractcontents(searchwebs, results, maxsectionlength, ignorelength, mincontentoverlap)
        #global start_timer
        #start_timer = time.time()
        #global global_counter
        #global_counter = 0
        #log("Start generating embeddings" + (" " * 20), endstr="\r")
        #if usecontent == True:
        #    df["embedding"] = df.content.apply(lambda x: rate_limit_embeddings(x))
        #else:
        #    df["embedding"] = df.combined.apply(lambda x: rate_limit_embeddings(x))
        #log("." * (int(global_counter/20) + 3) + "  ")
        #time.sleep(1)
        log("Finished embedding - hash=" + hashstr + (" " * 40))
        df.to_csv(embeddingfilename, sep="\t")
        time.sleep(2)
        outdf = pd.read_csv(embeddingfilename, sep="\t")
        return outdf
    else:
        log("Using cached embedding data - hash=" + hashstr + (" " * 20))
        outdf = pd.read_csv(embeddingfilename, sep="\t")
        return outdf

############################################
# Text/Embedding with Rate limit,
# first 48 hours: 60 requests/min, and 250,000 tokens/min
# after 48 hours: 350 requests/min,  350,000 tokens/min
#
# use 50 requests per 10 seconds -> 300/min
rate_limit = 50
rate_period = 10
request_counter = 0
global_counter = 0
start_timer = time.time()
def rate_limit_embeddings(text, model="text-embedding-ada-002"):
    if text == None or len(text) < 2:
        return 0.0

    global rate_limit
    global rate_period
    global request_counter
    global start_timer
    global global_counter

    check_timer = time.time()
    duration = check_timer - start_timer
    if int(duration) >= rate_period:
        start_timer = time.time()
        request_counter = 0
    if request_counter >= rate_limit and int(duration) <= rate_period:
        sleep_for = rate_period - int(duration)
        log("Rate limit wait " + str(sleep_for) + " seconds ", endstr="\r")
        time.sleep(sleep_for)
        start_timer = time.time()
        request_counter = 0
    if request_counter < rate_limit:
        request_counter +=1
        global_counter +=1
    # print dots for progress, and wipe out at least next 50 characters
    numdots = int(global_counter/20) + 1
    numspaces = 1
    if numdots < 50:
        numspaces = 50 - numdots
    log("embedding " + str(global_counter) + "." * numdots + (" " * numspaces), endstr="\r")
    return get_embedding(text, engine=model)


# given input_text, search through DataFrame to find top_n similarity entries,
# return dataframe of [webpage, content, n_tokens, similarity]
def search_embedding(df, input_text, top_n=5):
    # generate embeddings for input text
    searchword = get_embedding(
        input_text,
        engine="text-embedding-ada-002"
    )

    #### compare inputed embedding with combined embeddeing, get consine similarity
    df["similarity"] = df.nparray.parallel_apply(lambda x: cosine_similarity(x, searchword))

    ####  sort similarity by decending, return top n most irrelevant results
    sdf = df.sort_values(by="similarity", ascending=False)
    psdf = sdf[["webpage", "similarity", "subject", "content", "n_tokens"]].head(top_n)
    return psdf

userq=""
def search_for_answer(row):
    global userq
    global global_counter
    promptmsg=[
        {"role": "system", "content": "Answer with Context. If the answer is not in Context, answer 'i do not know.'."},
        {"role": "system", "content": "Context : " + row["content"]},
        {"role": "user", "content": userq}
    ]
    response = openai.ChatCompletion.create(
        model=langmodel,
        messages=promptmsg,
        temperature=0.0,
        max_tokens=1000,
        n=1
    )
    log(" query "+ langmodel + " with " + str(row["n_tokens"]) + " tokens " + ("." * (global_counter * 2)), endstr="\r")
    global_counter +=1
    return row["webpage"] + "===>" + response.choices[0].message["content"]

def summarize_answer(userq, resstr):
    usercontext = resstr[:5000]   # restrict user input to about 1200 tokens

    prefixstr = ""
    temp=0.0
    systemprompt = "Please use context to answer question of " + userq
    promptmsg=[
        {"role": "system", "content": systemprompt},
        {"role": "user", "content": "Context: " +  usercontext + "   Question: " + userq}
    ]
    if len(resstr.strip()) < 10:
        #  use sarcastic tune occasionally, and raise temprature to allow some varieties
        ri = random.randint(1,3)
        temp=0.2
        prefixstr = "(OpenAI) "
        promptsuffix = ""
        if ri == 1:
            prefixstr = "(OpenAI sarc) "
            promptsuffix = ", with sarcastic tune"
        promptmsg=[
            {"role": "system", "content": "Answer with your best knowledge" + promptsuffix + "."},
            {"role": "user", "content": userq}
        ]
    response = openai.ChatCompletion.create(
        model=langmodel,
        messages=promptmsg,
        temperature=temp,
        max_tokens=1800,
        n=1
    )
    return prefixstr + response.choices[0].message["content"]

def get_answer(df, userq, top_n=5):
    global global_counter
    parallel_num=2
    if os.cpu_count() > 4:
        parallel_num = os.cpu_count() - 1
    pandarallel.initialize(progress_bar=False, nb_workers=parallel_num, verbose=0)

    # add numpy array in df, for math
    df["nparray"] = df.embedding.parallel_apply(eval).apply(np.array)

    topdf = search_embedding(df, userq, top_n)
    topgooddf = topdf.loc[topdf["similarity"] >= 0.8 ]   # only use high similarity items
    useOpenAI = ""
    if len(topgooddf.index) > 0:
        log("selected " + str(len(topgooddf.index)) + " (among " + str(len(df.index)) + ") relevant sections to generate answers...")
    else:
        log("no relevant data from your materials, use OpenAI to generate answers...")

    resultstr = ""
    refs = []
    if len(topgooddf.index) > 0:
        global_counter = 1
        answerdf = topgooddf.apply(search_for_answer, axis=1)
        for index,value in answerdf.items():
            pair = value.split("===>")
            dstr = pair[1]
            if dstr[:13] != "I do not know":
                resultstr = resultstr + dstr + " "
                if pair[0] not in refs:
                    refs.append(pair[0])

    fanswer = summarize_answer(userq, resultstr)

    answerobj = {"answer": fanswer, "references": refs}
    return answerobj


