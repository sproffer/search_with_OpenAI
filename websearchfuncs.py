#!/usr/local/bin/python3
import asyncio, tiktoken, time, datetime, openai, os, sys, random, urllib.parse
import hashlib

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
from openai.embeddings_utils import get_embedding, cosine_similarity
from pandarallel import pandarallel
from commonfuncs import log, getfilenamehash
from webpagedigest import extractcontents

maxsectionlength = 2000  # about 500 tokens
minsectionoverlap = 400  # about 100 tokens
mincontentlength = 30    # indexed content has to have more than min content length
# use custom user-agent
customUA = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 GZPython3/OpenAI'}
langmodel = "gpt-3.5-turbo"

async def retrieve_webpage(url):
    try:
        session = AsyncHTMLSession()
        r = await session.get(url, headers=customUA, timeout=10000)
        ct = r.headers['Content-Type']
        if 'text/html' in ct:
            await r.html.arender(wait=5, timeout=20000)
        else:
            log(f' skip html rendering for {url=}      ', endstr='\r')
        await session.close()
        log("  retrieved  " + url[:45] + "...       ", endstr="\r")
        return r
    except Exception as err:
        print(f"FAILED to load {url} -- {err}", file=sys.stderr)
        return None

async def batchtasks(webs):
    tasks = (retrieve_webpage(url) for url in webs)
    return await asyncio.gather(*tasks)

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
                searchwebs = get_search_links(userquestion)
            else:
                raise Exception("Cannot generate embedding, missing list of webs or user question.")

        log("Read " + str(len(searchwebs)) + " webpages, render and collect contents..." + (" " * 20), endstr="\r")
        results = asyncio.run(batchtasks(searchwebs))
        log("Parse and break contents into data frames" + (" " * 20), endstr="\n")
        df = extractcontents(searchwebs, results)
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
        log("Finished embedding - hash=" + hashstr + (" " * 20))
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
        ri = random.randint(1,5)
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

# take user questions, run Bing search, and get first 2 pages of URLs, excluding ads
def get_search_links(userquestion):
    webs = []
    qstr=urllib.parse.quote(userquestion, safe='')
    srch1 = "https://www.bing.com/search?q=" + qstr + "rdr=1&first=1"
    srch2 = "https://www.bing.com/search?q=" + qstr + "rdr=1&first=2"
    srchs = [ srch1, srch2 ]
    log("run Bing search ...    ", endstr="\r")
    result = asyncio.run(batchtasks(srchs))
    contents1 = result[0].html.html
    contents2 = result[1].html.html
    try:
        log("parse search results... " + (" " * 20), endstr="\r")
        for b_algo in BeautifulSoup(contents1, 'html.parser').find("div", id="b_content").find_all("li", class_="b_algo"):
            s1 = BeautifulSoup(str(b_algo), 'html.parser')
            bhref = s1.find('a')["href"]
            webs.append(str(bhref))
        for b_algo in BeautifulSoup(contents2, 'html.parser').find("div", id="b_content").find_all("li", class_="b_algo"):
            s1 = BeautifulSoup(str(b_algo), 'html.parser')
            bhref = s1.find('a')["href"]
            webs.append(str(bhref))
    except Exception as err:
        log(f"Unexpected {err=}, {type(err)=}", sys.stderr)
    return webs[:6]

