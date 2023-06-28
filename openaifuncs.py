import traceback

import tiktoken, time, openai, os, sys, random
import pandas as pd
import numpy as np
from openai.embeddings_utils import cosine_similarity
from pandarallel import pandarallel
from commonfuncs import log, getFilenameHash, getAsyncWebResponses
from webpagedigest import extractWebContents, getBingSearchLinks

# OpenAI model and chunk size
maxcompletiontokens = 4000  # 4,000 tokens for completion, given inputs taking up-to 10,000 tokens
maxsectionlength =  24000   # approximitate 6k tokens, given embedding size limit of 8191 tokens, some single chars created more tokens.
mincontentoverlap = 800     # about 100 words, enough to not break up a logical continuous sentence in English
ignorelength =  30          # indexed content should have more than min content length
maxpromptsize = 40000       # about 10k tokens for user input/prompt size

lang_model = "gpt-3.5-turbo-16k"
embedding_model="text-embedding-ada-002"
embedding_encoding="cl100k_base"  # this the encoding for text-embedding-ada-002
def get_embedded_dataframe(webs=[], searchphrase="", filename=""):
    """
    From a user question, or a list of web URLs, retrieve web contents
    and then put into a dataframe, along with OpenAI embedding

    :param webs:    a list of web URLs; use either webs or userquestion, but not both
    :param searchphrase:   search phrase that would trigger Bing search to get a list of web URLs
    :param filename:   dataframe filename, as processed data store. If the file exists, the file content is returned.
    :return:  a dataframe with OpenAI embedding:  columns=['webpage', 'subject', 'content', 'combined', 'embedding']
    """
    searchwebs = webs.copy()
    embeddingfilename = filename
    if len(filename) > 5:
        fparts = filename.split("/")
        hashstr = fparts[len(fparts)-1]
    else:
        hashstr = getFilenameHash(webs, searchphrase)
        embeddingfilename = "/tmp/web-" + hashstr + ".csv"

    alreadyembedded = os.path.isfile(embeddingfilename)
    if alreadyembedded == False:
        if len(searchwebs) < 1:
            if  searchphrase != None and len(searchphrase) > 3:
                # do bing search to get webs
                searchwebs = getBingSearchLinks(searchphrase, numresults=16)
            else:
                raise Exception("Cannot generate embedding, missing list of webs or user question.")

        log("Load " + str(len(searchwebs)) + " webpages, render and collect contents..." + (" " * 40), endstr="\r")
        results = getAsyncWebResponses(searchwebs)
        log("Parse and break contents into data frames" + (" " * 30), endstr="\r")
        df = extractWebContents(searchwebs, results, maxsectionlength, ignorelength, mincontentoverlap)
        global start_timer
        start_timer = time.time()
        global global_counter
        global_counter = 0
        log("Start generating embeddings" + (" " * 20), endstr="\r")
        df["embedding"] = df.combined.apply(lambda x: rate_limit_embeddings(x))
        time.sleep(0.5)
        #  count number of tokens, put into second column
        #
        # embedding model parameters
        encoding = tiktoken.get_encoding(embedding_encoding)
        df["n_tokens"] = df.combined.apply(lambda x: len(encoding.encode(x)))
        time.sleep(1)
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
# OpenAI Rate Limit
#      text-embebbing-ada-002   3,000 RPM  1,000,000 TPM
#      gpt-3.5-turbo-16k        3,500 RPM,   180,000 TPM
#
#  Assuming each request has 6k tokens (worst case):
#  Set embedding limits:  25 requests per 10 seconds:
#    150 RPM
#    900,000 TPM
#  Set completion limits:  5 requests per 10 seconds:
#    30 RPM
#    180,000 TPM
#
embedding_rate_limit = 25
completion_rate_limit = 5
rate_period = 10
request_counter = 0
global_counter = 0
start_timer = time.time()

def rate_limit_control(rate_limit, time_period):
    """
    This function will keep track of global counters for API calls, and pause appropriately if the rate limit exceeded.
    this function should be put before OpenAI API calls.
    :param rate_limit: the number of requests limitation
    :param rate_period:    the time period, in seconds, for the number of requests (limit)
    """
    global request_counter
    global start_timer

    check_timer = time.time()
    duration = check_timer - start_timer
    if int(duration) >= time_period:
        start_timer = time.time()
        request_counter = 0
    if request_counter >= rate_limit and int(duration) <= time_period:
        sleep_for = time_period - int(duration) + 1
        log("Rate limit wait " + str(sleep_for) + " seconds      ", endstr="\r")
        time.sleep(sleep_for)
        start_timer = time.time()
        request_counter = 0
    if request_counter < rate_limit:
        request_counter +=1

def get_embedding_timeout(text: str, engine: str, timeout=10):
    """
    return embedding list, with timeout.
    :param text:    the text to be embedded
    :param engine:  should be text-embebbing-ada-002
    :param timeout: request timeout, default 10
    :return: array of embedding codes
    """
    embedding = openai.Embedding.create(
        input=text, model=engine, request_timeout=timeout
    )["data"][0]["embedding"]
    return embedding

def rate_limit_embeddings(text, model=embedding_model):
    if text == None or len(text) < 2:
        return 0.0

    global global_counter
    try:
        rate_limit_control(embedding_rate_limit, rate_period)

        # print dots for progress, and wipe out at least next 65 characters
        global_counter +=1
        numdots = int(global_counter/10) + 1
        numspaces = 10
        if numdots < 50:
            numspaces = 60 - numdots
        log("embedding " + str(global_counter) + "   " + ("." * numdots) + (" " * numspaces), endstr="\r")
        return get_embedding_timeout(text, model)
    except Exception as err:
        log(f"FAILED to embed {text[:60]} with length={len(text)} -- {err=}", endstr="\n", outfile=sys.stdout)
        traceback.print_stack(limit=6, file=sys.stderr)
        return 0.0

# given input_text, search through DataFrame to find top_n similarity entries,
# return dataframe of [webpage, content, n_tokens, similarity]
def search_embedding(df, input_text, top_n=5):
    # generate embeddings for input text
    searchword = get_embedding_timeout(
        input_text,
        embedding_model
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
    log("Query "+ lang_model + " " + str(row["n_tokens"]) + " tokens; Context: \033[1m" + row["content"][:50] + "\033[m" + ("." * (global_counter * 2)),
        endstr="\r")
    response = None

    c = 0;
    while (response == None) and (c < 3):
        try:
            rate_limit_control(completion_rate_limit, rate_period);
            response = openai.ChatCompletion.create(
                model=lang_model,
                messages=promptmsg,
                temperature=0.0,
                max_tokens=maxcompletiontokens,
                n=1,
                request_timeout=40
            )
        except Exception as ex:
            c = c + 1
            log(f" failed to query {lang_model} with {ex}; sleep {(c * 4)} seconds and do again", endstr="\n")
            traceback.print_stack(limit=6, file=sys.stderr)
            time.sleep(c * 4)
            response = None
    global_counter +=1
    if response == None:
        return row["webpage"] + "===>" + "None"
    return row["webpage"] + "===>" + response.choices[0].message["content"]

def summarize_answer(userq, resstr):
    usercontextsize = maxpromptsize - 400  # rough estimate to restrict total input size
    usercontext = resstr[:usercontextsize]

    prefixstr = ""
    temp=0.0
    systemprompt = "Use provided context to answer question, provided context:  " + usercontext
    promptmsg=[
        {"role": "system", "content": systemprompt},
        {"role": "user", "content": "Question: " + userq}
    ]
    if len(resstr.strip()) < 10:
        #  use sarcastic tune occasionally, and raise temperature to allow some varieties
        ri = random.randint(1,4)
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
    try:
        rate_limit_control(completion_rate_limit, rate_period);
        response = openai.ChatCompletion.create(
            model=lang_model,
            messages=promptmsg,
            temperature=temp,
            max_tokens=maxcompletiontokens,
            n=1,
            request_timeout=40
        )
    except Exception as ex:
        log(f" Failed to summarize answer with {ex}", endstr="\n")
        traceback.print_stack(limit=6, file=sys.stderr)
        return "ERROR "
    return prefixstr + response.choices[0].message["content"]

def get_answer(df, userq, top_n=6):
    global global_counter
    parallel_num=2
    if os.cpu_count() > 4:
        parallel_num = os.cpu_count() - 1
    pandarallel.initialize(progress_bar=False, nb_workers=parallel_num, verbose=0)

    # add numpy array in df, for math
    df["nparray"] = df.embedding.parallel_apply(eval).apply(np.array)

    topdf = search_embedding(df, userq, top_n)
    topgooddf = topdf.loc[topdf["similarity"] >= 0.8 ]   # only use high similarity items
    if len(topgooddf.index) > 0:
        log("selected " + str(len(topgooddf.index)) + " (among " + str(len(df.index)) + ") most relevant sections to generate answers...", endstr="\r")
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


