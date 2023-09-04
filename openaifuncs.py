import traceback, tiktoken, time, openai, os, sys, random, json
import pandas as pd
import numpy as np
from openai.embeddings_utils import cosine_similarity
from pandarallel import pandarallel
from commonfuncs import log, getFilenameHash, getAsyncWebResponses
from webpagedigest import extractWebContents, getBingSearchLinks

#########################################
#  OpenAI model and chunk size
#
lang_model = "gpt-3.5-turbo-16k"
maxprompttokens     = 8000    # 8,000 tokens for user input/prompt size
maxcompletiontokens = 8000    # 8,000 tokens for completion, given inputs taking up-to 10,000 tokens
maxsectionlength    = 20000   # length in char, single letter words or non-ASCII chars with more bytes could still exceed 8191 tokens.
#lang_model = 'gpt-4'   # 8k model
#maxprompttokens     = 4000   # max 8,000 tokens, including prompt and completion
#maxcompletiontokens = 4000   # 4,000 tokens for completion only
#maxsectionlength    = 12000  # in char, approximate 3k tokens, some single chars created more tokens.

mincontentoverlap = 400     # in char, about 50 words, enough to not break up a logical continuous sentence
ignorelength =  30          # indexed content should have more than min content length
embedding_model="text-embedding-ada-002"
embedding_encoding="cl100k_base"  # this the encoding for text-embedding-ada-002

############################################
# OpenAI Rate Limit
#      text-embebbing-ada-002   3,000 RPM  1,000,000 TPM
#      gpt-3.5-turbo-16k        3,500 RPM,   180,000 TPM
#      gpt-4                      200 RPM,    10,000 TPM
#
#  set embedding limit:
#      150,000 tokens per 10 seconds
#  Set completion limits.
#      gpt-3.5-turbo-16k:  30,000 tokens per 10 seconds
#      gpt-4 :  5,000 tokens per 30 seconds
#
embedding_token_limit   = 150000
embedding_token_counter = 0
embedding_start_timer = time.time()

completion_token_limit   = 30000
completion_token_counter = 0
completion_start_timer = time.time()

progress_counter = 0
rate_period = 10

def embedding_rate_limit_control(rate_period, curr_tokens):
    """
    This function will keep track of token count for embedding calls
        and pause appropriately if the rate limit exceeded.
    this function should be put before OpenAI API calls.
    :param rate_period:    the time period, in seconds, for the number of requests (limit)
    :param curr_tokens:   thenumber of tokens for current request
    """
    global embedding_token_limit
    global embedding_token_counter
    global embedding_start_timer

    check_timer = time.time()
    duration = check_timer - embedding_start_timer
    if int(duration) >= rate_period:
        embedding_start_timer = time.time()
        embedding_token_counter = curr_tokens
    else:
        embedding_token_counter += curr_tokens
        if embedding_token_limit < embedding_token_counter :
            sleep_for = rate_period - int(duration) + 1
            log(f"Rate limit wait {str(sleep_for)} seconds, , after accumulative {embedding_token_counter=}      ", endstr="\r")
            time.sleep(sleep_for)
            embedding_start_timer = time.time()
            embedding_token_counter = curr_tokens

def completion_rate_limit_control(rate_period, curr_tokens):
    """
    This function will keep track of token count for completion calls
        and pause appropriately if the rate limit exceeded.
    this function should be put before OpenAI API calls.
    :param rate_period:    the time period, in seconds, for the number of requests (limit)
    :param curr_tokens:   thenumber of tokens for current request
    """
    global completion_token_limit
    global completion_token_counter
    global completion_start_timer

    check_timer = time.time()
    duration = check_timer - completion_start_timer
    if int(duration) >= rate_period:
        completion_start_timer = time.time()
        completion_token_counter = curr_tokens
    else:
        completion_token_counter += curr_tokens
        if completion_token_limit < completion_token_counter :
            sleep_for = rate_period - int(duration) + 1
            log(f"Rate limit wait {str(sleep_for)} seconds, , after accumulative {completion_token_counter=}      ", endstr="\r")
            time.sleep(sleep_for)
            completion_start_timer = time.time()
            completion_token_counter = curr_tokens

def tokenCount(inputstr):
    encodingFunc = tiktoken.get_encoding("cl100k_base")
    return len(encodingFunc.encode(inputstr))

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
        global progress_counter
        progress_counter = 0
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

    global progress_counter
    try:
        curr_tokens_num = tokenCount(text)
        embedding_rate_limit_control(rate_period, curr_tokens_num)

        # print dots for progress, and wipe out at least next 65 characters
        progress_counter +=1
        numdots = int(progress_counter/10) + 1
        numspaces = 10
        if numdots < 60:
            numspaces = 70 - numdots
        log("embedding " + str(progress_counter) + "   " + ("." * numdots) + (" " * numspaces), endstr="\r")
        return get_embedding_timeout(text, model)
    except Exception as err:
        log(f"FAILED to embed {text[:80]} with length={len(text)} -- {err=}", endstr="\n", outfile=sys.stdout)
        traceback.print_stack(limit=6, file=sys.stderr)
        log(f"Use first 10k char to embed ignore text ...{text[10001:]}", endstr="\n", outfile=sys.stderr)
        return get_embedding_timeout(text[:10000], embedding_model)

# given input_text, search through DataFrame to find top_n similarity entries,
# return dataframe of [webpage, content, n_tokens, similarity]
def search_embedding(df, input_text, top_n=5):
    # generate embeddings for input text
    curr_tokens_num = tokenCount(input_text)
    embedding_rate_limit_control(rate_period, curr_tokens_num)
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
    global progress_counter
    promptmsg=[
        {"role": "system", "content": "Answer with Context. If the answer is not in Context, answer 'i do not know.'."},
        {"role": "system", "content": "Context : " + row["content"]},
        {"role": "user", "content": userq}
    ]
    log("Query "+ lang_model + " " + str(row["n_tokens"]) + " tokens; Context: \033[1m" + row["content"][:60] + "\033[m" + ("." * (progress_counter * 2)),
        endstr="\r")
    response = None
    prompt_tokens = tokenCount(json.dumps(promptmsg))

    c = 0;
    while (response == None) and (c < 3):
        try:
            completion_rate_limit_control(rate_period, prompt_tokens);
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
            log(f" failed to query {lang_model} with {ex}; sleep {(c * 5)} seconds and do again", endstr="\n")
            traceback.print_stack(limit=6, file=sys.stderr)
            time.sleep(c * 5)
            response = None
    progress_counter +=1
    if response == None:
        return row["webpage"] + "===>" + "None"
    return row["webpage"] + "===>" + response.choices[0].message["content"]

def summarize_answer(userq, syspromptstr, timeout=40):
    syscontext = syspromptstr
    num_tokens = tokenCount(userq + syspromptstr) + 50;
    if (num_tokens > maxprompttokens):
        syscontext = syspromptstr[:30000]
        log(f"Truncate system prompt, with {num_tokens=} too many. Result {syscontext=}.")
    prefixstr = ""
    temp=0.0
    systemprompt = "Use provided context to answer question, provided context:  " + syscontext
    promptmsg=[
        {"role": "system", "content": systemprompt},
        {"role": "user", "content": "Question: " + userq}
    ]
    if len(syspromptstr.strip()) < 10:
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
        completion_rate_limit_control(rate_period, num_tokens);
        response = openai.ChatCompletion.create(
            model=lang_model,
            messages=promptmsg,
            temperature=temp,
            max_tokens=maxcompletiontokens,
            n=1,
            request_timeout=timeout
        )
    except Exception as ex:
        log(f" Failed to summarize answer with {ex}", endstr="\n")
        traceback.print_stack(limit=6, file=sys.stderr)
        return "ERROR "
    return prefixstr + response.choices[0].message["content"]

def get_answer(df, userq, top_n=6):
    global progress_counter
    progress_counter = 1

    parallel_num=2
    if os.cpu_count() > 2:
        parallel_num = os.cpu_count() - 1
    pandarallel.initialize(progress_bar=False, nb_workers=parallel_num, verbose=0)

    # add numpy array in df, for math
    df["nparray"] = df.embedding.parallel_apply(eval).apply(np.array)

    log(f"search embedding ... {userq=}            ", endstr="\r")
    topdf = search_embedding(df, userq, top_n)
    topgooddf = topdf.loc[topdf["similarity"] >= 0.8 ]   # only use high similarity items
    if len(topgooddf.index) > 0:
        log("selected " + str(len(topgooddf.index)) + " (among " + str(len(df.index)) + ") most relevant sections to generate answers...", endstr="\r")
    else:
        log("no relevant data from your materials, use OpenAI to generate answers...")

    resultstr = ""
    refs = []
    if len(topgooddf.index) > 0:
        answerdf = topgooddf.apply(search_for_answer, axis=1)
        for index,value in answerdf.items():
            pair = value.split("===>")
            dstr = pair[1]
            if dstr[:13] != "I do not know":
                resultstr = resultstr + dstr + " "
                if pair[0] not in refs:
                    refs.append(pair[0])
    log(f'calling sumarize with:  {resultstr[:60]}....            ', endstr="\r")
    fanswer = summarize_answer(userq, resultstr)

    answerobj = {"answer": fanswer, "references": refs}
    return answerobj


