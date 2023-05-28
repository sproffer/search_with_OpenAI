import sys, time, hashlib, asyncio
from requests_html import AsyncHTMLSession

def canonicalize(userq):
    """
    canonicalize a string, lower-cased, alpha-numeric character sequence. all other characters are stripped.

    :param userq:  a string of user question
    :return:  a canonicalized string
    """
    if userq == None or len(userq) < 2:
        return ""

    lowerq = userq.lower()
    retstr = ''.join(c for c in lowerq if c.isalnum())
    return retstr

def getFilenameHash(webpages, questionstr):
    """
    Generate a hash string for the list of webpages or user question.  The hash is the same for the same input parameters.

    :param webpages:   a list of weburls
    :param questionstr:  user question
    :return:   a hash string based on webpages or questionstr
    """
    #  for user question, allow user typing variances (extra space), still recognized as the same question
    hashstr = canonicalize(questionstr)
    if webpages != None and len(webpages) > 0:
        hashstr = canonicalize(str(webpages))

    #  string function hash(..) will yield different results in different executions, use sha512 digest
    hashstrencoded = hashstr.encode('utf-8')
    retstr = hashlib.sha512(hashstrencoded).hexdigest()[:16]
    return retstr

async def retrieveWebpage(url):
    try:
        session = AsyncHTMLSession()

        # use custom user-agent
        customUA = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 GZPython3/OpenAI'}
        # set connect timeout and read timeout, in seconds
        r = await session.get(url, headers=customUA, timeout=(2, 10.0))
        ct = r.headers['Content-Type']
        if 'text/html' in ct:
            try:
                # to be safe, wait for 1.5 seconds (default 0.2) before calling JS render,
                # and JS render timeout after 20 seconds (default infinity) to avoid JS loop or manual interaction
                # JS render will launch chrome driver, allow longer time (20 sec)
                await r.html.arender(wait=1.5, timeout=20)
            except Exception as renderErr:
                log(f'Failed to render {url[:70]}: {renderErr}, use raw content\n', outfile=sys.stderr)
        await session.close()
        log(f"retrieved  {url[:60]}" + (" " * 30), endstr="\r")
        return r
    except Exception as err:
        log(f"FAILED to load {url[:70]} -- {err}\n", outfile=sys.stderr)
        return None

async def batchTasks(webs):
    tasks = (retrieveWebpage(url) for url in webs)
    return await asyncio.gather(*tasks)

def getAsyncWebResponses(urls):
    """
    With a list of urls, asynchronously retrieve http response
    return a list of html response objects.

    :param urls:  a list of URLs
    :return:     a list of request-html response object
    """
    responses = asyncio.run(batchTasks(urls))
    return responses


def log(msg, endstr="\n", outfile=sys.stdout):
    """
    log message on outfile (default sys.stdout), with choice of overwrite previous message or start a new line.

    :param msg:   the message to be logged
    :param endstr:  if specified as \\r, overwrite previous line
    :param outfile:  output file, default is sys.stdout
    """
    currtime = time.localtime()
    current_time = time.strftime("%H:%M:%S", currtime)
    print(current_time + " - " + msg, end=endstr, flush=True, file=outfile)