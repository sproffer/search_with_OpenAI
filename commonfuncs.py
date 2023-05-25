import sys, time, hashlib

def canonicalize(userq):
    """"
    canonicalize a string, lower-cased, alpha-numeric character sequence. all other characters are stripped.
    :param userq:  a string of user question
    :return:  a canonicalized string
    """
    if userq == None or len(userq) < 2:
        return ""

    lowerq = userq.lower()
    retstr = ''.join(c for c in lowerq if c.isalnum())
    return retstr

def getfilenamehash(webpages, questionstr):
    """
    Generate a filename which is consistent for the list of webpages or user question
    :param webpages:   a list of weburls
    :param questionstr:  user question
    :return:   a hash string based on webpages or questionstr
    """
    #  for user question, allow user typing variances (extra space), still recognized as the same question
    hashstr = canonicalize(questionstr)
    if webpages != None and len(webpages) > 0:
        hashstr = str(webpages)

    #  string function hash(..) will yield different results in different executions, so use sha512 digest
    hashstrencoded = hashstr.encode('utf-8')
    retstr = hashlib.sha512(hashstrencoded).hexdigest()[:16]
    log(" embedding hash: " + retstr + (" " * 10), "\r")
    return retstr


def log(msg, endstr="\n", outfile=sys.stdout):
    """
    log message on outfile (default sys.stdout)
    :param msg:   the message to be logged
    :param endstr:   '\r' or '\n' (default), for overwriting current line or start a new line
    :param outfile:  output file, default is sys.stdout
    """
    currtime = time.localtime()
    current_time = time.strftime("%H:%M:%S", currtime)
    print(current_time + " - " + msg, end=endstr, flush=True, file=outfile)