#
#  Parse HTML or PDF web contents and put into Pandas DataFrame
#
#  package installed: 
#     /usr/local/bin/python3 -m pip install py-pdf-parser beautifulsoup4
#
import os

from py_pdf_parser.loaders import load_file
import pandas as pd
from bs4 import BeautifulSoup
from commonfuncs import log, getAsyncWebResponses
import sys, traceback, urllib.parse, os, threading
import concurrent.futures as cf

def updateHeaderRow(df, sizenum, lengthnum):
    if df.loc[(df['font_size'] == sizenum)].any().all():
        if df.loc[ (df['font_size'] == sizenum) & (df['length'] < lengthnum) ].any().all():
            df.loc[df.font_size == sizenum, 'length'] = lengthnum

        c = df.loc[df.font_size == sizenum, 'count']
        c += 1
        df.loc[df.font_size == sizenum, 'count'] = c
    else:
        df.loc[len(df.index)] = [sizenum, lengthnum, 1]

def combinedlen(row):
    return row['length'] * row['count']

def buildPdfHeaderMapping(pdfdoc, headermaxlen, ignorelen, ignorecombinedlen):
    """
    Parse a PDF document to establish header mapping, based on font size.
    the returned dataframe has mapping of font_size and typecol (h1, h2, h3, text),
    all other contents should be discarded.

    :param pdfdoc:   the PDF document
    :param headermaxlen:  max length (# char) that can be considered as header
    :param ignorelen:     any non-header section less than this is discarded, to ignore such things as page numbers
    :param ignorecombinedlen:  any non-header section with combined length less than this, is discarded
    :return:   dataframe columns=['font_size', 'length', 'count', 'combined_len', 'typecol']
    """

    headerdf = pd.DataFrame(None, columns=['font_size', 'length', 'count'])
    elementlist = pdfdoc.elements;
    for anelem in elementlist:
        fs = int(anelem.font_size)
        tl = len(anelem.text())
        updateHeaderRow(headerdf, fs, tl)

    sortdf = headerdf.sort_values(by="font_size", ascending=False)
    sortdf = sortdf.reset_index(drop=True)
    sortdf['combined_len'] = sortdf.apply(combinedlen, axis=1)
    stoph = False
    typecol = ['h1']
    for i in sortdf.index:
        if i == 0:
            continue
        elif i == 1:
            if sortdf.loc[i].length <= headermaxlen:
                typecol.append('h2')
            else:
                typecol.append('text')
                stoph = True
        elif (i == 2) & (stoph == False) :
            if sortdf.loc[i].length <= headermaxlen:
                typecol.append('h3')
            else:
                typecol.append('text')
        else:
            typecol.append('text')
    sortdf['typecol'] = typecol
    # drop 'text' rows max length is ignorelen, page no etc.
    filterdf = sortdf.loc[ (sortdf['length'] > ignorelen) | (sortdf['typecol'] != 'text') ]
    # drop 'text' rows that length * count < ignorecombinedlen, insignificant
    filterdf = filterdf.loc[ (filterdf['combined_len'] > ignorecombinedlen) | (filterdf['typecol'] != 'text')]
    return(filterdf)

#  from fontsize, map to type of section
#  return value could be h1, h2, h3, text, ignore
def headermap(fs, headerdf):
    returntyp = 'ignore'
    if headerdf.loc[ headerdf['font_size'] == fs ].any().all():
        returntyp = headerdf.loc[ headerdf.font_size == fs, 'typecol'].squeeze()
    return(returntyp)

def concatstrings(strarray):
    retstring = ''
    if strarray != None:
        for astr in strarray:
            retstring += " ".join(astr.split()) + " "
    return retstring

def addrows(df, weburl, subjectstr, contentstr, maxcontentlength, ignorelength, mincontentoverlap):
    """
    add rows to dataframe, break contents into multiple rows if exceeding max number of tokens for GTP3

    :param df:    dataframe to add rows to
    :param weburl:  web url
    :param subjectstr:   Subject column
    :param contentstr:   Contents
    :param maxcontentlength:  max # of chars, longer contents will be broken into multiple
    :param ignorelength:      ignore short content, in chars
    :param mincontentoverlap: requires minimum # of chars overlap when breaking up contents
    :return:   dataframe with added rows
    """
    if (contentstr != None) and (len(contentstr) > ignorelength):
        contents = splitstring(contentstr, maxcontentlength, mincontentoverlap)
        for acontentstr in contents:
            combinestr = "Title: " + subjectstr + "; Content: " + acontentstr
            df.loc[len(df.index)] = [weburl, subjectstr, acontentstr, combinestr]
    return df

#   a function to split a string into array of strings
#   each with max length, and between strings with minimum overlap, in characters
#   safety check:
#       max length should be greater than minimum overlap by more than 20 chars
def splitstring(nStr, maxLen=8000, minOverlap=200):
    if maxLen < (minOverlap * 4):
        raise Exception("max length (" + str(maxLen) +") should be greater than minimum overlap ("+ str(minOverlap) + ") by more than 4 times")
    retList = []
    while len(nStr.encode('utf-8')) > maxLen:
        p1 = maxLen - 1
        encodelonger = False
        while p1 > (maxLen - minOverlap * 2) and encodelonger == False:
            ## safety check for non-ascii
            if len(nStr[0:p1-1].encode('utf-8')) > maxLen:
                p1 = int(p1 / 2) + 2
                log(f"splitstring, non-ascii makes more bytes, split at half: {nStr[0:120]}....      ", endstr="\r")
                encodelonger = True
            elif nStr[(p1-2):p1] == '. ':
                retList.append(nStr[0:p1-1].strip())
            elif p1 < maxLen - minOverlap and nStr[p1] == ' ':
                # after progressing an overlap length, space for break is also OK
                retList.append(nStr[0:p1].strip())
            else:
                p1 = p1 - 1

        if encodelonger == True:
            #  hard cut-off, after moving back twice minOverlap position, should not happen
            if p1 > 30 and len(nStr) > p1 + 25:
                log(f'Break at an unnatural place, with "{nStr[(p1-25):p1]}"|"{nStr[p1:(p1+25)]}" ==> {minOverlap=} is too small.', outfile=sys.stderr)
            retList.append(nStr[0:p1].strip())
        p2 = p1 - minOverlap
        if (p2 < 0):
            nStr = nStr[1:]
        else:
            while p2 > 4 and nStr[p2] != ' ':
                p2 = p2 -1
            nStr = nStr[p2:]

    retList.append(nStr.strip())
    return retList

def parsepdf(df, weburl, pdffile, maxcontentlength, ignorelength, mincontentoverlap):
    """
    parse a PDF file and add contents to the dataframe
    :param df:     existing dataframe, could already have data
    :param weburl:  the URL (or file location) from which this pdf is retrieved.
    :param pdffile:   pdffile name
    :param maxcontentlength:  max # of chars, longer contents will be broken into multiple
    :param ignorelength:      ignore short content, in chars
    :param mincontentoverlap: requires minimum # of chars overlap when breaking up contents
    :return:    updated dataframe with this PDF file:  DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])
    """
    pdfdoc = load_file(pdffile)
    headerdf = buildPdfHeaderMapping(pdfdoc, headermaxlen=200, ignorelen=10, ignorecombinedlen=ignorelength)
    h1 = ''
    h2 = ''
    h3 = ''
    # second scan put into df
    concattext = ''
    elementlist = pdfdoc.elements
    for anelem in elementlist:
        fs = int(anelem.font_size)
        stext = ' '.join(anelem.text().split())
        coltype = headermap(fs, headerdf)
        if coltype == 'h1':
            df = addpdfrows(df, weburl, h1, h2, h3, concattext, maxcontentlength, ignorelength, mincontentoverlap)
            concattext = ''
            h1 = stext
            h2 = ''
            h3 = ''
        elif coltype == 'h2':
            df = addpdfrows(df, weburl, h1, h2, h3, concattext, maxcontentlength, ignorelength, mincontentoverlap)
            concattext = ''
            h2 = stext
            h3 = ''
        elif coltype == 'h3':
            df = addpdfrows(df, weburl, h1, h2, h3, concattext, maxcontentlength, ignorelength, mincontentoverlap)
            concattext = ''
            h3 = stext
        elif coltype == 'text':
            concattext = concattext + ' ' + stext
    df = addpdfrows(df, weburl, h1, h2, h3, concattext, maxcontentlength, ignorelength, mincontentoverlap)
    return(df)

def addpdfrows(df, weburl, h1, h2, h3, concattext, maxcontentlength, ignorelength, mincontentoverlap):
    """
    add rows to dataframe, with PDF contents
    since PDF headers are guessed from font size, h3 could be contents, so ignorelength should be applied to the entire headers

    :param df:    data frame to add rows
    :param weburl: webrul for this content
    :param h1:    header 1 based on PDF font size
    :param h2:    header 2 based on PDF font size
    :param h3:    header 3 based on PDF font size
    :param concattext:    text
    :param maxcontentlength:   max length to break down to multiple rows
    :param ignorelength:    smaller contents are ignored
    :param mincontentoverlap:  overlap length
    :return:  a data frame containing added rows
    """
    key = h1 + " - " + h2 + " - " + h3
    if (len(concattext) < ignorelength) and ((len(key) - 6) > ignorelength) :
        concattext = h1 + " " + h2 + " " + h3 + "  " + concattext
    df = addrows(df, weburl, key, concattext, maxcontentlength, ignorelength, mincontentoverlap)
    return(df)

def parsehtml(df, weburl, htmltext, maxcontentlength, ignorelength, mincontentoverlap):
    # assume we always have h1
    h1str=''
    h2str=''
    h3str=''
    contentstr=''
    s = BeautifulSoup(htmltext, 'html.parser')
    currelem = s.find('h1')
    if currelem != None and currelem.string != None:
        h1str = currelem.string.strip()
        while currelem != None:
            if currelem.name == None:
                if currelem.string != None:
                    contentstr += " ".join(currelem.string.split())
                currelem = currelem.next_element
            elif currelem.name == 'h1':
                # save h1|h2|h3 contents so far, start a new h2
                key = h1str + " - " + h2str + " - " + h3str
                df = addrows(df, weburl, key, contentstr, maxcontentlength, ignorelength, mincontentoverlap)

                h1str = concatstrings(currelem.strings)
                h2str = ''
                h3str = ''
                contentstr = ''
                currelem = currelem.next_sibling
            elif currelem.name == 'h2':
                # save h1|h2|h3 contents so far, start a new h2
                key = h1str + " - " + h2str + " - " + h3str
                df = addrows(df, weburl, key, contentstr, maxcontentlength, ignorelength, mincontentoverlap)

                h2str = concatstrings(currelem.strings)
                h3str = ''
                contentstr = ''
                currelem = currelem.next_sibling
            elif currelem.name == 'h3':
                # save h1|h2|h3 contents so far, start a new h2
                key = h1str + " - " + h2str + " - " + h3str
                df = addrows(df, weburl, key, contentstr, maxcontentlength, ignorelength, mincontentoverlap)

                h3str = concatstrings(currelem.strings)
                contentstr = ''
                currelem = currelem.next_sibling
            elif currelem.name == 'script' or currelem.name == 'style' or currelem.name == 'meta' or currelem.name == 'svg':
                currelem = currelem.next_sibling
            else:
                allstrings = currelem.strings
                contentstr += concatstrings(allstrings)
                currelem = currelem.next_element
    else:
        # no h1 header, simply pick up all text
        contentstr = concatstrings(s.stripped_strings)

    #  write last section of this webpage
    key = h1str + " - " + h2str + " - " + h3str
    df = addrows(df, weburl, key, contentstr, maxcontentlength, ignorelength, mincontentoverlap)
    return(df)

def parseWebContent(webpage, aresponse, df, maxcontentlength=8000, ignorelength=30, mincontentoverlap=800):
    """
    given a list of web URLs and a list of Response object, extract contents and put into dataframe

    :param webs:              a list of web urls
    :param responses:         a list of response objects
    :param maxcontentlength:  max # of chars, longer contents will be broken into multiple
    :param ignorelength:      ignore short content, in chars
    :param mincontentoverlap: requires minimum # of chars overlap when breaking up contents
    :return:  dataframe, with extracted contents  columns=['webpage', 'subject', 'content', 'combined'])
    """
    try:
        log(f"{threading.current_thread().name} parsing web page {webpage[:80]}    ", endstr='\n')
        if aresponse == None:
            log(f"Skip page {webpage[:80]}  with no response.        \n",  outfile=sys.stderr)
            return df
        ct = aresponse.headers['Content-Type']
        if 'text/html' in ct.lower():
            htmltext = aresponse.html.html
            df = parsehtml(df, webpage, htmltext, maxcontentlength, ignorelength, mincontentoverlap)
        elif 'application/pdf' in ct.lower():
            pdffilename = '/tmp/web' + str(hash(webpage))+'.pdf'
            pdffile = open(pdffilename, 'wb')
            pdffile.write(aresponse.content)
            pdffile.close()
            df = parsepdf(df, webpage, pdffilename, maxcontentlength, ignorelength, mincontentoverlap)
        else:
            log(f"Skip page {webpage[:80]} with unknown content type {ct}   \n", outfile=sys.stderr)
    except Exception as ex:
        log(f"Failed to parse web content for {webpage=}    ", endstr="\n", outfile=sys.stdout)

    return df

def _parseWebContent(args):
    return parseWebContent(*args)
def extractWebContents(webs, responses, maxcontentlength=8000, ignorelength=30, mincontentoverlap=800):
    """
    given a list of web URLs and a list of Response object, extract contents and put into dataframe

    :param webs:              a list of web urls
    :param responses:         a list of response objects
    :param maxcontentlength:  max # of chars, longer contents will be broken into multiple
    :param ignorelength:      ignore short content, in chars
    :param mincontentoverlap: requires minimum # of chars overlap when breaking up contents
    :return:  dataframe, with extracted contents  columns=['webpage', 'subject', 'content', 'combined'])
    """

    df = pd.DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])

    h1str = ''
    h2str = ''
    h3str = ''
    contentstr = ''
    webindex = 0

    for aresponse in responses:
        webpage = webs[webindex]
        webindex = webindex + 1
        df = parseWebContent(webpage, aresponse, df, maxcontentlength, ignorelength, mincontentoverlap)

    return df



def collectArguments(webs, responses, df, maxcontentlength, ignorelength, mincontentoverlap):
    retargs = []
    idx = 0;
    for weburl in webs:
        aresponse = responses[idx]
        idx = idx + 1
        thisarg = (weburl, aresponse, df, maxcontentlength, ignorelength, mincontentoverlap)
        log(f"a pair of argument {thisarg=}        ", endstr="\n")
        retargs.append(thisarg)

    return retargs

def extractWebContentsParallel(webs, responses, maxcontentlength=8000, ignorelength=30, mincontentoverlap=800):
    """

    :param webs:
    :param responses:
    :param maxcontentlength:
    :param ignorelength:
    :param mincontentoverlap:
    :return: combined data frame from all results
    """
    pcount = 4
    if (os.cpu_count() > 6):
        pcount = os.cpu_count() - 2

    dfs = []
    df = pd.DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])

    log(f"Parse {len(webs)} web responses in {pcount} threads." + (" " * 50), endstr="\n")
    try:
        # args = collectArguments(webs, responses, df, maxcontentlength, ignorelength, mincontentoverlap)
        with cf.ThreadPoolExecutor(max_workers=pcount) as executor:
            fs = []
            webidx = 0;
            try:
                for aresponse in responses:
                    weburl = webs[webidx]
                    webidx = webidx + 1
                    thisarg = (weburl, aresponse, df, maxcontentlength, ignorelength, mincontentoverlap)
                    afuture = executor.submit(_parseWebContent, thisarg)
                    fs.append(afuture)
                for af in fs:
                    aresult = af.result()
                    dfs.append(aresult)
                return pd.concat(dfs, ignore_index=True)
            except Exception as procErr:
                log(f"Some processes are timed out: {procErr=} \n", outfile=sys.stdout)
                traceback.print_exc(limit=10, file=sys.stderr, chain=True)
    except Exception as err:
        log(f"Unexpected {err=} \n", outfile=sys.stdout)
        traceback.print_exc(limit=8, file=sys.stderr, chain=True)
    return None


def getBingSearchLinks(searchphrase, numresults=10):
    """
    Search BING with a search phrase, return a list of URLs.
    Ignore ads, and other URLs, just the main page URLs from Bing algorithm

    :param searchphrase:  the search query to Bing
    :param numresults:   the number of URLs returned, default 10 - first Bing page
    :return:  a list of URLs in string format
    """
    # use smaller web pages to avoid out of memory errors
    webs = ['https://downloads.regulations.gov/NTIA-2023-0005-0108/attachment_1.pdf',
            'https://garyzhu.net/',
            'https://www.garyzhu.net/notes/ZombieCookie.html',
            'https://www.garyzhu.net/notes/SentientAI.html',
            'https://garyzhu.net/notes.html',
            'https://garyzhu.net/notes/Oracle_64.html',
            'https://www.garyzhu.net/notes/LVM_Linux.html']
    # webs = []
    if len(webs) > 0:
        return webs

    qstr=urllib.parse.quote(searchphrase, safe='')
    srch1 = "https://www.bing.com/search?q=" + qstr + "&rdr=1&first=1"
    srch2 = "https://www.bing.com/search?q=" + qstr + "&rdr=1&first=2"
    srch3 = "https://www.bing.com/search?q=" + qstr + "&rdr=1&first=3"

    srchs = [ srch1 ]
    if numresults > 20:
        srchs = [srch1, srch2, srch3]
    elif numresults > 10:
        srchs = [srch1, srch2]

    log(f" run Bing search ...  {searchphrase}" + (" " * 20), endstr="\r")
    results = getAsyncWebResponses(srchs)
    for aresult in results:
        contents = aresult.html.html
        try:
            log(" parse Bing search results..." + (" " * 60), endstr="\r")
            # carefully skip Ads and other non-essential materials
            for b_algo in BeautifulSoup(contents, 'html.parser').find("div", id="b_content").find_all("li", class_="b_algo"):
                s1 = BeautifulSoup(str(b_algo), 'html.parser')
                a_tags = s1.find_all('a')
                #  find first <a> tag with href and starts with https://, skip positional or javascript <a> tag
                for a_tag in a_tags:
                    bhref = a_tag.get("href")
                    if bhref != None and  bhref.lower().startswith('https://'):
                        log(f'  a valid search result url {bhref[:60]}        ', endstr='\r')
                        webs.append(str(bhref))
                        break

        except Exception as err:
            log(f"Unexpected {err=} when parsing Bing result\n", outfile=sys.stderr)
            traceback.print_exc(limit=8, file=sys.stderr, chain=False)
    return webs[:numresults]
    return webs