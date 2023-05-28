#!/usr/local/bin/python3
#
#  Parse PDF files from command line arguments and puyt into Pandas DataFrame
#
#  package installed: (version lagging for python3.11)
#     /usr/local/bin/python3 -m pip install py-pdf-parser\[dev\]
#     /usr/local/bin/python3 -m pip install cython
#     brew install python-tk@3.9
#     brew install freetype imagemagick
#
from py_pdf_parser.loaders import load_file
import pandas as pd
from bs4 import BeautifulSoup
from commonfuncs import log, getAsyncWebResponses
import sys, traceback, urllib.parse

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

def buildPdfHeaderMapping(pdfdoc, headermaxlen=200, ignorelen=10, ignorecombinedlen=20):
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
def splitstring(nStr, maxLen=2000, minOverlap=200):
    if maxLen < (minOverlap + 20):
        raise Exception("max length (" + str(maxLen) +") should be greater than minimum overlap ("+ str(minOverlap) + ") by more than 20 chars")
    retList = []
    while len(nStr) > maxLen:
        p1 = maxLen - 1
        while nStr[p1] != ' ' and p1 > 0:
            p1 = p1 - 1
        retList.append(nStr[0:p1])
        p2 = p1 - minOverlap
        while nStr[p2] != ' ' and p2 > 1:
            p2 = p2 -1
        nStr = nStr[p2+1:]

    retList.append(nStr)
    return retList

def parsepdf(df, weburl, pdffile, maxcontentlength=8000, ignorelength=30, mincontentoverlap=800):
    """
    parse a PDF file and add contents to the dataframe
    :param df:     existing dataframe, could already have data
    :param pdffile:   pdffile name
    :param viewpdf:   whether to launch a PDF visualize, default is False.
    :return:    updated dataframe with this PDF file:  DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])
    """
    pdfdoc = load_file(pdffile)
    headerdf = buildPdfHeaderMapping(pdfdoc)
    #write(headerdf.to_string())
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

def parsehtml(df, weburl, htmltext, maxcontentlength=8000, ignorelength=30, mincontentoverlap=800):
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
        log(f"  parsing web page {webpage[:60]}    ", endstr='\r')
        if aresponse == None:
            log(f"Skip page {webpage[:80]}  with no response.        \n",  outfile=sys.stderr)
            continue
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

    #embedding_encoding = "cl100k_base"  # this the encoding for text-embedding-ada-002
    #encoding = tiktoken.get_encoding(embedding_encoding)
    #df["n_tokens"] = df.combined.apply(lambda x: len(encoding.encode(x)))
    return df

def getBingSearchLinks(searchphrase, numresults=10):
    """
    Search BING with a search phrase, return a list of URLs.
    Ignore ads, and other URLs, just the main page URLs from Bing algorithm

    :param searchphrase:  the search query to Bing
    :param numresults:   the number of URLs returned, default 10 - first Bing page
    :return:  a list of URLs in string format
    """
    webs = []
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