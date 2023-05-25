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
from commonfuncs import log
import sys

def updateHeaderRow(df, sizenum, lengthnum):
    if df.loc[(df['font_size'] == sizenum)].any().all():
        if df.loc[ (df['font_size'] == sizenum) & (df['length'] < lengthnum) ].any().all():
            df.loc[df.font_size == sizenum, 'length'] = lengthnum

        c = df.loc[df.font_size == sizenum, 'count']
        c += 1
        df.loc[df.font_size == sizenum, 'count'] = c
    else:
        df.loc[len(df.index)] = [sizenum, lengthnum, 1]

def combined_len(row):
    return row['length'] * row['count']

def build_headerdata(pdfdoc, headermaxlen=200, ignorelen=5, ignorecombinedlen=20):
    """
    Parse document to establish header mapping (based on font size)
        [font_size, max_length, count, combined_len, typecol]
    :param pdfdoc:   the PDF document
    :param headermaxlen:  max length that can be considered as header
    :param ignorelen:     any section less than this is discoarded, to ignore such things as page numbers
    :param ignorecombinedlen:  any section with combined length less than this, is discarded
    :return:   dataframe font_size and typecol (with 'h1', 'h2', 'h3', 'text')
    """

    headerdf = pd.DataFrame(None, columns=['font_size', 'length', 'count'])
    elementlist = pdfdoc.elements;
    for anelem in elementlist:
        fs = int(anelem.font_size)
        tl = len(anelem.text())
        updateHeaderRow(headerdf, fs, tl)

    sortdf = headerdf.sort_values(by="font_size", ascending=False)
    sortdf = sortdf.reset_index(drop=True)
    sortdf['combined_len'] = sortdf.apply(combined_len, axis=1)
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

def addrow(df, afile, h1, h2, h3, ctext):
    if len(h1) > 5:
        hh = f'{h1} - {h2} - {h3}'
        df.loc[len(df.index)] = [afile, hh, ctext, f'{hh} : {ctext}' ]


def concatstrings(strarray):
    retstring = ''
    if strarray != None:
        for astr in strarray:
            retstring += " ".join(astr.split()) + " "
    return retstring

def addarow(df, weburl, subjectstr, contentstr, maxcontentlength=2000, ignorelength=20, mincontentoverlap=400):
    """
    add rows to dataframe, break contents into multiple rows if exceeding max number of tokens for GTP3
    :param df:    dataframe to add rows to
    :param weburl:  web url
    :param subjectstr:   Subject column
    :param contentstr:   Contents
    :param maxcontentlength:  max # chars, contents will be broken into multiple
    :param ignorelength:      ignore short content, in chars
    :param mincontentoverlap:  requires minimum # of chars overlap when breaking up contents
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

def parsepdf(df, weburl, pdffile):
    """
    parse a PDF file and add contents to the dataframe
    :param df:     existing dataframe, could already have data
    :param pdffile:   pdffile name
    :param viewpdf:   whether to launch a PDF visualize, default is False.
    :return:    updated dataframe with this PDF file:  DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])
    """
    pdfdoc = load_file(pdffile)
    headerdf = build_headerdata(pdfdoc)
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
            key = h1 + ' - ' + h2 + ' - ' + h3
            df = addarow(df, weburl, key, concattext)
            concattext = ''
            h1 = stext
            h2 = ''
            h3 = ''
        elif coltype == 'h2':
            key = h1 + ' - ' + h2 + ' - ' + h3
            df = addarow(df, weburl, key, concattext)
            concattext = ''
            h2 = stext
            h3 = ''
        elif coltype == 'h3':
            key = h1 + ' - ' + h2 + ' - ' + h3
            df = addarow(df, weburl, key, concattext)
            concattext = ''
            h3 = stext
        elif coltype == 'text':
            concattext = concattext + ' ' + stext
    key = h1 + ' - ' + h2 + ' - ' + h3
    df = addarow(df, weburl, key, concattext)
    return(df)

def parsehtml(df, weburl, htmltext):
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
                df = addarow(df, weburl, key, contentstr)

                h1str = concatstrings(currelem.strings)
                h2str = ''
                h3str = ''
                contentstr = ''
                currelem = currelem.next_sibling
            elif currelem.name == 'h2':
                # save h1|h2|h3 contents so far, start a new h2
                key = h1str + " - " + h2str + " - " + h3str
                df = addarow(df, weburl, key, contentstr)

                h2str = concatstrings(currelem.strings)
                h3str = ''
                contentstr = ''
                currelem = currelem.next_sibling
            elif currelem.name == 'h3':
                # save h1|h2|h3 contents so far, start a new h2
                key = h1str + " - " + h2str + " - " + h3str
                df = addarow(df, weburl, key, contentstr)

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
    df = addarow(df, weburl, key, contentstr)
    return(df)

def extractcontents(webs, responses):
    """
    given a list of web URLs and a list of Response object, extract contents and write into dataframe
    :param webs:   a list of web urls
    :param responses: a list of corresponding response
    :return:  dataframe, with extracted contents in the format of
     pd.DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])
    """

    df = pd.DataFrame(None, columns=['webpage', 'subject', 'content', 'combined'])

    h1str = ''
    h2str = ''
    h3str = ''
    contentstr = ''
    log(' dataframe is ' + str(len(df.index)))
    webindex = 0

    for aresponse in responses:
        webpage = webs[webindex]
        webindex = webindex + 1

        if aresponse == None:
            print("Skip page " + webpage)
            continue
        ct = aresponse.headers['Content-Type']
        if 'text/html' in ct.lower():
            htmltext = aresponse.html.html
            df = parsehtml(df, webpage, htmltext)
        elif 'application/pdf' in ct.lower():
            pdffilename = '/tmp/web' + str(hash(webpage))+'.pdf'
            pdffile = open(pdffilename, 'wb')
            pdffile.write(aresponse.content)
            pdffile.close()
            df = parsepdf(df, webpage, pdffilename)
        else:
            log(f"Skip page {webpage=} with unknown content type {ct}")

    #embedding_encoding = "cl100k_base"  # this the encoding for text-embedding-ada-002
    #encoding = tiktoken.get_encoding(embedding_encoding)
    #df["n_tokens"] = df.combined.apply(lambda x: len(encoding.encode(x)))
    return df
