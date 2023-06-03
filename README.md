# Change the way we search

## Summary

With traditional search process, I type in a question, and search engine would return some URLs. 
It may also include snippets with highlighted (hint: literal matching) search words. But responses are seggregated by different web pages, 
and I might have to read through several pages and summarize the contents, if there is no single webpage give me all the answers.

Bottom line, how to finish ***the last mile*** of user search journey, by providing answers, instead of places (URLs) to find answers.


##  What this app does

1. Given user's search query, look up search engine to find top 15 (configurable) URLs.
2. Retrieve these web pages, digest contents, find most relevant materials for users' search query
3. Generate a consolidated response, attaching reference URLs from which this answer is derived.
4. Users can continue ask questions within the same context
5. If subsequent queries are not relevant to the established context, the app would fall back to OpenAI for answer.

##  To use this app
1. packages needed:
    ```
    /usr/local/bin/python3 -m pip install  pandas openai tiktoken  matplotlib plotly scikit-learn pandarallel  requests-html py-pdf-parser Jinja2
   ```
2. environment variables OPENAI_ORG_ID and OPENAI_API_KEY (with your OpenAI account) should be set up beforehand.
3. the app has coded in [OpenAI rate limit](https://platform.openai.com/docs/guides/rate-limits/overview), based on ***pay-as-you-go*** plan.
4. answers from OpenAI was set up to randomly include sarcastic tones, to lift my mood during long arduous language model analysis. 
5. it is *slow*, all Open API calls are sequential. I tried and failed to make parallel calls, regardless rate limit.
For commercial use, there are several potentials to speed up response time. 
But even with current form, **it might still be faster than most people reading through 10 web pages**.


### Exhibits
(set video player to high resolution)
##### [Sample run to search for "transformer"](https://youtu.be/LoG6fMjZQ7o)
##### [Sample run to search for US debt ceiling solution](https://youtu.be/ZC5cSXwPaWM)

## Deficiencies -- help to improve this software
### parse multi-column PDF document 
The software will extract text contents from PDF, but the scan is always from left to right and then downwards, this would cause problems when parsing multi-column documents.

### get coherent messages from tabled contents
A table could be interpreted horizontally or virtically, how to improve the code to extract the meaning from table contents. 

### multimodal contents
In many documents, a diagram or image could carry a lot of information, how to get the information from graphs, and make it part of the knowledgebase.


## Possible application on top of this - personalized responses
The software could have a set of knowledges and attempt to provide answers with preference of local knowledge, before falling back to OpenAI answer.
Set up personalized knowledgebase, and hence have the answers tailored to individual persons. 
* Searching for **healthy diet**  could have different answers for different people, based on age and medical conditions; 
* searching for **daily joke**  could be based on person's cultural and religious background, or political affiliations; 

A user has the flexibilty to build up personalized knowledgebase, 
including medical records (treatment and prevention), your favorite bible or books reflecting your moral compass, 
 materials reflecting your cultural background and traditions.  Make the response of a search (such as below) more suitable to the user.
##### What is a healthy diet? 
##### Should I eat pork or beef?  Should I eat fish?
##### How to compare gas stove vs electric stove, which one is better?
##### Tell me a joke about *&lt;your choice of political figure&gt;*.


