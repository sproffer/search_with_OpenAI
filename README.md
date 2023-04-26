# Change the way we search

## Summary

With traditional search process, I type in a question, and search engine would return some URLs likely containing the answer. The search engine may also include snippets with highlighted (literral matching) words.\
On an unlucky day, I might have to click through several links, read through all these pages.   Frustrations arise, "if you know these pages have what I wanted, you must have figured out something, why not you read through these pages and give me a straight answer" -- here comes OpenAI.

Bottom line, finish ***the last mile*** of user search journey, by providing answers, instead of places to find answers.


##  What this app does

1. Given user's search query, look up search engine to find top 10 URLs.
2. Retrieve these web pages, digest contents, find most relevant materials for users' search query
3. Generate answer, attaching reference URLs, from which this answer is derived.
4. Users can continue ask questions within the same context
5. If subsequent queries are not relevant to the context, the app would fallback to GPT-3.5 for answer.

##  To use this app
1. commfuncs.pyc is compiled with Python 3.9, stuck to this version because of other library dependencies.
2. environment variables OPENAI_ORG_ID and OPENAI_API_KEY should be set up beforehand.
3. The app has coded in OpenAI rate limit, based on ***pay-as-you-go*** plan.
4. answers from OPENAI was set up with *some* sarcastic tones.

## Exhibit 1

###  Search information about 2023 Miami ultra music festival
*Note: this event is ***not*** in GPT-3.5 training data*


%  ./webSearch.py\
Enter your question :\
    <b>provide a list of artists in 2023 ultra music festival in Miami.</b>\
\
17:45:55 - ...........  .........\
17:45:56 - Finished embedding - hash=3c55c82523ef25\
17:45:59 - selected 12 (among 174) relevant sections to generate answers...\
17:46:40 -  query gpt-3.5-turbo with 439 tokens ........................\
\
\
17:46:46 - Answer:\
<b>The given content includes information about the Phase 2 lineup for Ultra Music Festival Miami 2023, which includes over 100 additional acts with headliners such as Adam Beyer, Cirez D, Afrojack, Alesso, CamelPhat, Charlotte de Witte, David Guetta, Gareth Emery, M.I.A., Marshmello, Michael Bibi, Stephan Bodzin (Live), Tiesto, and Vini Vici. The article also mentions the Phase 1 lineup, registration for an online platform, and the stages that will be featured at the festival. However, there is no information provided about the lineup for Ultra Music Festival in March 2024.</b>\
\
17:46:46 - References:\
[\
  <b>"https://soundrivemusic.com/2023/02/03/ultra-music-festival-phase-2/",</b> \
  <b>"https://weraveyou.com/2022/09/ultra-music-festival-miami-2023-lineup/",</b> \
  <b>"https://ultramusicfestival.com/lineup/",</b> \
  <b>"https://www.timeout.com/miami/music/everything-you-need-to-know-about-the-ultra-music-festival-miami-2023-lineup-and-schedule"</b> \
]\
\
Enter your next question, if you want to stop, type stop:\
    <b>who is the governor of California?</b>\
\
17:47:20 - no relevant data from your materials, use OpenAI to generate answers...\
\
17:47:24 - Answer:\
<b>(OpenAI sarc) Oh, I don't know, maybe it's Arnold Schwarzenegger? Wait, no, that was like a decade ago. Let me check my crystal ball... Oh, it says here that the current governor of California is Gavin Newsom. But who knows, maybe my crystal ball is outdated too.</b>\
\
Enter your next question, if you want to stop, type stop:\
    stop \
\
17:47:45 - The End

##  Exhibit 2
### subsequent questions that can be answered in prepared materials

% ./webSearch.py \
Enter your question : \
***what is the population of San Francisco?*** \
20:00:48 - .............................................  ......... \
20:00:49 - Finished embedding - hash=ccb8249b73b225 \
20:00:53 - selected 12 (among 849) relevant sections to generate answers... \
20:01:37 -  query gpt-3.5-turbo with 1138 tokens ........................ \
\
\
20:01:40 - Answer: \
***The population of San Francisco is 815,201 as of the 2020 United States Census.***
\
20:01:40 - References: \
[ \
***"https://en.wikipedia.org/wiki/Demographics_of_San_Francisco",*** \
***"https://worldpopulationreview.com/us-cities/san-francisco-ca-population",*** \
***"https://censusreporter.org/profiles/16000US0667000-san-francisco-ca/",*** \
***"https://www.britannica.com/place/San-Francisco-California",*** \
***"https://www.biggestuscities.com/city/san-francisco-california"*** \
] \
\
Enter your next question, if you want to stop, type stop: \
***did the San Francisco population decline or increase in the past 2 years ?*** \
\
20:05:39 - selected 12 (among 849) relevant sections to generate answers... \
20:06:25 -  query gpt-3.5-turbo with 709 tokens ........................ \
\
20:06:32 - Answer: \
***Based on the context provided, it appears that the population of San Francisco increased slightly in the past 2 years. The current metro area population of San Francisco in 2023 is 3,328,000, which is a 0.3% increase from 2022. However, without specific data for the past 2 years, it is difficult to provide a more precise answer.*** \
\
20:06:32 - References: \
[ \
***"https://censusreporter.org/profiles/16000US0667000-san-francisco-ca/",*** \
***"https://worldpopulationreview.com/us-cities/san-francisco-ca-population",*** \
***"https://en.wikipedia.org/wiki/Demographics_of_San_Francisco",*** \
***"https://www.britannica.com/place/San-Francisco-California",*** \
***"https://usafacts.org/data/topics/people-society/population-and-demographics/our-changing-population/state/california/county/san-francisco-county/"*** \
] \
\
Enter your next question, if you want to stop, type stop: \
***who is the governor of california ?*** \
\
20:07:13 - no relevant data from your materials, use OpenAI to generate answers... \
\
20:07:15 - Answer: \
***(OpenAI sarc) Oh, I don't know, maybe it's Mickey Mouse? Of course, it's Gavin Newsom!*** \
\
Enter your next question, if you want to stop, type stop: \
***who is the 42nd president of the united states?*** \
\
20:08:06 - no relevant data from your materials, use OpenAI to generate answers... \
\
20:08:12 - Answer: \
***(OpenAI sarc) Oh, that's easy. It's obviously Abraham Lincoln.*** \
\
Enter your next question, if you want to stop, type stop: \
***who is the 43rd president of the united states ?*** \
\
20:08:42 - no relevant data from your materials, use OpenAI to generate answers... \
\
20:08:44 - Answer: \
***(OpenAI sarc) Oh, come on! Everyone knows that it's obviously Abraham Lincoln. Just kidding, it's George W. Bush.*** \
\
Enter your next question, if you want to stop, type stop: \
stop \
\
20:09:03 - The End


