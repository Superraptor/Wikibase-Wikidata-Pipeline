import constants
import feedparser
import json
import os


def rss_feed_properties():
    property_dict = {
        "author": "",
        "authors": "",
        "content": "",
        "guidislink": "",
        "id": "",
        "link": "",
        "links": "",
        "published": "",
        "published_parsed": "",
        "summary": "",
        "summary_detail": "",
        "tags": "",
        "title": "",
        "title_detail": ""
    }


def fetch_rss_articles(site, feed_url):
    feed_dict = {}
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        feed_dict['id'] = entry


def main():
    rss_feeds = {}
    if os.path.exists(constants.RSS_FEEDS_FILE):
        with open(constants.RSS_FEEDS_FILE, "r", encoding="utf-8") as f:
            rss_feeds = json.load(f)

    for site_name, feed_url in rss_feeds.items():
        articles = fetch_rss_articles(site_name, feed_url)


if __name__ == "__main__":
    main()