from lxml import html
import requests
import mysql.connector
import firebase_admin
from datetime import datetime, timedelta
from dateutil import parser
from unidecode import unidecode
from transliterate import translit
from firebase_admin import messaging, db, credentials

# Database setup
mydb = mysql.connector.connect(
    host="localhost",
    user="milan",
    passwd="blrkmnjs1",
    database="masinski_news"
)

mycursor = mydb.cursor()
cred = credentials.Certificate(
    '/home/milan/Downloads/mech-engineering-notifications-firebase-adminsdk-wdzn4-4760a2e9da.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://mech-engineering-notifications.firebaseio.com/'
})


class NewsItem:

    def __init__(self, news_item):
        # Link of the news item
        self.link = news_item.xpath("a/@href")[0].split("\'")[0].strip()
        # Subject name
        self.name = translit(news_item.xpath("a/text()")[0].split("•")[0].split(")")[0].strip() + ')', 'sr',
                             reversed=True)
        # Body of the news item
        self.body = translit(news_item.xpath("a/text()")[0].split("•")[1].split("\'")[0].strip(), 'sr', reversed=True)
        # Extract date and parse to datetime object
        self.original_date = news_item.xpath("text()")[0].split("(")[1].split(")")[0].strip()

        self.date = parser.parse(self.original_date, dayfirst=True)


# Get HTML of the page
page = requests.get("http://www.mas.bg.ac.rs/studije/vesti")
tree = html.fromstring(page.content)

# Get all news items on the website
all_news = tree.xpath('//ul[@class="rss"]/li')
# Date of the last item in the database
mycursor.execute("SELECT * FROM news_list ORDER BY id DESC LIMIT 1")
result = mycursor.fetchall()
if mycursor.rowcount == 0:
    # empty table, set date to yesterday
    last_date = datetime.now() - timedelta(3)
else:
    last_date = result[0][4]
print('Running cron... last time ran: ' + str(last_date))
# For each item check if the date is newer than the last item in the database
# and if so, add it.
for item in reversed(all_news):
    base_item = item.xpath('div[@class="li"]')
    news = NewsItem(base_item[0])

    if news.date > last_date:
        # if the date of the current item is newer than the last date before the cronjob
        # had started save the item to the db
        sql = "INSERT INTO news_list (url, name, body, date) VALUES (%s, %s, %s, %s)"
        val = (news.link, news.name, news.body, news.date)
        mycursor.execute(sql, val)
        mydb.commit()
        # Send a Firebase push notification to relevant topic
        message = messaging.Message(
            data={
                'title': news.name,
                'body': news.body + str(' (' + news.original_date + ')'),
                'url': news.link,
            },
            topic=unidecode(news.name).replace(' ', '_').replace('(', '').replace(')', '').strip()
        )
        print('Found new item. Sending message to topic: ' + message.topic)

        response = messaging.send(message)
