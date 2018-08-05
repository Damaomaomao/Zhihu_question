# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import MapCompose,TakeFirst,Join
from Zhihu.settings import SQL_DATETIME_FORMAT
import datetime
from scrapy.loader import ItemLoader


class ZhihuItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass



# 排除none值


def exclude_none(value):
    if value:
        return value
    else:
        value = "无"
        return value

def extract_num_include_dot(text):
    # 从包含,的字符串中提取出数字
    text_num = text.replace(',', '')
    try:
        nums = int(text_num)
    except:
        nums = -1
    return nums

def get_nums(value):
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums


# 自定义itemloader实现默认取第一个值
class ZhihuItemLoader(ItemLoader):
    default_output_processor = TakeFirst()

class ZhihuQuestionItem(scrapy.Item):
    question_id = scrapy.Field()
    topics = scrapy.Field(
        input_processor=Join(",")
    )
    url = scrapy.Field()
    title = scrapy.Field(
        input_processor=Join(",")
    )
    # content = scrapy.Field(
    #     input_processor = MapCompose(exclude_none),
    # )
    answer_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = '''
        insert into zhihu_question(question_id,topics,url,title,answer_num,watch_user_num,click_num,crawl_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE answer_num=VALUES(answer_num)
        '''
        # try:
        #     content = "".join(self['content'])
        # except BaseException:
        #     content = "无"

        if len(self["click_num"]) == 2:
            watch_user_num = extract_num_include_dot(self["click_num"][0])
            click_num = extract_num_include_dot(self["click_num"][1])
        else:
            watch_user_num = extract_num_include_dot(self["click_num"][0])
            click_num = 0

        answer_num = extract_num_include_dot(self['answer_num'][0])

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        params=(
            self['question_id'],
            self['topics'],
            self['url'],
            self['title'],
            answer_num,
            watch_user_num,
            click_num,
            crawl_time,
        )
        return insert_sql,params



class ZhihuAnswerItem(scrapy.Item):
    #知乎的问题回答item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()
    author_name = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎answer表的sql语句
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comments_num,
              create_time, update_time, crawl_time,author_name
              ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
              ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num), praise_num=VALUES(praise_num),
              update_time=VALUES(update_time), author_name=VALUES(author_name)
        """

        create_time = datetime.datetime.fromtimestamp(
            self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(
            self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = (
            self["zhihu_id"], self["url"], self["question_id"],
            self["author_id"], self["content"], self["praise_num"],
            self["comments_num"], create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
            self["author_name"],
        )

        return insert_sql, params