# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import scrapy
from scrapy import Request
import re
import json
import datetime
from Zhihu.items import ZhihuQuestionItem, ZhihuAnswerItem
from urllib import parse
from scrapy.loader import ItemLoader
from datetime import datetime


class ZhihucrawlSpider(CrawlSpider):
    name = 'zhihucrawl'
    allowed_domains = ['zhihu.com']
    start_urls = ['https://zhihu.com/']

    def process_value(value):
        url = parse.urljoin("https://zhihu.com/", value)
        match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", url)
        if match_obj:
            question_url = match_obj.group(1)
            return question_url
        else:
            pass

    rules = (
        Rule(LinkExtractor(allow=r'question/\d+',process_value=process_value), callback='parse_question', follow=True),
    )

    '''
    answer_url共由三部分组成：
    answer_base_url = "https://www.zhihu.com/api/v4/questions/{question_id}/answers?"
    include="include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics&"
    param = 'limit=5&offset=0&sort_by=default'# question的第一页answer的请求url   
    '''
    answer_url = 'https://www.zhihu.com/api/v4/questions/{question_id}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics&limit=5&offset=0&sort_by=default'


    def parse_question(self, response):

        id_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)
        if id_obj:
            question_id = int(id_obj.group(2))
        item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response)
        item_loader.add_css("title", "h1.QuestionHeader-title::text")
        # item_loader.add_css("content", ".QuestionHeader-detail")
        item_loader.add_value("url", response.url)
        item_loader.add_value("question_id", question_id)
        item_loader.add_css("answer_num", ".List-headerText span::text")
        item_loader.add_css("click_num", ".NumberBoard-itemValue::text")
        item_loader.add_css("topics", ".QuestionHeader-topics .Popover div::text")
        item_loader.add_value("crawl_time", datetime.now())
        question_item = item_loader.load_item()
        yield Request(url=self.answer_url.format(question_id=question_id),callback=self.parse_answer)
        yield question_item

    def parse_answer(self, response):
        html = json.loads(response.text)
        is_end = html['paging']['is_end']
        next_url = html['paging']['next']
        for answer in html['data']:
            answer_item = ZhihuAnswerItem()
            answer_item["zhihu_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"]["id"] if "id" in answer["author"] else None
            answer_item["author_name"] = answer["author"]["name"] if "name" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer else None
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, callback=self.parse_answer)

