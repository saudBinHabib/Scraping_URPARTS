# -*- coding: utf-8 -*-
# importing all the required packages.

import csv
import urllib.parse
import scrapy

from scrapy.http import Request


class UrpartsCrawlingSpider(scrapy.Spider):
    # spider name for crawling
    name = 'urparts_crawling'
    # this variable defines that this domain is allowed to be crawled.
    allowed_domains = ['urparts.com']
    # this variable contains the starting url for crawling.
    start_urls = ['https://www.urparts.com/index.cfm/page/catalogue']

    # create mapping for the xpath address to extract the content.
    response_mapping_xpath = {
        # xpath for extracting the names of manufacture, category and model path.
        'man_cat_model_path': '//*[@id="path"]/a[{index}]/text()',
        # xpath for extracting the urls of manufacturers and the categories.
        'man_cat_url': '//*[@id="content"]/div/div/ul/li/a/@href',
        # xpath for extracting the model_urls.
        'model_urls': '//*[@id="content"]/div/div[2]/ul/li/a/@href',
        # xpath for extracting the parts.
        'parts': '//*[@id="content"]/div/div/div/ul/li/a/text()',
        # xpath for extracting the part_categories.
        'part_categories': '//*[@id="content"]/div/div/div/ul/li/a/span/text()'

    }

    # this function is automatically called while crawling for start crawling.
    def parse(self, response):
        # extracting all the manufacturer URLs.
        manufacturers_urls = response.xpath(
            self.response_mapping_xpath['man_cat_url']
        ).extract()

        # loop through each manufacturer to crawl the categories of the manufacturers.
        for url in manufacturers_urls:
            # creating the absolute url for crawling. By Combining the extracted URL and merging it with domain.
            absolute_manufacturer_url = response.urljoin(urllib.parse.quote(url))
            # calling the method for crawling the manufacturer categories URL.
            yield Request(
                absolute_manufacturer_url,
                callback=self.parse_category,
            )

    # This method crawl the categories of the manufacturers.
    def parse_category(self, response):
        # extracting all the categories URLs.
        categories_urls = response.xpath(
            self.response_mapping_xpath['man_cat_url']
        ).extract()

        # loop through each categories to crawl the models of those categories.
        for url in categories_urls:
            # creating the absolute URL.
            absolute_categories_url = response.urljoin(urllib.parse.quote(url))
            # calling the method for crawling the categories models URL.
            yield Request(
                absolute_categories_url,
                callback=self.parse_model,
            )

    # This method crawl the models of the categories.
    def parse_model(self, response):
        # extracting all the models URLs.
        model_urls = response.xpath(
            self.response_mapping_xpath['model_urls']
        ).extract()

        # loop through each categories to crawl the parts of those models.
        for url in model_urls:
            # creating the absolute URL.
            absolute_model_url = response.urljoin(urllib.parse.quote(url))
            # calling the method for crawling the models parts URL.
            yield Request(
                absolute_model_url,
                callback=self.parse_part,
            )

    # This method crawl the parts and the parts_Categories of the model.
    def parse_part(self, response):
        # Extracting all the parts.
        parts = response.xpath(self.response_mapping_xpath['parts']).extract()
        # clean the parts name.
        parts = [part.split()[0] for part in parts]
        # Extracting the parts categories.
        parts_category = response.xpath(self.response_mapping_xpath['part_categories']).extract()
        # cleaning the categories.
        parts_category = [part.strip() for part in parts_category]
        # extracting the manufacturer name.
        manufacturer = response.xpath(
            self.response_mapping_xpath['man_cat_model_path'].format(index=3)
        ).extract_first().strip()
        # extracting the category name.
        category = response.xpath(
            self.response_mapping_xpath['man_cat_model_path'].format(index=4)
        ).extract_first().strip()
        # extracting the model name.
        model = response.xpath(
            self.response_mapping_xpath['man_cat_model_path'].format(index=5)
        ).extract_first().strip()

        # writing the CSV file,

        # with open('urparts_crawled_data.csv', 'w') as output_file:
        #     fieldnames = ['manufacturer', 'category', 'model', 'part', 'part_category']
        #
        #     csv_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        #     csv_writer.writeheader()
        #     csv_writer.writerow(line)

        # loop through the parts and parts_categories and create a dictionary to yield for saving.
        for part, part_category in zip(parts, parts_category):
            yield {
                'manufacturer': manufacturer,
                'category': category,
                'model': model,
                'part': part,
                'part_category': part_category
            }
