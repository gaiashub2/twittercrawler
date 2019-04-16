#!/usr/local/bin/python
# -*- coding: utf-8 -*-

'''
Created on Feb 07, 2019
Updated on Feb 11, 2019

@author: g-suzuki
'''

import twittercrawler

if __name__ == '__main__':
    obj = twittercrawler.TwitterCrawler("user")
    obj.run()