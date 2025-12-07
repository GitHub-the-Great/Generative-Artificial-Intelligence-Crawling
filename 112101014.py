#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import json
import re
import sys
import os
import codecs
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configure UTF-8 output
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Request headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

# Image URL pattern (matches URLs ending with jpg, jpeg, png, gif - case insensitive)
IMAGE_PATTERN = re.compile(r'(https?://\S+\.(jpg|jpeg|png|gif))', re.IGNORECASE)

def get_page_content(url):
    """Get page content with over18 cookie set"""
    try:
        res = requests.get(url, cookies={"over18": "1"}, headers=headers, timeout=10)
        res.raise_for_status()
        return res.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        time.sleep(1)  # Back off on errors
        return None

def is_article_valid(title):
    """Check if article title is valid based on requirements"""
    if title is None or title.strip() == "":
        return False
    if "[公告]" in title or "Fw:[公告]" in title:
        return False
    return True

def crawl_articles():
    """Crawl all articles from 2024"""
    # Starting from this article
    target_url = "https://www.ptt.cc/bbs/Beauty/M.1704040318.A.E87.html"
    target_title = "[正妹] aespa WINTER"
    #target_url = "https://www.ptt.cc/bbs/Beauty/M.1672503968.A.5B5.html"
    #target_title = "[正妹] 周子瑜"
    target_url_end = "https://www.ptt.cc/bbs/Beauty/M.1735659922.A.157.html"
    target_title_end = "[正妹] Cosplay 1563 日本 原神"
    #target_url_end = "https://www.ptt.cc/bbs/Beauty/M.1704040318.A.E87.html"
    #target_title_end = "[正妹] aespa WINTER"
    
    found_target = False
    found_target_end = False
    all_articles = []
    popular_articles = []
    
    # Use a broader range to ensure we find the first article
    for page in tqdm(range(3645, 3950), desc="Crawling pages"): #tqdm(range(3300, 3646), desc="Crawling pages"):
        index_url = f"https://www.ptt.cc/bbs/Beauty/index{page}.html"
        content = get_page_content(index_url)
        
        if not content:
            continue
            
        soup = BeautifulSoup(content, 'html.parser')
        articles = soup.select("div.r-ent")

        if found_target_end:
            break
        
        # Process each article in this page
        for article in articles:
            title_div = article.select_one("div.title a")
            if not title_div:
                continue
                
            title = title_div.text.strip()
            url = "https://www.ptt.cc" + title_div.get('href')
            date_div = article.select_one("div.date")
            date = date_div.text.strip().replace('/', '') if date_div else ""
            # print(f"title = {title} in page id = {page}, date = {date}. found_target = {found_target}")
            
            # Only process if this is our target or we've already found it
            if not found_target:
                if url == target_url and title.strip() == target_title:
                    found_target = True
                else:
                    continue
            
            # Filter invalid articles
            if not is_article_valid(title) or not url:
                continue
                
            # Format date as MMDD
            if len(date) == 3:
                date = "0" + date
            
            # Process popularity
            nrec = article.select_one("div.nrec")
            push_content = nrec.text.strip() if nrec else ""
            is_popular = push_content == "爆"
                
            # Create article data
            article_data = {
                "date": date,
                "title": title,
                "url": url
            }

            if not found_target_end:
                all_articles.append(article_data)

            if url == target_url_end and title.strip() == target_title_end:
                found_target_end = True;
                break;
            
            if is_popular:
                popular_articles.append(article_data)
        
        # If we found and processed the target, and no articles on this page
        # are from 2024 (check first digit of month), we can stop
        if found_target and all(int(a["date"][0]) > 1 for a in all_articles[-len(articles):] if len(a["date"]) >= 1):
            break
            
        # Avoid rate limiting
        time.sleep(0.5)
    
    # Write results to files
    with open('articles.jsonl', 'w', encoding='utf-8') as f:
        for article in all_articles:
            f.write(json.dumps(article, ensure_ascii=False) + '\n')
    
    with open('popular_articles.jsonl', 'w', encoding='utf-8') as f:
        for article in popular_articles:
            f.write(json.dumps(article, ensure_ascii=False) + '\n')
    
    print(f"Crawled {len(all_articles)} articles, {len(popular_articles)} of which are popular")

def process_article_comments(url):
    """Process comments in an article and return push/boo counts and user IDs"""
    content = get_page_content(url)
    if not content:
        return 0, 0, [], []
    
    soup = BeautifulSoup(content, 'html.parser')
    pushes = soup.select('div.push')
    
    good_count = 0
    bad_count = 0
    good_list = []
    bad_list = []
    
    for push in pushes:
        tag = push.select_one('span.push-tag')
        if not tag:
            continue
            
        tag_text = tag.text.strip()
        
        user_id_span = push.select_one('span.push-userid')
        if not user_id_span:
            continue
            
        user_id = user_id_span.text.strip()
        
        if '推' in tag_text:
            good_count += 1
            good_list.append(user_id)
        elif '噓' in tag_text:
            bad_count += 1
            bad_list.append(user_id)
    
    return good_count, bad_count, good_list, bad_list

def create_top10_list(user_counts):
    """Create top 10 list sorted by count and then by user_id in reverse lexical order"""
    # Sort by count (desc) and then by user_id (desc)
    sorted_users = sorted(user_counts.items(), key=lambda x: (-x[1], ''.join(chr(255 - ord(c)) for c in x[0])))
    
    # Get top 10 (or fewer if not enough)
    top10 = []
    for i, (user_id, count) in enumerate(sorted_users):
        if i >= 10:
            break
        top10.append({"user_id": user_id, "count": count})
    
    return top10

def push_analysis(start_date, end_date):
    """Analyze push data between dates"""
    url_list = []
    
    # Read articles from file
    with open('articles.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            if int(data['date']) >= int(start_date) and int(data['date']) <= int(end_date):
                url_list.append(data['url'])
    
    good_count = 0
    bad_count = 0
    good_users = {}
    bad_users = {}
    
    # Process articles in parallel
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_article_comments, url) for url in url_list]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing comments"):
            g, b, g_list, b_list = future.result()
            good_count += g
            bad_count += b
            
            # Count users
            for user in g_list:
                good_users[user] = good_users.get(user, 0) + 1
            
            for user in b_list:
                bad_users[user] = bad_users.get(user, 0) + 1
    
    # Create result
    result = {
        "push": {
            "total": good_count,
            "top10": create_top10_list(good_users)
        },
        "boo": {
            "total": bad_count,
            "top10": create_top10_list(bad_users)
        }
    }
    
    # Write result to file
    output_file = f'push_{start_date}_{end_date}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
    
    print(f"Push analysis written to {output_file}")

def extract_image_urls(content):
    """Extract image URLs from content"""
    if not content:
        return []
    
    # Find all URLs ending with image extensions
    matches = re.findall(IMAGE_PATTERN, content)
    # Extract just the URLs (first element of each match tuple)
    return [match[0] for match in matches]

def process_article_images(url):
    """Extract image URLs from an article"""
    content = get_page_content(url)
    if not content:
        return []
    
    return extract_image_urls(content)

def popular_analysis(start_date, end_date):
    """Analyze popular articles between dates"""
    url_list = []
    popular_count = 0
    
    # Read popular articles from file
    with open('popular_articles.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            if int(data['date']) >= int(start_date) and int(data['date']) <= int(end_date):
                url_list.append(data['url'])
                popular_count += 1
    
    image_urls = []
    
    # Process articles in parallel
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_article_images, url) for url in url_list]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing images"):
            image_urls.extend(future.result())
    
    # Create result
    result = {
        "number_of_popular_articles": popular_count,
        "image_urls": image_urls
    }
    
    # Write result to file
    output_file = f'popular_{start_date}_{end_date}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
    
    print(f"Popular analysis written to {output_file}")

def contains_keyword(url, keyword):
    """Check if article contains keyword and return image URLs if it does"""
    content = get_page_content(url)
    if not content:
        return []
    
    soup = BeautifulSoup(content, 'html.parser')
    
    # Get main container
    main_container = soup.find(id="main-container")
    if not main_container:
        return []
    
    # Find the content between author and '※ 發信站'
    article_text = main_container.text
    
    # Check if '※ 發信站' exists
    if '※ 發信站' not in article_text:
        return []
    
    # Get content before '※ 發信站'
    content_before_station = article_text.split('※ 發信站')[0]
    
    # Check if keyword is in content
    if keyword in content_before_station:
        return extract_image_urls(content)
    
    return []

def keyword_analysis(start_date, end_date, keyword):
    """Analyze articles containing keyword between dates"""
    url_list = []
    
    # Read articles from file
    with open('articles.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            if int(data['date']) >= int(start_date) and int(data['date']) <= int(end_date):
                url_list.append(data['url'])
    
    image_urls = []
    
    # Process articles in parallel
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(contains_keyword, url, keyword) for url in url_list]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing keywords"):
            image_urls.extend(future.result())
    
    # Create result
    result = {
        "image_urls": image_urls
    }
    
    # Write result to file
    output_file = f'keyword_{start_date}_{end_date}_{keyword}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
    
    print(f"Keyword analysis written to {output_file}")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python 112101014.py [crawl|push|popular|keyword] [args...]")
        return
    
    command = sys.argv[1]
    
    if command == 'crawl':
        crawl_articles()
    elif command == 'push':
        if len(sys.argv) != 4:
            print("Usage: python 112101014.py push <start_date> <end_date>")
            return
        push_analysis(sys.argv[2], sys.argv[3])
    elif command == 'popular':
        if len(sys.argv) != 4:
            print("Usage: python 112101014.py popular <start_date> <end_date>")
            return
        popular_analysis(sys.argv[2], sys.argv[3])
    elif command == 'keyword':
        if len(sys.argv) != 5:
            print("Usage: python 112101014.py keyword <start_date> <end_date> <keyword>")
            return
        keyword_analysis(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()